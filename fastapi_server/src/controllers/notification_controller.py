"""
Controller xử lý nghiệp vụ thông báo (notification).

Controller có nhiệm vụ:
- Kiểm tra quyền người gọi API.
- Validate dữ liệu đầu vào.
- Gọi các hàm query / service để thao tác database, Redis.
- Chuyển lỗi nghiệp vụ thành HTTPException.

Router (api/notification_api.py) chỉ nhận request rồi gọi controller.
"""
from __future__ import annotations

from typing import Any, Literal

from fastapi import HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession

from db.query_notification_recipients import (
    count_unread_notifications,
    mark_all_notifications_read,
    mark_notification_read,
)
from db.query_notification_views import get_user_notification_feed
from db.query_user_auth import get_active_user_ids
from schemas.noti_schema import NotificationSendRequest
from services.notification_orchestrator import send_custom_notification
from services.ws_ticket_service import create_ws_ticket

# Các kênh WebSocket mà người dùng có thể lắng nghe
WsChannel = Literal["user", "tenant_dashboard", "job", "global"]

# Quyền cần có để mở WebSocket theo từng kênh
CHANNEL_PERMISSIONS: dict[str, str] = {
    "user": "notification:subscribe",
    "tenant_dashboard": "notification:tenant_dashboard",
    "global": "notification:global_subscribe",
    "job": "notification:job_watch",
}

# Thời gian sống của WebSocket ticket (giây) - khớp với ws_ticket_service
WS_TICKET_EXPIRES_IN = 60


class NotificationController:
    """
    Controller xử lý nghiệp vụ liên quan đến thông báo:
    - Cấp WebSocket ticket.
    - Gửi thông báo.
    - Hộp thư: danh sách, đếm chưa đọc, đánh dấu đã đọc.
    """
    @staticmethod
    def _require_login(current_user: dict[str, Any]) -> None:
        """
        Kiểm tra đã xác thực người dùng.
        """
        if not current_user:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail={"message": "Chưa xác thực người dùng"},
            )

    @staticmethod
    def has_permission(current_user: dict[str, Any], permission: str) -> bool:
        """
        Kiểm tra permission của user.
        Superuser được qua toàn bộ.
        """
        if current_user.get("is_superuser"):
            return True
        # Lấy quyền hạn của người dùng
        permissions = current_user.get("permissions") or []

        # Nếu permission cần kiểm tra nằm trong danh sách quyền hạn của người dùng -> có quyền
        return permission in permissions

    @staticmethod
    def _identity(current_user: dict[str, Any]) -> tuple[str, str]:
        """
        Trả về (tenant_id, user_id) của người đang đăng nhập.
        """
        NotificationController._require_login(current_user)
        return str(current_user["tenant_id"]), str(current_user["user_id"])

    # ============================================================
    # WEBSOCKET TICKET
    # ============================================================

    @staticmethod
    async def create_ws_ticket_for_user(
        *,
        channel: WsChannel,
        job_id: str | None,
        current_user: dict[str, Any],
    ) -> dict[str, Any]:
        """
        Cấp WebSocket ticket.

        Luồng xử lý:
        1. Xác định quyền cần có theo kênh (user / tenant_dashboard / job / global).
        2. channel=job bắt buộc có job_id.
        3. Kiểm tra quyền, không đủ quyền -> 403.
        4. Tạo ticket trong Redis (sống 60 giây, dùng 1 lần).
        """
        # Kiêm tra đã xác thực người dùng
        NotificationController._require_login(current_user)

        # Với từng kênh thì ta yêu cầu từng quyền hạn riêng
        required_permission = CHANNEL_PERMISSIONS[channel]

        if channel == "job" and not job_id:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail={"message": "job_id là bắt buộc khi lắng nghe sự kiện của job"},
            )

        if not NotificationController.has_permission(current_user, required_permission):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail={"message": f"Bạn không có quyền truy cập kênh {channel}"},
            )

        # Tạo 1 ticket cho người dùng
        ticket = await create_ws_ticket(
            identity=current_user,
            channel=channel,
            job_id=job_id,
        )

        return {
            "success": True,
            "data": {
                "ticket": ticket,
                "expires_in": WS_TICKET_EXPIRES_IN,
                "channel": channel,
                "job_id": job_id,
            },
            "message": "Đã cấp WebSocket ticket",
        }

    # ============================================================
    # GỬI THÔNG BÁO
    # ============================================================

    @staticmethod
    async def send_notification(
        *,
        request: NotificationSendRequest,
        db: AsyncSession,
        current_user: dict[str, Any],
    ) -> dict[str, Any]:
        """
        Gửi 1 thông báo qua luồng chuẩn: SQL Server (outbox) -> Redis Stream -> Notification Server -> WebSocket.

        Quyền:
        - Gửi cho chính mình: chỉ cần đăng nhập.
        - Gửi cho người khác / tất cả người dùng / tenant dashboard / global / job:
          cần superuser hoặc quyền `notification:send`.
        """
        # Lấy tenant_id, user_id của người đang đăng nhập
        tenant_id, self_user_id = NotificationController._identity(current_user)
        # Kiểm tra người dùng có quyền gửi thông báo cho người khác
        can_send = NotificationController.has_permission(current_user, "notification:send")

        # Lấy danh sách user_id từ request, loại bỏ rỗng và khoảng trắng
        target_user_ids = [u.strip() for u in request.target_user_ids if u and u.strip()]

        # Gửi cho tất cả người dùng trong tenant: kiểm tra quyền rồi lấy danh sách user_id từ DB
        if request.send_to_all_users:
            if not can_send:
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail={"message": f"Bạn không có quyền gửi thông báo cho tất cả người dùng trong tổ chức {tenant_id}"},
                )
            # Lấy danh sách user_id đang hoạt động trong tenant
            target_user_ids = await get_active_user_ids(db, tenant_id=tenant_id)
            # Trả connection về pool trước khi orchestrator mở session riêng (tránh giữ 2 connection/request).
            await db.rollback()

        # Không có người nhận và không gửi tenant dashboard / global / job -> mặc định gửi cho chính mình
        if not target_user_ids and not (
            request.send_to_tenant_dashboard or request.send_to_global or request.job_id
        ):
            target_user_ids = [self_user_id]

        # Có gửi cho người khác (ngoài chính mình) không
        sends_to_others = (
            any(u != self_user_id for u in target_user_ids)
            or request.send_to_tenant_dashboard
            or request.send_to_global
            or bool(request.job_id)
        )
        if sends_to_others and not can_send:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail={"message": "Bạn không có quyền gửi thông báo cho người khác / tenant dashboard / global / job"},
            )

        try:
            # Gửi thông báo qua orchestrator
            result = await send_custom_notification(
                tenant_id=tenant_id,
                title=request.title,
                body=request.body,
                current_user=current_user,
                target_user_ids=target_user_ids,
                send_to_tenant_dashboard=request.send_to_tenant_dashboard,
                send_to_global=request.send_to_global,
                job_id=request.job_id,
                severity=request.severity,
                notification_type=request.notification_type,
                data=request.data,
                mode=request.mode,
            )
        except ValueError as exc:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail={"message": str(exc)},
            ) from exc
        except Exception as exc:  # noqa: BLE001 - lỗi DB/Redis
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail={"message": f"Không gửi được thông báo: {type(exc).__name__}"},
            ) from exc

        return {
            "success": True,
            "data": result,
            "message": "Đã gửi thông báo",
        }

    # ============================================================
    # HỘP THƯ THÔNG BÁO CỦA NGƯỜI DÙNG
    # ============================================================
    # Redis Stream chỉ giữ một lượng event gần nhất cho mỗi user (để gửi realtime + gửi bù
    # khi reconnect ngắn). Lịch sử đầy đủ và trạng thái đã đọc nằm trong SQL Server.
    # Client nên gọi /feed khi mở app và mỗi lần kết nối lại WebSocket để đồng bộ.

    @staticmethod
    async def get_my_feed(*, db: AsyncSession, current_user: dict[str, Any],
                          state: Literal["unread", "seen", "read"] | None = None,
                          limit: int = 30, offset: int = 0) -> dict[str, Any]:
        """
        Danh sách thông báo của người đang đăng nhập (mới nhất trước, ưu tiên ghim/quan trọng)
        kèm số thông báo chưa đọc.
        """
        tenant_id, user_id = NotificationController._identity(current_user)

        if limit < 1 or limit > 100:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail={"message": "limit phải nằm trong khoảng 1 đến 100"},
            )
        if offset < 0:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail={"message": "offset không được âm"},
            )

        items = await get_user_notification_feed(
            db,
            tenant_id=tenant_id,
            user_id=user_id,
            state=state,
            limit=limit,
            offset=offset,
        )
        unread = await count_unread_notifications(
            db,
            tenant_id=tenant_id,
            recipient_user_id=user_id,
        )
        return {"success": True, "data": {"items": items, "unread_count": unread}}

    @staticmethod
    async def get_my_unread_count(*, db: AsyncSession, current_user: dict[str, Any]) -> dict[str, Any]:
        """
        Số thông báo chưa đọc (dùng cho badge trên biểu tượng chuông).
        """
        tenant_id, user_id = NotificationController._identity(current_user)

        unread = await count_unread_notifications(
            db,
            tenant_id=tenant_id,
            recipient_user_id=user_id,
        )
        return {"success": True, "data": {"unread_count": unread}}

    @staticmethod
    async def read_all(*, db: AsyncSession, current_user: dict[str, Any]) -> dict[str, Any]:
        """
        Đánh dấu tất cả thông báo là đã đọc.
        """
        tenant_id, user_id = NotificationController._identity(current_user)

        updated = await mark_all_notifications_read(
            db,
            tenant_id=tenant_id,
            recipient_user_id=user_id,
        )
        await db.commit()
        return {"success": True, "data": {"updated": updated}}

    @staticmethod
    async def read_one(*, notification_id: str, db: AsyncSession, current_user: dict[str, Any]) -> dict[str, Any]:
        """
        Đánh dấu 1 thông báo là đã đọc. Không tìm thấy -> 404.
        """
        tenant_id, user_id = NotificationController._identity(current_user)

        notification_id = (notification_id or "").strip()
        if not notification_id:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail={"message": "notification_id không được rỗng"},
            )

        updated = await mark_notification_read(
            db,
            tenant_id=tenant_id,
            notification_id=notification_id,
            recipient_user_id=user_id,
        )
        await db.commit()

        if not updated:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail={"message": "Không tìm thấy thông báo"},
            )
        return {"success": True, "data": {"notification_id": notification_id}}
