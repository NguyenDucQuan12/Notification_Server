"""
API quản lý thông báo (notification) cho người dùng.
Các thông báo được gửi qua Redis Stream, WebSocket, và lưu trong SQL Server.

Router chỉ nhận request rồi gọi NotificationController (controllers/notification_controller.py).
"""
from __future__ import annotations

from typing import Any, Literal

from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession

from auth.oauth2 import required_token_user
from controllers.notification_controller import NotificationController
from db.database import get_db
from schemas.noti_schema import NotificationSendRequest


router = APIRouter(
    prefix="/notifications",
    tags=["Notifications"],
)


@router.post("/ws-ticket")
async def api_create_ws_ticket(
    channel: Literal["user", "tenant_dashboard", "job", "global"] = Query(...),
    job_id: str | None = Query(default=None),
    current_user: dict[str, Any] = Depends(required_token_user),
) -> Any:
    """
    API cấp WebSocket ticket.
    Ticket dùng để client mở WebSocket và subscribe vào stream tương ứng. Ticket sống rất ngắn (60 giây),
    chỉ dùng một lần và được lưu trong Redis để Notification Server xác thực khi client kết nối.

    Sau khi WebSocket mở thành công, ticket bị xóa khỏi Redis; client nhận event qua WebSocket
    mà không cần gửi token nữa. Client gọi API này trước mỗi lần mở WebSocket.

    Ví dụ:
    - POST /notifications/ws-ticket?channel=user
    - POST /notifications/ws-ticket?channel=tenant_dashboard
    - POST /notifications/ws-ticket?channel=job&job_id=<JOB_ID>
    - POST /notifications/ws-ticket?channel=global
    """
    return await NotificationController.create_ws_ticket_for_user(
        channel=channel,
        job_id=job_id,
        current_user=current_user,
    )

@router.post("/send")
async def api_send_notification(
    request: NotificationSendRequest,
    db: AsyncSession = Depends(get_db),
    current_user: dict[str, Any] = Depends(required_token_user),
) -> Any:
    """
    Gửi 1 thông báo qua luồng chuẩn: SQL Server (outbox) -> Redis Stream -> Notification Server -> WebSocket.

    Quyền:
    - Gửi cho chính mình: chỉ cần đăng nhập.
    - Gửi cho người khác / tất cả người dùng / tenant dashboard / global / job:
      cần superuser hoặc quyền `notification:send`.

    Ví dụ body tối thiểu (gửi cho chính mình):
    ```json
    {"title": "Xin chào", "body": "Thông báo thử nghiệm"}
    ```
    """
    return await NotificationController.send_notification(
        request=request,
        db=db,
        current_user=current_user,
    )

# ============================================================
# HỘP THƯ THÔNG BÁO CỦA NGƯỜI DÙNG
# ============================================================

@router.get("/feed")
async def api_get_my_notifications(
    limit: int = Query(default=30, ge=1, le=100, description="Số thông báo tối đa trả về."),
    offset: int = Query(default=0, ge=0, description="Bỏ qua bao nhiêu thông báo (phân trang)."),
    state: Literal["unread", "seen", "read"] | None = Query(default=None, description="Lọc theo trạng thái đọc."),
    db: AsyncSession = Depends(get_db),
    current_user: dict[str, Any] = Depends(required_token_user),
) -> Any:
    """
    Danh sách thông báo của người đang đăng nhập (mới nhất trước, ưu tiên ghim/quan trọng)
    kèm số thông báo chưa đọc.

    Client nên gọi API này khi mở app và mỗi lần kết nối lại WebSocket để đồng bộ.
    """
    return await NotificationController.get_my_feed(
        db=db,
        current_user=current_user,
        state=state,
        limit=limit,
        offset=offset,
    )

@router.get("/unread-count")
async def api_get_my_unread_count(
    db: AsyncSession = Depends(get_db),
    current_user: dict[str, Any] = Depends(required_token_user),
) -> Any:
    """
    Số thông báo chưa đọc (dùng cho badge trên biểu tượng chuông).
    """

    return await NotificationController.get_my_unread_count(
        db=db,
        current_user=current_user,
    )

@router.post("/read-all")
async def api_read_all_my_notifications(
    db: AsyncSession = Depends(get_db),
    current_user: dict[str, Any] = Depends(required_token_user),
) -> Any:
    """
    Đánh dấu tất cả thông báo là đã đọc.
    """

    return await NotificationController.read_all(
        db=db,
        current_user=current_user,
    )

@router.post("/{notification_id}/read")
async def api_read_my_notification(
    notification_id: str,
    db: AsyncSession = Depends(get_db),
    current_user: dict[str, Any] = Depends(required_token_user),
) -> Any:
    """
    Đánh dấu 1 thông báo là đã đọc.
    """

    return await NotificationController.read_one(
        notification_id=notification_id,
        db=db,
        current_user=current_user,
    )
