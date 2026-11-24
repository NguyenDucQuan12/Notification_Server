"""
Xác thực và phân quyền cho kết nối WebSocket.

Luồng xác thực:
1. (Tùy chọn) Kiểm tra header Origin nằm trong danh sách cho phép (chống CSWSH).
2. Lấy `ticket` từ query string, GETDEL trong Redis để đổi lấy Identity.
3. Kiểm tra ticket được cấp cho đúng channel đang kết nối
   (ticket channel=user không được dùng để nghe tenant_dashboard).
4. Kiểm tra quyền (permission) tương ứng với channel.

Mọi lỗi đều ném WebSocketAuthError kèm close code + reason để router đóng
kết nối thống nhất. Danh sách close code: xem utils/close_codes.py.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from fastapi import WebSocket

from config.configs import settings
from services.ws_ticket_service import TicketServiceUnavailable, consume_ws_ticket
from utils import close_codes


@dataclass(frozen=True)
class Identity:
    """
    Thông tin người dùng lấy từ ticket.

    frozen=True: không cho sửa sau khi tạo, tránh vô tình đổi quyền giữa chừng.
    """
    tenant_id: str
    user_id: str
    user_name: str | None = None
    role_id: str | None = None
    role_name: str | None = None
    is_superuser: bool = False
    permissions: frozenset[str] = field(default_factory=frozenset)
    channel: str | None = None
    job_id: str | None = None

    @classmethod
    def from_ticket_payload(cls, payload: dict[str, Any]) -> "Identity":
        """
        Chuyển payload JSON của ticket thành Identity, ép kiểu an toàn.
        payload JSON có thể bị giả mạo, nên KHÔNG dùng trực tiếp.
        """
        raw_permissions = payload.get("permissions") or []
        if not isinstance(raw_permissions, (list, tuple, set)):
            raw_permissions = []

        def _opt_str(value: Any) -> str | None:
            return None if value is None else str(value)

        return cls(
            tenant_id=str(payload["tenant_id"]),
            user_id=str(payload["user_id"]),
            user_name=_opt_str(payload.get("user_name")),
            role_id=_opt_str(payload.get("role_id")),
            role_name=_opt_str(payload.get("role_name")),
            # Chỉ nhận đúng giá trị True. Tránh trường hợp chuỗi "false" bị bool() thành True.
            is_superuser=payload.get("is_superuser") is True,
            permissions=frozenset(str(p) for p in raw_permissions),
            channel=_opt_str(payload.get("channel")),
            job_id=_opt_str(payload.get("job_id")),
        )

class WebSocketAuthError(Exception):
    """
    Lỗi xác thực/phân quyền. Router dùng code + reason để đóng WebSocket.
    """
    def __init__(self, code: int, reason: str) -> None:
        super().__init__(reason)
        self.code = code
        self.reason = reason


def identity_has_permission(identity: Identity, permission: str) -> bool:
    """
    Kiểm tra xem Identity có quyền đó hay không.

    Superuser có mọi quyền; user thường phải có đúng permission.
    """
    return identity.is_superuser or permission in identity.permissions


def check_origin(websocket: WebSocket) -> None:
    """
    Chống Cross-Site WebSocket Hijacking (CSWSH).

    Trình duyệt KHÔNG áp dụng CORS cho WebSocket: một trang web độc hại vẫn có
    thể mở WebSocket tới server của mình. Trình duyệt luôn gửi header Origin,
    nên server tự kiểm tra Origin đó.

    - WS_ALLOWED_ORIGINS rỗng       -> bỏ qua kiểm tra (chế độ dev / client nội bộ).
    - Không có header Origin         -> cho qua (client không phải trình duyệt,
                                        vd Python/mobile; vẫn phải có ticket hợp lệ).
    - Có Origin nhưng không nằm trong danh sách -> từ chối.
    """
    allowed = settings.ws_allowed_origins
    if not allowed:
        return

    origin = websocket.headers.get("origin")
    if origin and origin.rstrip("/") not in allowed:
        raise WebSocketAuthError(close_codes.FORBIDDEN, "origin_not_allowed")

async def authenticate_websocket(
    websocket: WebSocket,
    *,
    expected_channel: str,
    required_permission: str,
    job_id: str | None = None,
) -> Identity:
    """
    Thực hiện toàn bộ các bước xác thực + phân quyền cho 1 kết nối.

    Thứ tự kiểm tra có chủ ý:
    Origin (rẻ nhất, không cần Redis) -> ticket (tốn 1 lệnh Redis) -> channel -> quyền.
    """
    # Kiểm tra Origin để chống CSWSH. Nếu không hợp lệ, ném WebSocketAuthError.
    check_origin(websocket)

    # Lấy ticket từ query string
    ticket = websocket.query_params.get("ticket")
    if not ticket:
        raise WebSocketAuthError(close_codes.UNAUTHORIZED, "ticket_missing")

    # Lấy payload từ Redis và xóa ticket luôn (GETDEL)
    try:
        payload = await consume_ws_ticket(ticket)
    except TicketServiceUnavailable as exc:
        raise WebSocketAuthError(close_codes.TRY_AGAIN_LATER, "auth_service_unavailable") from exc

    if not payload:
        raise WebSocketAuthError(close_codes.UNAUTHORIZED, "ticket_invalid_or_expired")

    identity = Identity.from_ticket_payload(payload)

    # Ticket được cấp cho channel nào thì chỉ dùng cho channel đó.
    if identity.channel != expected_channel:
        raise WebSocketAuthError(close_codes.FORBIDDEN, "ticket_channel_mismatch")

    # Với channel job: ticket phải được cấp cho đúng job_id trong URL.
    if job_id is not None and identity.job_id != job_id:
        raise WebSocketAuthError(close_codes.FORBIDDEN, "ticket_job_mismatch")

    if not identity_has_permission(identity, required_permission):
        raise WebSocketAuthError(close_codes.FORBIDDEN, "permission_denied")

    return identity
