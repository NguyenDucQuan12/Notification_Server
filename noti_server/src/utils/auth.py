from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from fastapi import WebSocket

from services.ws_ticket_service import consume_ws_ticket


@dataclass
class Identity:
    tenant_id: str
    user_id: str
    username: str | None = None
    role_id: str | None = None
    role_name: str | None = None
    is_superuser: bool = False
    permissions: list[str] = field(default_factory=list)
    channel: str | None = None
    job_id: str | None = None


def identity_has_permission(identity: Identity, permission: str) -> bool:
    """
    Kiểm tra quyền.

    Superuser được phép toàn bộ.
    """

    if identity.is_superuser:
        return True

    return permission in identity.permissions


async def get_current_identity_from_websocket(websocket: WebSocket) -> Identity:
    """
    Xác thực WebSocket bằng ticket ngắn hạn.

    Không dùng access token trong query nữa vì có thể bị lộ trong logs, thay vào đó là ticket ngắn hạn chỉ dùng để xác thực WebSocket một lần duy nhất.
    Ticket này được tạo ra sau khi user đăng nhập thành công, chứa thông tin cần thiết để xác thực và phân quyền cho WebSocket, và được lưu tạm thời trong Redis với TTL ngắn (ví dụ 5 phút). Khi client kết nối WebSocket, họ sẽ gửi ticket này lên để xác thực. Server sẽ kiểm tra ticket này trong Redis, nếu hợp lệ thì trả về thông tin identity tương ứng, nếu không thì từ chối kết nối. Sau khi ticket được sử dụng, nó sẽ bị xóa khỏi Redis để đảm bảo tính bảo mật.
    Cách này giúp giảm rủi ro lộ token dài hạn và chỉ cho phép kết nối WebSocket với các quyền đã được xác định trước, đồng thời vẫn đảm bảo tính tiện lợi cho client khi không phải gửi token dài hạn trong query string.
    """

    # Lấy ticket từ query params
    ticket = websocket.query_params.get("ticket")

    if not ticket:
        raise ValueError("Ticket chưa được cung cấp để xác thực WebSocket")

    # Xác thực ticket và lấy payload. Nếu có lỗi thì trả về None.
    payload = await consume_ws_ticket(ticket)

    if not payload:
        raise ValueError("Ticket không hợp lệ hoặc đã hết hạn")

    return Identity(
        tenant_id=str(payload["tenant_id"]),
        user_id=str(payload["user_id"]),
        username=payload.get("username"),
        role_id=payload.get("role_id"),
        role_name=payload.get("role_name"),
        is_superuser=bool(payload.get("is_superuser", False)),
        permissions=[str(p) for p in payload.get("permissions", [])],
        channel=payload.get("channel"),
        job_id=payload.get("job_id"),
    )