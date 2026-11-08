from __future__ import annotations

import json
import secrets
from typing import Any

from fastapi import HTTPException, status
from services.redis_client import get_redis_fast
from utils.redis_keys import ws_ticket_key
from config.config import settings


# Kết nối tới redis
redis_client = get_redis_fast()

WS_TICKET_TTL_SECONDS = int(settings.websocket_ticket_ttl)


async def create_ws_ticket(
    *,
    identity: dict[str, Any],
    channel: str,
    job_id: str | None = None,
) -> str:
    """
    Tạo WebSocket ticket ngắn hạn.

    identity:
    - thông tin user lấy từ JWT/cookie sau khi HTTP API xác thực thành công

    channel:
    - user
    - tenant_dashboard
    - job
    - global

    job_id:
    - chỉ dùng khi cấp ticket nghe riêng một job

    Ticket này:
    - dùng một lần
    - TTL ngắn
    - không phải access token thật
    - nếu lộ cũng giảm rủi ro hơn nhiều so với JWT dài hạn
    """

    # Tạo 1 mã khóa token ngẫu nhiên
    ticket = secrets.token_urlsafe(32)

    # Tạo payload chứa thông tin người dùng
    payload = {
        "tenant_id": identity["tenant_id"],
        "user_id": identity["user_id"],
        "username": identity.get("username"),
        "role_id": identity.get("role_id"),
        "role_name": identity.get("role_name"),
        "is_superuser": bool(identity.get("is_superuser", False)),
        "permissions": identity.get("permissions", []),
        "channel": channel,
        "job_id": job_id,
    }
    # Tạo một ticket lên redis
    try:
        await redis_client.set(
            ws_ticket_key(ticket),
            json.dumps(payload, ensure_ascii=False),
            ex=WS_TICKET_TTL_SECONDS,
        )
    except Exception as e:
        raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail={"message":f"Không thể tạo ticket kết nối websocket lên Redis: {str(e)}"},
                )
    return ticket


async def consume_ws_ticket(ticket: str) -> dict[str, Any] | None:
    """
    Lấy và xóa WebSocket ticket.

    Đây là thao tác dùng một lần.

    Nếu ticket tồn tại:
    - đọc payload
    - xóa ticket
    - trả identity

    Nếu ticket không tồn tại:
    - trả None
    """
    # Tạo key ticket giống với lúc đẩy lên Redis
    key = ws_ticket_key(ticket)
    # Lấy thông tin ticket trên Redis
    try:
        raw = await redis_client.get(key)
    except Exception as e:
        raise HTTPException (
            status_code= status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail= {
                "message": f"Không thể lấy key ticket từ Redis: {str(e)}"
            }
        )

    if not raw:
        return None

    # Xóa ngay để ticket không dùng lại được.
    await redis_client.delete(key)

    try:
        return json.loads(raw)
    except Exception:
        return None