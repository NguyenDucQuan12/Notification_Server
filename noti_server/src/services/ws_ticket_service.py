from __future__ import annotations

import json
import secrets
from typing import Any

import redis.asyncio as redis_async

from config.configs import settings
from utils.redis_keys import ws_ticket_key


redis_client = redis_async.from_url(
    settings.REDIS_URL,
    decode_responses=True,
)
WS_TICKET_TTL_SECONDS = 60


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

    job_id:
    - chỉ dùng khi cấp ticket nghe riêng một job

    Ticket này:
    - dùng một lần
    - TTL ngắn
    - không phải access token thật
    - nếu lộ cũng giảm rủi ro hơn nhiều so với JWT dài hạn
    """

    ticket = secrets.token_urlsafe(32)

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

    await redis_client.set(
        ws_ticket_key(ticket),
        json.dumps(payload, ensure_ascii=False),
        ex=WS_TICKET_TTL_SECONDS,
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
    # Tạo key trong Redis từ ticket
    key = ws_ticket_key(ticket)
    # Lấy payload từ Redis. Nếu không có thì trả về None.
    raw = await redis_client.get(key)
    # Nếu có thì xóa ngay để ticket không dùng lại được, sau đó trả về payload đã giải mã. Nếu có lỗi khi giải mã thì cũng trả về None.
    if not raw:
        return None

    # Xóa ngay để ticket không dùng lại được.
    await redis_client.delete(key)

    # Tiến hành giải mã payload. Nếu có lỗi khi giải mã thì trả về None.
    try:
        return json.loads(raw)
    except Exception:
        return None