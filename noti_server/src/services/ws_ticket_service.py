"""
Đọc và "tiêu thụ" WebSocket ticket do API server tạo ra.

Phân chia trách nhiệm:
- API server (fastapi_server) TẠO ticket: xác thực JWT -> lưu payload vào Redis
  với key `ws:ticket:<ticket>` và TTL ngắn (vd 60 giây).
- Notification server (file này) CHỈ ĐỌC + XÓA ticket. Không tạo ticket

Bây giờ dùng lệnh GETDEL (Redis >= 6.2): đọc và xóa trong MỘT lệnh nguyên tử.
Redis xử lý lệnh tuần tự, nên chỉ đúng 1 kết nối nhận được payload.
"""

from __future__ import annotations

import json
import logging
from typing import Any

from redis.exceptions import RedisError

from services.redis_client import get_redis_fast
from utils.redis_keys import ws_ticket_key

logger = logging.getLogger("noti.ticket")

# Ticket do secrets.token_urlsafe(32) tạo ra dài ~43 ký tự.
# Giới hạn độ dài để chặn input rác/khổng lồ trước khi gửi vào Redis.
_MAX_TICKET_LENGTH = 256


class TicketServiceUnavailable(Exception):
    """
    Không kiểm tra được ticket vì Redis lỗi (khác với ticket SAI).

    Phân biệt 2 trường hợp này rất quan trọng:
    - Ticket sai/hết hạn  -> lỗi của client, đóng code 4401, client phải xin ticket mới.
    - Redis lỗi           -> lỗi của server, đóng code 1013 (Try Again Later),
                             client nên thử lại sau vài giây.
    """

async def consume_ws_ticket(ticket: str) -> dict[str, Any] | None:
    """
    Đọc và xóa ticket trong một thao tác nguyên tử.  
    Trả về:
    - dict payload nếu ticket hợp lệ.
    - None nếu ticket không tồn tại, đã dùng, hết hạn, hoặc payload hỏng.

    Ném:
    - TicketServiceUnavailable nếu Redis không trả lời được.
    """
    if not ticket or len(ticket) > _MAX_TICKET_LENGTH:
        return None
    try:
        # GETDEL: lấy giá trị và xóa key luôn. Không ai dùng lại được ticket này.
        raw = await get_redis_fast().getdel(ws_ticket_key(ticket))
    except RedisError as exc:
        logger.error("Cannot read ws ticket from Redis", extra={"error": repr(exc)})
        raise TicketServiceUnavailable("redis_unavailable") from exc

    if not raw:
        return None
    try:
        payload = json.loads(raw)
    except (TypeError, ValueError):
        logger.warning("Ws ticket payload is not valid JSON")
        return None

    # Payload phải là object và có đủ 2 trường định danh bắt buộc.
    if not isinstance(payload, dict) or not payload.get("tenant_id") or not payload.get("user_id"):
        logger.warning("Ws ticket payload missing tenant_id/user_id")
        return None

    return payload
