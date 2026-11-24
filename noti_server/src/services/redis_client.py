"""
Quản lý kết nối Redis của Notification Server.

Có HAI client (hai connection pool) riêng:

1. redis_fast_client  -> lệnh ngắn: GETDEL ticket, PING, XREVRANGE/XINFO khi replay.
   - socket_timeout ngắn (vài giây): Redis treo thì request lỗi nhanh.
   - Dùng BlockingConnectionPool: khi cả pool đang bận, request CHỜ tối đa
     REDIS_POOL_TIMEOUT giây để có kết nối rảnh, thay vì lỗi ngay lập tức
     ("Too many connections") như ConnectionPool thường. Rất quan trọng khi
     hàng nghìn client reconnect cùng lúc (sau deploy, sau sự cố mạng).

2. redis_stream_client -> CHỈ dùng cho XREAD BLOCK của Stream Hub.
   - socket_timeout > HUB_BLOCK_MS (đã kiểm tra trong config).
   - Pool rất nhỏ (HUB_SHARDS + 2) vì cả process chỉ có HUB_SHARDS lệnh XREAD
     chạy cùng lúc, KHÔNG phụ thuộc số WebSocket. Đây chính là điểm giúp server
     phục vụ hàng nghìn kết nối: bản cũ mỗi WebSocket giữ 1 kết nối Redis.

Ghi chú:
- decode_responses=True: Redis trả str thay vì bytes (dữ liệu là JSON/văn bản).
- Client tạo 1 lần khi import (singleton), dùng lại cho mọi request.
- Tạo client chưa mở kết nối; kết nối chỉ mở khi chạy lệnh đầu tiên, nên import
  module này khi Redis chưa chạy cũng không lỗi.
"""

from __future__ import annotations

import logging

import redis.asyncio as redis_async
from redis.asyncio import Redis

from config.configs import settings

logger = logging.getLogger("noti.redis")


_fast_pool = redis_async.BlockingConnectionPool.from_url(
    settings.REDIS_URL,
    decode_responses=True,
    socket_keepalive=True,                           # Giữ TCP sống, phát hiện kết nối chết ở tầng OS.
    socket_connect_timeout=settings.REDIS_CONNECT_TIMEOUT,
    socket_timeout=settings.REDIS_SOCKET_TIMEOUT,
    max_connections=settings.REDIS_MAX_CONNECTIONS,
    timeout=settings.REDIS_POOL_TIMEOUT,             # Thời gian tối đa chờ 1 kết nối rảnh.
    health_check_interval=30,                        # Kết nối rảnh > 30s sẽ được PING lại trước khi dùng.
)
# from_pool: client "sở hữu" pool, nên aclose() của client đóng luôn pool.
redis_fast_client: Redis = Redis.from_pool(_fast_pool)


redis_stream_client: Redis = redis_async.from_url(
    settings.REDIS_URL,
    decode_responses=True,
    socket_keepalive=True,
    socket_connect_timeout=settings.REDIS_CONNECT_TIMEOUT,
    socket_timeout=settings.REDIS_STREAM_SOCKET_TIMEOUT,  # > HUB_BLOCK_MS
    max_connections=settings.redis_stream_max_connections,
    health_check_interval=30,
)


def get_redis_fast() -> Redis:
    """Client cho lệnh ngắn (ticket, ping, replay)."""
    return redis_fast_client


def get_redis_stream() -> Redis:
    """Client cho XREAD BLOCK (chỉ Stream Hub dùng)."""
    return redis_stream_client


async def ping_redis() -> bool:
    """
    Kiểm tra Redis còn sống không. Dùng cho readiness check (/health/ready).

    Trả True/False thay vì ném lỗi để endpoint health luôn trả lời được.
    """
    try:
        return bool(await redis_fast_client.ping())
    except Exception as exc:  # pylint: disable=broad-except
        logger.warning("Redis ping failed", extra={"error": repr(exc)})
        return False


async def close_redis_clients() -> None:
    """
    Đóng toàn bộ kết nối khi server tắt (gọi trong lifespan của main.py).

    redis.asyncio yêu cầu đóng rõ ràng bằng aclose(), nếu không Python sẽ cảnh báo
    "Unclosed connection" và kết nối có thể bị treo phía Redis.
    """
    for name, client in (("fast", redis_fast_client), ("stream", redis_stream_client)):
        try:
            await client.aclose()
        except Exception as exc:  # pylint: disable=broad-except
            logger.warning("Error closing Redis client", extra={"client": name, "error": repr(exc)})
