

import os                 # Đọc biến môi trường để lấy REDIS_URL
import redis              # Thư viện redis-py (pip install redis)
import redis.asyncio as redis_async
from redis.asyncio import Redis
"""
Đầu tiên cần chạy Redis server (có thể chạy local hoặc Docker).  
Với Docker: `docker run -p 6379:6379 -it redis:latest`

Hoặc chi tiết hơn: `docker run -d --name my-redis -p 6379:6379 -v ./redis-data:/data -e TZ=Asia/Ho_Chi_Minh redis:latest --appendonly yes`
Cách chạy thứ 2 này sẽ lưu dữ liệu Redis ra thư mục redis-data trên host, tránh mất dữ liệu khi container dừng. (nhớ tạo thư mục redis-data trước khi chạy lệnh)

Sau đó, cài thư viện redis-py: `pip install redis`
Xem thêm tài liệu: https://pypi.org/project/redis/
Lưu ý: redis-py mặc định trả về bytes, bạn có thể decode thành str (decode_responses=True) nhưng sẽ chậm hơn.
Ở đây ta để decode_responses=False để nhận bytes, nhanh hơn, và tự decode khi cần.
Việc kết nối Redis nên dùng connection pool để tái sử dụng kết nối TCP, giúp hiệu năng ổn định trong môi trường nhiều request.
Mặc định Redis đã trả về pool nên ta ko cần cấu hình nữa
"""

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")  # REDIS_URL chuẩn trong Docker: redis://redis:6379/0

# ============================================================
# 1. Timeout nhanh: dùng cho ping/cache/ticket/xadd/publish
# ============================================================
# Tạo connection pool cho kết nối nhanh
redis_fast_client: Redis = redis_async.from_url(
    REDIS_URL,
    # Giữ connection sống lâu hơn.
    socket_keepalive=True,
    # Timeout kết nối đến Redis.
    socket_connect_timeout=0.5,
    # Timeout cho command thường.
    # Không nên quá thấp như 0.2 vì dễ lỗi giả khi Redis hơi chậm.
    socket_timeout=1.0,
    # Giới hạn số connection của pool.
    max_connections=int(os.getenv("REDIS_MAX_CONNECTIONS", "200")),
    # Ping định kỳ để phát hiện connection chết.
    health_check_interval=30,

    # Nếu False: Redis trả bytes.
    # Nếu True: Redis trả str, code dễ đọc hơn, nhưng sẽ mất thời gian hơn
    
    decode_responses=False,
)


# ============================================================
# 2. Client stream: dùng riêng cho XREAD BLOCK
# ============================================================

redis_stream_client: Redis = redis_async.from_url(
    REDIS_URL,
    socket_keepalive=True,
    socket_connect_timeout=0.5,
    # Phải lớn hơn thời gian XREAD BLOCK.
    # Ví dụ STREAM_BLOCK_MS = 15000 thì socket_timeout nên > 15 giây.
    socket_timeout=float(os.getenv("REDIS_STREAM_SOCKET_TIMEOUT", "20")),
    max_connections=int(os.getenv("REDIS_STREAM_MAX_CONNECTIONS", "200")),
    health_check_interval=30,
    # Với notification/event JSON, True thường tiện hơn.
    decode_responses=True,
)


def get_redis_fast() -> Redis:
    """
    Trả Redis client dùng cho command nhanh.

    Dùng cho:
    - ping
    - get/set/delete
    - ws ticket
    - cache
    - xadd event
    - publish event

    Không tạo client mới mỗi lần gọi.
    """

    return redis_fast_client


def get_redis_stream() -> Redis:
    """
    Trả Redis client dùng cho stream blocking.

    Dùng cho:
    - XREAD BLOCK
    - các tác vụ chờ lâu

    Không dùng client fast cho XREAD BLOCK vì socket_timeout ngắn.
    """

    return redis_stream_client


async def close_redis_clients() -> None:
    """
    Đóng Redis clients khi app shutdown.

    Với redis.asyncio, nên đóng client async rõ ràng.
    redis-py docs cũng ghi rõ async Redis cần gọi close/aclose để đóng connection.
    """

    await redis_fast_client.aclose()
    await redis_stream_client.aclose()