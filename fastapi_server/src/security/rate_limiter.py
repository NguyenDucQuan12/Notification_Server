from typing import Tuple                 # # Kiểu trả về (bool, int)
import time
import asyncio
from log.system_log import system_logger
from services.redis_client import get_redis_fast      # # Hàm tạo client Redis

"""
Rate Limiter (dựa trên Redis + Lua script)
- Giới hạn số request trong "cửa sổ" thời gian gần nhất (sliding window). Có nghĩa là mỗi ip chỉ được gửi tối đa N request trong M giây gần nhất.
- Cách làm: dùng Redis Sorted Set (ZSET) để lưu timestamp (ms) của các request, với score = value = timestamp (ms).
- Mỗi lần có request mới:
    1. Thêm timestamp hiện tại vào ZSET.
    2. Xoá các timestamp cũ hơn (ngoài cửa sổ M giây).
    3. Đếm số phần tử còn lại trong ZSET (số request trong cửa sổ).
    4. So sánh với giới hạn N: nếu <= N thì cho phép, ngược lại từ chối.
- Để tránh ZSET quá lớn, ta set TTL tự dọn rác (gấp đôi cửa sổ).
- Viết bằng Lua script để chạy atomically trên Redis.
Redis rất nhanh, lệnh atomic khi gói vào Lua (không race giữa nhiều instance/app).
Có TTL (Time To Live: Thời gian sống) gốc (EXPIRE/PEXPIRE) để tự dọn key khi nguội. 

"""

# Lua "sliding window" dùng TIME của Redis (Để đảm bảo thời gian nhất quán) và member unique (now:seq)
SLIDING_WINDOW_LUA = r"""
-- KEYS[1] : key ZSET cho IP+bucket, ví dụ: rl:global:127.0.0.1
-- ARGV[1] : window_ms (độ dài cửa sổ M 000 ms = M s)
-- ARGV[2] : limit (số request tối đa trong cửa sổ: N request)

local key       = KEYS[1]

-- Lấy thời gian chuẩn từ Redis (tránh lệch giờ app, mỗi thiết bị lệch giờ sẽ khiến lệch theo, vì vậy tất cả lấy chung 1 nơi là redis server):
-- TIME trả { seconds, microseconds }
local t         = redis.call('TIME')
local now       = (t[1] * 1000) + math.floor(t[2] / 1000)

local window    = tonumber(ARGV[1])     
local limit     = tonumber(ARGV[2])

-- Tạo "member unique" để tránh va chạm khi có nhiều request trong cùng 1 ms:
-- Dùng counter riêng cho key, có TTL(Time To Live), để reset khi key nguội.
local seq_key   = key .. ':seq'
local seq       = redis.call('INCR', seq_key)
if seq == 1 then
  redis.call('PEXPIRE', seq_key, window * 2)
end
local member    = tostring(now) .. ':' .. tostring(seq)

-- (Bước 1) Ghi dấu request hiện tại (score = now, member = now:seq)
redis.call('ZADD', key, now, member)

-- (Bước 2) Cắt bỏ mọi dấu đã vượt ra khỏi cửa sổ [now-window, now]
-- (ở đây loại bỏ <= now - window; tức ta giữ (now-window, now])
redis.call('ZREMRANGEBYSCORE', key, 0, now - window)

-- (Bước 3) Đếm số request còn lại trong cửa sổ
local count = redis.call('ZCARD', key)

-- (Bước 4) Đặt TTL để key tự sạch khi nguội (không hoạt động sẽ tự động xóa các key hết hạn sau thời gian TTL)
redis.call('PEXPIRE', key, window * 2)

-- (Bước 5) Kiểm tra xem số lượng requets đã vượt qua giới hạn cho phép chưa: 1=allow, 0=deny; kèm count để log/giám sát
if count <= limit then
  return {1, count}
else
  return {0, count}
end
"""

# Biên dịch & cache script trên Redis (tăng hiệu năng)
_redis = get_redis_fast()

# Redis down -> skip kiểm tra trong COOL_DOWN giây
REDIS_RL_COOLDOWN_SECONDS = 5.0

# Timestamp đến khi nào thì sẽ skip Redis (circuit breaker)
_skip_until_mono: float = 0.0

_last_log_mono: float = 0.0
_LOG_EVERY_SECONDS = 1.0

# Throttle log: tối đa 1 log/giây
_LOG_EVERY_SECONDS = 1.0

# Lock tránh nhiều request đồng thời cùng register script / update state.
_STATE_LOCK = asyncio.Lock()
_SCRIPT_LOCK = asyncio.Lock()

async def _should_skip_redis() -> bool:
    """
    Trả True nếu đang trong thời gian skip Redis do lỗi trước đó.
    """
    
    async with _STATE_LOCK:
        return time.monotonic() < _skip_until_mono


async def _mark_redis_down() -> None:
    """
    Đánh dấu Redis down và skip trong REDIS_RL_COOLDOWN_SECONDS giây.
    """

    global _skip_until_mono

    async with _STATE_LOCK:
        _skip_until_mono = time.monotonic() + REDIS_RL_COOLDOWN_SECONDS


async def _log_redis_error_once(ex: Exception, op: str) -> None:
    """
    Log lỗi Redis có throttle để tránh spam log.
    """

    global _last_log_mono

    now = time.monotonic()

    async with _STATE_LOCK:
        if now - _last_log_mono < _LOG_EVERY_SECONDS:
            return

        _last_log_mono = now

    system_logger.warning(
        "RateLimiter Redis error at %s, skip %.1fs: %s: %s",
        op,
        REDIS_RL_COOLDOWN_SECONDS,
        type(ex).__name__,
        str(ex),
    )

# Chạy Script LUA an toàn trong trường hợp Redis sập
SLIDING_WINDOW_SCRIPT = None

async def _ensure_script():
    """
    Đảm bảo SLIDING_WINDOW_SCRIPT đã được tạo.

    Lưu ý:
    - register_script() không cần await.
    - Lỗi Redis thường xảy ra khi gọi script, không phải lúc register_script.
    """

    global SLIDING_WINDOW_SCRIPT

    if await _should_skip_redis():
        return None

    if SLIDING_WINDOW_SCRIPT is not None:
        return SLIDING_WINDOW_SCRIPT

    async with _SCRIPT_LOCK:
        # nếu đang skip redis thì tiếp tục bỏ qua không làm gì hết
        if await _should_skip_redis():
            return None

        # Nếu đã có thì trả về luôn
        if SLIDING_WINDOW_SCRIPT is not None:
            return SLIDING_WINDOW_SCRIPT

        try:
            # Ở đây không cần sử dụng redis với từ khóa await
            SLIDING_WINDOW_SCRIPT = _redis.register_script(SLIDING_WINDOW_LUA) # Register script trên Redis
            return SLIDING_WINDOW_SCRIPT

        except Exception as ex:
            await _log_redis_error_once(ex, "REGISTER_SCRIPT")   # Log lỗi
            await _mark_redis_down()                             # Bật skip Redis
            SLIDING_WINDOW_SCRIPT = None                         # Giữ None để lần sau thử lại
            return None


# # Tạo tên key rate-limit theo IP + nhóm (bucket)
def rl_key(ip: str, bucket: str) -> str:
    """
    Tạo các key tương ứng để kiểm soát mỗi lần gọi  
    Vì mỗi router sẽ có những ngưỡng khác nhau nên cần truyền bucket để phân loại.  
    Ví dụ: các api bucket: golbal sẽ có ngưỡng giới hạn 120 request/ 1 phút  
    Các api bucket: login/upload nghiêm ngặt hơn có ngưỡng 60 request/ 1 phút
    """
    return f"rl:{bucket}:{ip}"

# # Kiểm tra rate limit: trả (allowed, current_count)
async def rl_check(ip: str, bucket: str, window_ms: int, limit: int) -> Tuple[bool, int]:
    """
    Kiểm tra 1 request khi gọi api  
    - ip: địa chỉ ip gọi api  
    - bucket: phân loại api  
    - window_ms: Thời gian giới hạn 
    - limit: Số request giới hạn trong window_ms  
    Giả sử window_ms = 60_000 (60 giây), limit = 120 (tức ~2 rps trung bình).  
    Kiểm tra ip này có truy cập 1 api có bucket như trên vượt quá 120 request trong vòng 60s không
    - Redis OK: chạy Lua, trả kết quả thật
    - Redis down: fail-open -> return (True, 0)
    - Redis down sẽ skip trong COOL_DOWN giây, tránh mỗi request chờ timeout
    """
    # Nếu đang skip Redis -> fail-open ngay lập tức
    if await _should_skip_redis():
        return True, 0

    # Đảm bảo script đã register (nếu chưa có)
    await _ensure_script()

    # Nếu script vẫn None (do Redis down) -> fail-open
    if SLIDING_WINDOW_SCRIPT is None:
        return True, 0

    try:
        # Với redis.asyncio, gọi Lua script phải await.
        res = await SLIDING_WINDOW_SCRIPT(
            keys=[rl_key(ip, bucket)],
            args=[window_ms, limit],
        )
        allowed = (res[0] == 1)              # 1 -> allowed
        count = int(res[1])                  # số request trong cửa sổ
        return allowed, count
    
    except Exception as ex:
        # Redis lỗi trong lúc call script -> bật circuit breaker và fail-open
        await _log_redis_error_once(ex, "SCRIPT_CALL")
        await _mark_redis_down()
        return True, 0
