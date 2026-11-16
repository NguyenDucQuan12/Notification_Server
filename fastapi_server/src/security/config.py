"""
Cấu hình bảo mật (security) cho FastAPI server
- Rate limit (giới hạn số request/giây)
"""
import os
from dataclasses import dataclass  # # Dùng dataclass cho nhóm TTL gọn gàng

@dataclass(frozen=True)
class TTLConfig:
    """
    Gom TTL (Time To Live) vào 1 struct:
    - metric_minute: Thời gian (giây) cho các counter theo phút (req/min, 5xx/min, bans/min)
    - suspicious_5min: Thời gian (giây) cho điểm nghi vấn (cửa sổ 5 phút)
    - ban_seconds: Thời gian (giây) khi BAN 1 IP
    - rl_expire_multiplier: nhân Thời cho key rate-limit (ZSET + seq) so với window
    """
    metric_minute: int = 180
    suspicious_5min: int = 300
    ban_seconds: int = 60 * 1  # Mặc định ban 15 phút
    rl_expire_multiplier: int = 2

# Cấu hình TTL mặc định (có thể thay bằng biến môi trường khi cần)
TTL = TTLConfig()

# Map path -> bucket (để áp threshold riêng theo endpoint nhạy cảm), các endpoint này áp dụng các biện pháp khắt khe hơn
# Ví dụ các api public thì giới hạn : 120 request trong 60 giây, còn các api này giới hạn 10 request trong 60 giây
BUCKET_BY_PATH = {
    "/login": "login",
    "/auth/login": "login",     # API login thật của hệ thống (bản cũ thiếu => login không bị giới hạn riêng)
    "/auth/token": "login",
    "/upload": "upload",
    "/api/upload": "upload",
}

# Rate limit theo bucket (window_ms + limit)
# Giới hạn tính theo MỖI IP. Nếu nhiều người dùng đi ra Internet qua CÙNG 1 IP (NAT văn phòng,
# reverse proxy chưa cấu hình TRUSTED_PROXIES...) thì phải tăng các giá trị này, nếu không
# cả văn phòng sẽ bị chặn/BAN chung. Cấu hình qua biến môi trường trong env/.env.api.
RATE = {
    "global": dict(window_ms=60_000, limit=int(os.getenv("RATE_LIMIT_GLOBAL_PER_MIN", "120"))),
    "login":  dict(window_ms=60_000, limit=int(os.getenv("RATE_LIMIT_LOGIN_PER_MIN", "10"))),
    "upload": dict(window_ms=60_000, limit=int(os.getenv("RATE_LIMIT_UPLOAD_PER_MIN", "20"))),
}

# IP không bao giờ bị rate-limit / BAN (máy chủ nội bộ, máy chạy load test, health check...).
# Ví dụ: RATE_LIMIT_EXEMPT_IPS=127.0.0.1,10.0.0.5
RATE_LIMIT_EXEMPT_IPS = frozenset(
    ip.strip() for ip in os.getenv("RATE_LIMIT_EXEMPT_IPS", "").split(",") if ip.strip()
)

# Luật BAN dựa trên điểm nghi vấn trong 5 phút
BAN_RULE = dict(
    suspicious_per_5min=15,   # # ≥ 15 sự kiện nghi vấn/5 phút -> BAN
    ban_ttl_sec=TTL.ban_seconds
)

# Patterns nghi vấn cơ bản (SQLi/XSS)
# Nếu trong chuỗi api người dùng gửi lên có chứa các từ này thì gọi là nghi vấn
SUSPICIOUS_PATTERNS = [
    "../", "<script", " onerror=", "javascript:", "union select",
    "' or 1=1", "--", "/*", "*/", "xp_cmdshell"
]
