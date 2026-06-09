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
    "/auth/token": "login",
    "/upload": "upload",
    "/api/upload": "upload",
}

# Rate limit theo bucket (window_ms + limit)
RATE = {
    "global": dict(window_ms=60_000, limit=120),  # 120 req / 60s / IP
    "login":  dict(window_ms=60_000, limit=10),   # /login chặt hơn, chỉ cho phép 10 req/60s/ip
    "upload": dict(window_ms=60_000, limit=20),   # /upload trung bình
}

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
