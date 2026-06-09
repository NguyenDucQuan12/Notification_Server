"""
Tập trung hoá việc tạo TÊN KHOÁ (key) Redis.
Mọi nơi khác chỉ GỌI HÀM ở đây -> nếu đổi format key, ta chỉ sửa file này.
"""

from typing import Union

# ===== Metric per-minute (đếm theo phút) =====

def k_metric_req(minute_bucket: int) -> str:
    """Đếm tổng request của phút minute_bucket (floor(epoch/60))."""
    return f"metric:req:{minute_bucket}"

def k_metric_5xx(minute_bucket: int) -> str:
    """Đếm tổng response 5xx của phút minute_bucket."""
    return f"metric:5xx:{minute_bucket}"

def k_metric_bans(minute_bucket: int) -> str:
    """Đếm tổng số lần BAN của phút minute_bucket."""
    return f"metric:bans:{minute_bucket}"

# ===== BAN / NOTIFY =====

def k_ban_ip(ip: str) -> str:
    """Flag IP đang bị BAN."""
    return f"ban:ip:{ip}"

def k_ban_notify(ip: str) -> str:
    """Key khoá chống spam email khi IP bị BAN (1 email/chu kỳ BAN)."""
    return f"ban:notify:{ip}"

# ===== Suspicious score (điểm nghi vấn theo 5 phút) =====

def k_suspicious(ip: str) -> str:
    """Điểm nghi vấn tích luỹ của IP trong cửa sổ 5 phút (có TTL)."""
    return f"sus:ip:{ip}:5min"

# ===== Rate-limit (sliding window) =====
# Dùng 2 key: ZSET chính + seq counter (member unique).
# Nếu sau này dùng Redis Cluster, thay đổi giá trị trả về của hai hàm dưới:
# return f"rl:{{{bucket}:{ip}}}"         # ZSET
# return f"rl:{{{bucket}:{ip}}}:seq"     # SEQ

def k_rl(ip: str, bucket: str) -> str:
    """
    Key ZSET cho sliding-window rate-limit của 1 IP trong 1 bucket.
    Ví dụ: rl:global:203.0.113.10
    """
    return f"rl:{bucket}:{ip}"

def k_rl_seq(ip: str, bucket: str) -> str:
    """Key counter seq tương ứng để tạo member unique (now:seq)."""
    return f"rl:{bucket}:{ip}:seq"
