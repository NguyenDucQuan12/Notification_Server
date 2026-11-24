"""
Bảng mã đóng (close code) WebSocket mà Notification Server sử dụng.

Quy ước của chuẩn RFC 6455:
- 1000-2999: mã chuẩn của giao thức (1000 bình thường, 1011 lỗi server, ...).
- 4000-4999: dành cho ỨNG DỤNG tự định nghĩa. Ta mô phỏng theo mã HTTP
  cho dễ nhớ: 4401 ~ HTTP 401, 4403 ~ HTTP 403, 4429 ~ HTTP 429.

Client nên xử lý theo nhóm:
┌───────┬──────────────────────────┬─────────────────────────────────────────────┐
│ Code  │ Ý nghĩa                  │ Client nên làm gì                           │
├───────┼──────────────────────────┼─────────────────────────────────────────────┤
│ 1000  │ Đóng bình thường         │ Không cần làm gì                            │
│ 1008  │ Client gửi quá nhiều msg │ Sửa client, reconnect sau vài giây          │
│ 1011  │ Lỗi nội bộ server        │ Đợi vài giây (backoff), xin ticket mới      │
│ 1012  │ Server đang khởi động lại│ Đợi 1-5 giây, xin ticket mới, reconnect     │
│ 1013  │ Server quá tải/Redis lỗi │ Backoff lâu hơn (5-30 giây) rồi thử lại     │
│ 4400  │ Request sai (vd job_id)  │ Sửa request, KHÔNG tự reconnect             │
│ 4401  │ Ticket thiếu/sai/hết hạn │ Xin ticket mới rồi reconnect                │
│ 4403  │ Không đủ quyền/sai channel│ KHÔNG reconnect, báo người dùng            │
│ 4408  │ Client nhận quá chậm     │ Kiểm tra mạng, reconnect với last_event_id  │
│ 4429  │ Quá nhiều kết nối/user   │ Đóng bớt tab/kết nối cũ                     │
└───────┴──────────────────────────┴─────────────────────────────────────────────┘

Lý do (reason) gửi kèm là mã ngắn dạng snake_case (vd "ticket_invalid_or_expired")
vì chuẩn WebSocket giới hạn reason tối đa 123 byte và để client dễ so sánh bằng code.
"""

NORMAL = 1000
POLICY_VIOLATION = 1008
INTERNAL_ERROR = 1011
SERVICE_RESTART = 1012
TRY_AGAIN_LATER = 1013

BAD_REQUEST = 4400
UNAUTHORIZED = 4401
FORBIDDEN = 4403
SEND_TIMEOUT = 4408
TOO_MANY_CONNECTIONS = 4429

# Giới hạn độ dài reason theo chuẩn RFC 6455 (125 byte payload - 2 byte code).
MAX_REASON_BYTES = 123


def safe_reason(reason: str) -> str:
    """Cắt reason cho vừa 123 byte UTF-8 mà không làm vỡ ký tự tiếng Việt."""
    encoded = reason.encode("utf-8")
    if len(encoded) <= MAX_REASON_BYTES:
        return reason
    return encoded[:MAX_REASON_BYTES].decode("utf-8", errors="ignore")
