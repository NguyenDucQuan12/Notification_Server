"""
Các hàm tiện ích: xử lý Redis Stream ID và tạo các message hệ thống gửi cho client.

Các loại message hệ thống (field "type"), KHÔNG có event_id nên client không lưu:
- connected          : gửi ngay khi kết nối thành công.
- heartbeat          : gửi định kỳ khi không có event, báo kết nối còn sống.
- replay_incomplete  : client đã bỏ lỡ nhiều event hơn mức server replay được,
                       client nên gọi REST API để đồng bộ lại dữ liệu.
- pong               : trả lời khi client gửi "ping".
- server_error       : báo lỗi trước khi server đóng kết nối.
"""

from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from typing import Any

from utils.auth import Identity

# Redis Stream ID hợp lệ: "<milliseconds>-<sequence>", cả 2 là số nguyên KHÔNG âm.
# Cho phép dạng rút gọn "<milliseconds>" (Redis hiểu là "<ms>-0").
_STREAM_ID_RE = re.compile(r"^\d{1,20}(-\d{1,20})?$")

# Cursor bắt đầu từ đầu stream.
STREAM_START_ID = "0-0"


def utc_now_iso() -> str:
    """
    Trả về thời gian hiện tại theo UTC, định dạng ISO 8601. Ví dụ: 2026-11-08T03:00:00.123456Z
    """
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

def safe_json_dumps(data: dict[str, Any]) -> str:
    """
    Chuyển dict thành chuỗi JSON để gửi qua WebSocket.

    - ensure_ascii=False: tiếng Việt giữ nguyên, không bị đổi thành \\u1ea1...
    - allow_nan=False   : chặn NaN/Infinity (không phải JSON hợp lệ với trình duyệt).
    - default=str       : kiểu lạ (datetime, Decimal...) tự chuyển thành chuỗi thay vì crash.
    """
    return json.dumps(data, ensure_ascii=False, allow_nan=False, default=str)

def is_valid_stream_id(event_id: str | None) -> bool:
    """
    Kiểm tra last_event_id client gửi lên có đúng định dạng Redis Stream ID không.
    """
    return bool(event_id) and bool(_STREAM_ID_RE.match(event_id))

def normalize_last_event_id(value: str | None) -> str:
    """
    Chuẩn hóa last_event_id client gửi lên.

    Không gửi hoặc gửi sai định dạng -> "0-0" (đọc từ đầu stream, nhưng vẫn bị
    giới hạn bởi REPLAY_MAX_EVENTS nên không bị dội quá nhiều event).
    Không bao giờ đưa chuỗi chưa kiểm tra vào lệnh Redis.
    """
    if value and is_valid_stream_id(value):
        # Dạng rút gọn "123" -> "123-0" để mọi lệnh Redis nhận ID đầy đủ.
        return value if "-" in value else f"{value}-0"
    return STREAM_START_ID

def compare_stream_ids(a: str, b: str) -> int:
    """
    So sánh 2 Stream ID theo thứ tự thời gian. Trả -1 (a<b), 0 (bằng), 1 (a>b).

    Không so sánh chuỗi trực tiếp được vì "9-0" > "10-0" theo thứ tự chữ.
    """
    def _parts(value: str) -> tuple[int, int]:
        ms, _, seq = value.partition("-")
        return int(ms), int(seq or 0)

    pa, pb = _parts(a), _parts(b)
    return (pa > pb) - (pa < pb)

# ----------------------------------------------------------------------
# Các message hệ thống
# ----------------------------------------------------------------------

def build_connected_message(
    *,
    channel: str,
    last_event_id: str,
    identity: Identity,
    connection_id: str,
    heartbeat_interval_ms: int,
) -> dict[str, Any]:
    """
    Gửi ngay sau khi kết nối thành công.

    connection_id: mã kết nối, khi báo lỗi cho đội vận hành chỉ cần đưa mã này
    là tìm được toàn bộ log liên quan.
    heartbeat_interval_ms: client dùng để tự phát hiện mất kết nối, ví dụ
    "quá 2 lần khoảng này không nhận được gì thì tự reconnect".
    """
    return {
        "type": "connected",
        "channel": channel,
        "connection_id": connection_id,
        "tenant_id": identity.tenant_id,
        "user_id": identity.user_id,
        "last_event_id": last_event_id,
        "heartbeat_interval_ms": heartbeat_interval_ms,
        "server_time": utc_now_iso(),
    }

def build_heartbeat_message(channel: str, *, stream_healthy: bool = True) -> dict[str, Any]:
    """
    Heartbeat không phải event từ Redis nên không có event_id, client không cần lưu.

    stream_status:
    - "ok"       : server đang đọc Redis bình thường.
    - "degraded" : server tạm mất kết nối Redis. Kết nối WebSocket VẪN giữ, event sẽ
                   được gửi bù khi Redis trở lại. Giao diện có thể hiện "Đang kết nối lại...".
    """
    return {
        "type": "heartbeat",
        "channel": channel,
        "stream_status": "ok" if stream_healthy else "degraded",
        "server_time": utc_now_iso(),
    }

def build_replay_incomplete_message(
    *,
    channel: str,
    reason: str,
    requested_last_event_id: str,
    resumed_from_event_id: str | None,
) -> dict[str, Any]:
    """
    Báo client biết có thể đã mất một số event.

    reason:
    - "limit"   : client bỏ lỡ nhiều hơn REPLAY_MAX_EVENTS, server chỉ gửi phần mới nhất.
    - "trimmed" : event cũ đã bị Redis xóa do giới hạn MAXLEN của stream.

    Client nên gọi REST API (vd GET danh sách notification) để đồng bộ lại.
    """
    return {
        "type": "replay_incomplete",
        "channel": channel,
        "reason": reason,
        "requested_last_event_id": requested_last_event_id,
        "resumed_from_event_id": resumed_from_event_id,
        "server_time": utc_now_iso(),
    }

def build_pong_message() -> dict[str, Any]:
    """
    Trả lời khi client gửi "ping". Chỉ báo kết nối còn sống.
    """
    return {"type": "pong", "server_time": utc_now_iso()}

def build_server_error_message(message: str, *, code: str = "internal_error") -> dict[str, Any]:
    """
    Báo lỗi cho client trước khi đóng kết nối.

    BẢO MẬT: chỉ gửi thông báo chung chung. Chi tiết lỗi (exception, địa chỉ Redis...)
    chỉ ghi vào log server. Code cũ gửi nguyên str(exception) cho client, có thể
    làm lộ thông tin hạ tầng.
    """
    return {
        "type": "server_error",
        "code": code,
        "message": message,
        "server_time": utc_now_iso(),
    }
