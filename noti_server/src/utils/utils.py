from __future__ import annotations
import json
from typing import Any
from datetime import datetime, timezone    

from utils.auth import Identity


def utc_now_iso() -> str:
    """
    Lấy thời gian hiện tại theo UTC, định dạng ISO 8601.

    Ví dụ: 2024-09-01T12:00:00Z
    """

    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

def safe_json_dumps(data: dict[str, Any]) -> str:
    """
    Chuyển dict thành JSON string để gửi qua WebSocket.

    ensure_ascii=False giúp tiếng Việt hiển thị bình thường,
    không bị chuyển thành dạng \\u....
    """

    return json.dumps(data, ensure_ascii=False)

def parse_stream_id(event_id: str) -> tuple[int, int]:
    """
    Redis Stream ID có dạng:
    <milliseconds>-<sequence>

    Ví dụ:
    1713000000000-0
    """

    major, minor = event_id.split("-", 1)
    return int(major), int(minor)

def is_valid_stream_id(event_id: str | None) -> bool:
    """
    Kiểm tra last_event_id client gửi lên có đúng định dạng Redis Stream ID không.

    Nếu sai thì không đưa trực tiếp vào XRANGE/XREAD.
    """

    if not event_id:
        return False

    try:
        parse_stream_id(event_id)
        return True
    except Exception:
        return False
    
def normalize_last_event_id(value: str | None) -> str:
    """
    Chuẩn hóa last_event_id.

    Nếu client không gửi hoặc gửi sai thì dùng 0-0,
    nghĩa là replay từ đầu stream.
    """

    if is_valid_stream_id(value):
        return value or "0-0"

    return "0-0"

def build_connected_message(
    *,
    channel: str,
    last_event_id: str,
    identity: Identity,
) -> dict[str, Any]:
    """
    Message gửi ngay sau khi WebSocket accept thành công.

    Client dùng message này để biết:
    - đã kết nối thành công
    - đang nghe channel nào
    - server_time hiện tại
    """

    return {
        "type": "connected",
        "channel": channel,
        "tenant_id": identity.tenant_id,
        "user_id": identity.user_id,
        "last_event_id": last_event_id,
        "server_time": utc_now_iso(),
    }

def build_heartbeat_message(channel: str) -> dict[str, Any]:
    """
    Heartbeat báo cho client biết kết nối vẫn còn sống.

    Client không cần lưu last_event_id của heartbeat,
    vì heartbeat không phải event lấy từ Redis Stream.
    """

    return {
        "type": "heartbeat",
        "channel": channel,
        "server_time": utc_now_iso(),
    }

def build_server_error_message(message: str) -> dict[str, Any]:
    """
    Message lỗi server gửi về client trước khi đóng WebSocket.
    """

    return {
        "type": "server_error",
        "message": message,
        "server_time": utc_now_iso(),
    }