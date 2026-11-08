from datetime import datetime, timezone
from zoneinfo import ZoneInfo
import json
from typing import Any
from uuid import uuid4



VIETNAM_TIMEZONE = ZoneInfo("Asia/Ho_Chi_Minh")

def utc_now() -> datetime:
    """
    Trả về thời gian hiện tại theo UTC.

    Dùng UTC để tránh lệch giờ giữa:
    - API service
    - worker service
    - notification service
    - database server
    Kết quả trả về sẽ là: `2026-06-23 06:32:34.2458650 +00:00` múi giờ `+00:00`
    """
    return datetime.now(timezone.utc)

def format_vietnam_time(value: datetime) -> str:
    """
    Chuyển datetime UTC sang giờ Việt Nam để hiển thị.

    Định dạng:
        YYYY-MM-DD HH:MM:SS.mmm
    """
    if value.tzinfo is None:
        # Giả định datetime không có timezone đang là UTC.
        value = value.replace(tzinfo=timezone.utc)

    vietnam_time = value.astimezone(VIETNAM_TIMEZONE)

    return vietnam_time.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

def create_public_id(prefix: str) -> str:
    """
    Sinh ID public.

    Ví dụ:
    - noti_7d91...
    - evt_6af2...
    - delivery_a83f...
    """

    return f"{prefix}_{uuid4().hex}"


def enum_value(value: Any) -> str:
    """
    Lấy giá trị string từ Enum hoặc từ string thông thường.

    Hỗ trợ cả hai trường hợp:

        class NotificationPriority(str, Enum):
            LOW = "low"
            NORMAL = "normal"
            HIGH = "high"

        "normal"

    Điều này tránh lỗi:

        AttributeError: 'str' object has no attribute 'value'

    trong trường hợp Pydantic được cấu hình use_enum_values=True.
    """

    if hasattr(value, "value"):
        return str(value.value)

    return str(value)

def json_dumps(value: Any) -> str:
    """
    Chuyển object Python thành JSON string.  
    Ví dụ:  
    ```
    data = {
        "role_id": uuid4(),
        "role_name": "Nhân viên kho",
        "created_at": datetime.now(timezone.utc),
        "permissions": [
            "warehouse:read",
            "warehouse:update",
        ],
        "is_active": True,
    }

    json_string = json_dumps(data)

    print(json_string)
    ```
    Sẽ cho ra mỗi chuỗi (string): `{"role_id":"242ebdc1-20d5-43d9-aa23-6a159d61bb5b","role_name":"Nhân viên kho","created_at":"2026-06-23 06:32:34.245865+00:00","permissions":["warehouse:read","warehouse:update"],"is_active":true}`  
    Có thể chuyển ngược lại `string` thành `dict` bằng: `restored_data = json.loads(json_string)`
    """

    return json.dumps(
        value,
        ensure_ascii=False,       # Giữ nguyên các ký tự có dấu (như tiếng Việt)
        separators=(",", ":"),    # Xóa các khoảng trắng sau dấu phẩy và dấu hai chấm
        default=str,              # Hỗ trợ datetime, UUID hoặc các object có thể chuyển thành string trước khi đưa vào json.dum để không lỗi
    )

def parse_permissions(
    permissions_value: Any,
) -> list[str]:
    """
    Chuẩn hóa permissions thành list[str].  
    Trong DB sẽ lưu: `'["warehouse:read","warehouse:update"]'`  
    Nhưng trong code sẽ là dạng list:  
    ```
    [
        "warehouse:read",
        "warehouse:update",
    ]
    ```
    Vì vậy ta cần chuyển từ dạng string từ DB để có thể sử dụng trong code
    """
    # Nếu giá trị là None thì trả về list rỗng
    if permissions_value is None:
        return []

    # Nếu giá trị là list sẵn thì bỏ các khoảng trắng thừa bằng, và sắp xếp theo chữ cái rồi tự động trả về dạng list (sort trả về list)
    if isinstance(permissions_value, list):
        return sorted(
            {
                str(item).strip()
                for item in permissions_value
                if str(item).strip()
            }
        )

    # Nếu là chuỗi Json:  '["warehouse:update","warehouse:read"]'
    if isinstance(permissions_value, str):
        try:
            # Đọc chuỗi và đưa về list
            parsed = json.loads(permissions_value)
        except json.JSONDecodeError:
            return []

        if not isinstance(parsed, list):
            return []

        # Xử lý tương tự như trên
        return sorted(
            {
                str(item).strip()
                for item in parsed
                if str(item).strip()
            }
        )

    return []

def parse_permissions_from_role(role: dict | None) -> list[str]:
    """
    Lấy permissions từ role.permissions_json.  
    Dùng cho việc tạo auth  
    Nếu role không có hoặc JSON lỗi thì trả list rỗng.
    """

    if not role:
        return []

    permissions_json = role.get("permissions_json") or "[]"

    try:
        permissions = json.loads(permissions_json)
    except Exception:
        return []

    if not isinstance(permissions, list):
        return []

    return [str(p) for p in permissions]