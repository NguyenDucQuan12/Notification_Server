from datetime import datetime, timezone
from typing import Any
import json

def utc_now() -> datetime:
    """
    Trả về thời gian hiện tại theo UTC.

    Dùng UTC để tránh lệch giờ giữa:
    - API service
    - worker service
    - notification service
    - database server
    """
    return datetime.now(timezone.utc)


def parse_permissions_from_role(role: dict | None) -> list[str]:
    """
    Lấy permissions từ role.permissions_json.

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