from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

def event_to_redis_fields(event: dict[str, Any]) -> dict[str, str]:
    """
    Chuyển event dict thành fields để lưu vào Redis Stream.

    Ví dụ:  
    ```
    {
        "event_id": "evt_001",
        "recipient_count": 2,
        "is_active": True,
        "metadata": {
            "warehouse_id": "WH01",
        },
    }
    ```
    Chứa các kiểu như str, int, datetime, ...  
    Ta cần chuyển hóa về dạng field-value kiểu str để ghi vào Redis Stream  
    ```
    {
        "event_id": "evt_001",
        "recipient_count": "2",
        "is_active": "True",
        "metadata": '{"warehouse_id": "WH01"}',
    }
    ```
    """

    fields: dict[str, str] = {}

    for key, value in event.items():
        if value is None:
            continue

        if isinstance(value, (dict, list)):
            fields[key] = json.dumps(value, ensure_ascii=False)
        else:
            fields[key] = str(value)

    return fields


def fields_to_event(stream_id: str, fields: dict[str, str]) -> dict[str, Any]:
    """
    Chuyển fields từ Redis Stream thành event gửi về client.

    stream_id:
    - là ID Redis Stream, ví dụ 1713000000000-0
    - client nên lưu lại event_id này để reconnect bằng last_event_id
    """

    event: dict[str, Any] = {
        "event_id": stream_id,
    }

    for key, value in fields.items():
        # Một số field có thể là JSON string.
        # Thử parse JSON, nếu lỗi thì giữ nguyên string.
        try:
            event[key] = json.loads(value)
        except Exception:
            event[key] = value

    return event
