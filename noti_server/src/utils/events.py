from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any


def utc_now_iso() -> str:
    """
    Trả thời gian UTC dạng ISO string.

    Ví dụ:
    2026-06-06T04:30:00.000000+00:00
    """

    return datetime.now(timezone.utc).isoformat()


def event_to_redis_fields(event: dict[str, Any]) -> dict[str, str]:
    """
    Chuyển event dict thành fields để lưu vào Redis Stream.

    Redis Stream lưu field-value dạng string.
    Vì vậy object/list/dict sẽ được json.dumps.
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


def build_job_event(
    *,
    event_type: str,
    tenant_id: str,
    user_id: str,
    job_id: str,
    status: str,
    progress: int,
    message: str,
    filename: str | None = None,
    result_object_key: str | None = None,
    error: str | None = None,
    trace_id: str | None = None,
) -> dict[str, Any]:
    """
    Tạo event chuẩn cho job.

    event_type nên là:
    - job.created
    - job.queued
    - job.processing
    - job.progress
    - job.success
    - job.failed
    """

    return {
        "type": event_type,
        "tenant_id": tenant_id,
        "user_id": user_id,
        "job_id": job_id,
        "filename": filename,
        "status": status,
        "progress": progress,
        "message": message,
        "result_object_key": result_object_key,
        "error": error,
        "trace_id": trace_id,
        "created_at": utc_now_iso(),
    }


def build_role_event(
    *,
    event_type: str,
    role_id: str,
    role_name: str,
    message: str,
    actor_user_id: str | None = None,
    actor_username: str | None = None,
    description: str | None = None,
    permissions_json: str | None = None,
) -> dict[str, Any]:
    """
    Tạo event chuẩn cho các thao tác role.

    event_type nên là:
    - role.created
    - role.updated
    - role.activated
    - role.deactivated
    - role.deleted

    actor_user_id:
    - user_id của người thực hiện thao tác.

    actor_username:
    - username của người thực hiện thao tác.

    permissions_json:
    - quyền của role tại thời điểm tạo/cập nhật.
    """

    return {
        "type": event_type,
        "scope": "global",
        "entity": "role",
        "role_id": role_id,
        "role_name": role_name,
        "description": description,
        "permissions_json": permissions_json,
        "actor_user_id": actor_user_id,
        "actor_username": actor_username,
        "message": message,
        "created_at": utc_now_iso(),
    }