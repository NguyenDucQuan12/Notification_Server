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


def _decode_field(value: Any) -> Any:
    """
    Giải mã 1 field đọc từ Redis Stream.

    Publisher (event_to_redis_fields) chỉ json.dumps cho dict/list, còn lại dùng str().
    Vì vậy CHỈ parse JSON khi chuỗi trông giống object/array ("{...}" hoặc "[...]").

    SỬA LỖI: code cũ json.loads() MỌI field, gây ra:
    - user_id "123"   -> số 123 (đổi kiểu, client so sánh "123" === 123 bị sai).
    - giá trị "NaN"/"Infinity" -> float nan/inf -> json.dumps sinh ra NaN,
      không phải JSON hợp lệ, trình duyệt JSON.parse() sẽ lỗi.
    Bây giờ các giá trị đơn (số, bool, chuỗi) được giữ NGUYÊN là chuỗi
    đúng như publisher đã ghi.
    """
    if isinstance(value, bytes):  # Phòng trường hợp client Redis không bật decode_responses.
        value = value.decode("utf-8", errors="replace")

    if isinstance(value, str):
        stripped = value.strip()
        if stripped[:1] in ("{", "[") and stripped[-1:] in ("}", "]"):
            try:
                return json.loads(stripped)
            except ValueError:
                return value  # JSON hỏng thì giữ nguyên chuỗi, không làm mất dữ liệu.
    return value


def fields_to_event(stream_id: str, fields: dict[str, Any]) -> dict[str, Any]:
    """
    Chuyển 1 entry của Redis Stream thành message gửi về client.

    Hợp đồng dữ liệu (contract) với client:
    - "event_id"  : LUÔN là Redis Stream ID (vd "1713000000000-0").
                    Client lưu giá trị này làm last_event_id để reconnect.
    - "stream_id" : giống event_id, thêm vào để tên gọi rõ nghĩa hơn.
    - "source_event_id": ID nghiệp vụ gốc (vd "evt_001") nếu publisher có ghi
                    field "event_id" riêng.

    SỬA LỖI: luồng Outbox của API server có ghi field "event_id" = ID nghiệp vụ.
    Code cũ đặt event_id = stream_id TRƯỚC rồi mới lặp fields, nên bị ghi đè
    thành "evt_001". Client lưu "evt_001" làm last_event_id -> server coi là
    không hợp lệ -> replay lại TOÀN BỘ stream mỗi lần reconnect.
    """

    event: dict[str, Any] = {"event_id": stream_id, "stream_id": stream_id}

    for key, value in fields.items():
        if isinstance(key, bytes):
            key = key.decode("utf-8", errors="replace")

        if key in ("event_id", "stream_id"):
            # Không cho dữ liệu nghiệp vụ ghi đè cursor. Giữ lại ID gốc ở field khác.
            if key == "event_id" and value != stream_id:
                event["source_event_id"] = _decode_field(value)
            continue

        event[key] = _decode_field(value)

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