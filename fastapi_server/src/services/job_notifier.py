"""
Thông báo cho luồng xử lý file/job.

Chỉ là lớp mỏng chọn nội dung + người nhận theo từng giai đoạn của job, rồi gọi
hàm dùng chung DUY NHẤT: services.notification_orchestrator.send_custom_notification().

| Giai đoạn      | notification_type | Gửi tới                                   | Lưu vào hộp thư? |
|----------------|-------------------|-------------------------------------------|------------------|
| API nhận file  | job.queued        | user tạo job + stream của job             | Có               |
| Worker bắt đầu | job.started       | stream của job                            | Không            |
| Đang xử lý     | job.progress      | stream của job                            | Không            |
| Lỗi, thử lại   | job.retrying      | stream của job                            | Không            |
| Thành công     | job.succeeded     | user tạo job + stream của job             | Có               |
| Thất bại hẳn   | job.failed        | user tạo job + stream của job + dashboard | Có               |

"Lưu vào hộp thư" = có người nhận trong notification_recipients (hiện trong /notifications/feed,
tính vào số chưa đọc). Các bước trung gian chỉ đẩy realtime tới màn hình đang mở job
(channel=job), tránh làm đầy hộp thư bằng hàng chục thông báo tiến độ.
"""
from __future__ import annotations

from typing import Any, Literal

from schemas.noti_schema import (
    NotificationAction,
    NotificationActionType,
    NotificationActorType,
    NotificationDispatchMode,
    NotificationPriority,
    NotificationResource,
    NotificationSeverity,
)
from services.notification_orchestrator import send_custom_notification

JobStage = Literal["queued", "started", "progress", "retrying", "succeeded", "failed"]

# Actor khi tiến trình nền (Job Worker) gửi thông báo.
JOB_WORKER_ACTOR: dict[str, Any] = {"user_id": "job-worker", "username": "Job Worker"}

# Thông báo trung gian sống ngắn; thông báo kết quả giữ lâu trong hộp thư.
_SHORT_TTL = 60 * 60
_LONG_TTL = 7 * 24 * 60 * 60


async def notify_job(
    job: dict[str, Any],
    stage: JobStage,
    *,
    current_user: dict[str, Any] | None = None,
    progress: int | None = None,
    message: str | None = None,
    error: str | None = None,
    mode: NotificationDispatchMode = NotificationDispatchMode.DIRECT,
) -> dict[str, Any]:
    """
    Gửi thông báo cho 1 giai đoạn của job. Không raise (send_custom_notification tự bắt lỗi).

    job: dict từ job_to_dict() (cần job_id, tenant_id, user_id, filename, trace_id).
    current_user: JWT payload nếu gọi từ API; None nếu gọi từ Job Worker (actor = hệ thống).
    """
    job_id = str(job["job_id"])
    tenant_id = str(job["tenant_id"])
    owner_id = str(job["user_id"])
    filename = str(job.get("filename") or "")

    if current_user is None:
        actor = dict(JOB_WORKER_ACTOR, trace_id=job.get("trace_id"))
        actor_type = NotificationActorType.SYSTEM
        source_instance = "job-worker"
    else:
        actor = current_user
        actor_type = NotificationActorType.USER
        source_instance = "job-controller"

    pct = progress if progress is not None else job.get("progress")

    # Mặc định: chỉ stream của job (không vào hộp thư).
    target_user_ids: list[str] = []
    send_to_dashboard = False
    severity = NotificationSeverity.INFO
    priority = NotificationPriority.NORMAL
    ttl = _SHORT_TTL
    action: NotificationAction | None = None
    dedup_key: str | None = None

    if stage == "queued":
        title = "Đã nhận file"
        body = f"File '{filename}' đã được tải lên và đang chờ xử lý."
        target_user_ids = [owner_id]
        ttl = _LONG_TTL
        dedup_key = f"job:{tenant_id}:{job_id}:queued"
        action = NotificationAction(
            type=NotificationActionType.NAVIGATE, target=f"/jobs/{job_id}", label="Xem tiến trình",
        )
    elif stage == "started":
        title = "Bắt đầu xử lý"
        body = message or f"Đang xử lý file '{filename}' (lần thử {job.get('attempts', 1)})."
    elif stage == "progress":
        title = f"Đang xử lý {pct}%"
        body = message or f"File '{filename}': {pct}%"
        priority = NotificationPriority.LOW
    elif stage == "retrying":
        title = "Xử lý lỗi, đang thử lại"
        body = message or f"File '{filename}' xử lý lỗi, hệ thống sẽ thử lại."
        severity = NotificationSeverity.WARNING
    elif stage == "succeeded":
        title = "Xử lý hoàn tất"
        body = message or f"File '{filename}' đã xử lý xong."
        target_user_ids = [owner_id]
        severity = NotificationSeverity.SUCCESS
        priority = NotificationPriority.HIGH
        ttl = _LONG_TTL
        dedup_key = f"job:{tenant_id}:{job_id}:succeeded"
        action = NotificationAction(
            type=NotificationActionType.NAVIGATE, target=f"/jobs/{job_id}/result", label="Tải kết quả",
        )
    elif stage == "failed":
        title = "Xử lý thất bại"
        body = message or f"File '{filename}' xử lý thất bại: {error or 'lỗi không xác định'}"
        target_user_ids = [owner_id]
        send_to_dashboard = True  # admin theo dõi tenant dashboard thấy được job lỗi
        severity = NotificationSeverity.ERROR
        priority = NotificationPriority.HIGH
        ttl = _LONG_TTL
        dedup_key = f"job:{tenant_id}:{job_id}:failed"
        action = NotificationAction(
            type=NotificationActionType.NAVIGATE, target=f"/jobs/{job_id}", label="Xem chi tiết",
        )
    else:  # pragma: no cover - Literal đã chặn
        raise ValueError(f"stage không hợp lệ: {stage}")

    return await send_custom_notification(
        tenant_id=tenant_id,
        current_user=actor,
        actor_type=actor_type,
        notification_type=f"job.{stage}",
        category="job",
        title=title,
        body=body,
        severity=severity,
        priority=priority,
        target_user_ids=target_user_ids,
        send_to_tenant_dashboard=send_to_dashboard,
        job_id=job_id,  # luôn gửi vào stream của job: stream:noti:tenant:<t>:job:<job_id>
        resource=NotificationResource(type="job", id=job_id),
        action=action,
        data={
            "job_id": job_id,
            "filename": filename,
            "status": job.get("status"),
            "stage": stage,
            "progress": pct,
            "attempts": job.get("attempts"),
            "error": error,
            "owner_user_id": owner_id,
        },
        template_variables={"filename": filename, "progress": pct},
        deduplication_key=dedup_key,
        ttl_seconds=ttl,
        causation_id=f"job:{job_id}:{stage}",
        source_instance=source_instance,
        mode=mode,
    )
