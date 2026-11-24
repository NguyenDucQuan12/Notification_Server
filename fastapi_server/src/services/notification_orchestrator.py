"""
Dịch vụ xử lý thông báo (Notification Orchestrator)

Luồng tạo thông báo (notification) trong hệ thống:
1. Mọi nơi (API nghiệp vụ, Job Worker...) gọi DUY NHẤT hàm send_custom_notification()
   (role.created, role.updated, job.queued, job.progress, job.succeeded, ...).
2. Hàm này tạo NotificationEvent, xác định các Redis Stream đích (NotificationStreamTarget), và ghi vào DB (notifications, notification_recipients, notification_deliveries, notification_outbox).
3. Nếu mode là DIRECT và số lượng outbox nhỏ, hàm sẽ publish ngay các outbox vào Redis Stream.
4. Notification Server đọc Redis Stream và đẩy thông báo tới client qua WebSocket.
"""
from __future__ import annotations
from typing import Any

from db.database import AsyncSessionLocal
from db.query_users import get_admin_user_ids
from db.query_notifications import add_notification_event_to_session
from services.notification_event_service import build_custom_notification_event
from services.notification_publisher import (
    NotificationStreamTarget,
    deduplicate_targets,
    publish_multiple_outboxes_immediately,
    wake_outbox_worker,
)
from schemas.noti_schema import (
    NotificationAction,
    NotificationActorType,
    NotificationDispatchMode,
    NotificationPriority,
    NotificationResource,
    NotificationSeverity,
)
from utils.constants import DIRECT_PUBLISH_MAX_OUTBOX


async def _dispatch(outbox_ids: list[str], mode: "NotificationDispatchMode") -> dict[str, Any]|None:
    """
    Đẩy thông báo ngay nếu mode là `DIRECT` và số outbox nhỏ; ngược lại để Outbox Worker xử lý
    """
    # Đánh thức Outbox Worker để publish các outbox pending (nếu có)
    # Worker sẽ quét theo chu kỳ poll, nhưng wake_up giúp giảm độ trễ.
    if mode != NotificationDispatchMode.DIRECT:
        await wake_outbox_worker()
        return None

    if len(outbox_ids) > DIRECT_PUBLISH_MAX_OUTBOX:
        # Nếu số lượng outbox vượt ngưỡng thì không publish ngay, để worker publish theo lô ở nền.
        await wake_outbox_worker()
        return {
            "deferred_to_worker": True,
            "reason": (
                f"Số lượng thông báo ({len(outbox_ids)}) vượt ngưỡng cấu hình ({DIRECT_PUBLISH_MAX_OUTBOX}), sẽ để Outbox Worker đẩy theo lô ở nền."
            ),
        }
    return await publish_multiple_outboxes_immediately(outbox_ids=outbox_ids)

async def send_custom_notification(
    *,
    tenant_id: str,
    title: str,
    body: str,
    current_user: dict[str, Any],
    target_user_ids: list[str] | None = None,
    include_tenant_admins: bool = False,
    send_to_tenant_dashboard: bool = False,
    send_to_global: bool = False,
    job_id: str | None = None,
    severity: NotificationSeverity = NotificationSeverity.INFO,
    priority: NotificationPriority = NotificationPriority.NORMAL,
    notification_type: str = "system.message",
    category: str = "system",
    data: dict[str, Any] | None = None,
    mode: NotificationDispatchMode = NotificationDispatchMode.DIRECT,
    # ---- Metadata tùy chọn cho thông báo nghiệp vụ ----
    short_body: str | None = None,
    template_variables: dict[str, Any] | None = None,
    resource: NotificationResource | None = None,
    action: NotificationAction | None = None,
    deduplication_key: str | None = None,
    ttl_seconds: int = 24 * 60 * 60,
    causation_id: str | None = None,
    source_instance: str = "notification-api",
    actor_type: NotificationActorType = NotificationActorType.USER,
) -> dict[str, Any]:
    """
    Gửi 1 thông báo qua ĐÚNG luồng chuẩn của hệ thống:

        SQL Server (notifications + recipients + deliveries + outbox)
            -> publish outbox vào Redis Stream (DIRECT) hoặc để worker publish (WORKER)
            -> Notification Server đọc stream -> đẩy WebSocket tới client

    Người nhận (có thể kết hợp nhiều loại):
    - target_user_ids            -> stream cá nhân của từng user  (gửi cho người dùng / nhóm)
    - include_tenant_admins=True -> thêm toàn bộ admin của tenant vào target_user_ids
    - send_to_tenant_dashboard   -> stream dashboard của tenant     (toàn bộ tenant)
    - send_to_global             -> stream global                   (toàn hệ thống / monitoring)
    - job_id                     -> stream của job

    current_user: JWT payload của người thực hiện. Tiến trình nền (Job Worker) truyền
    {"user_id": "job-worker", "username": "Job Worker"} kèm actor_type=SYSTEM.
    """
    try:
        # Lấy id của người đang thực hiện hành động
        actor_user_id = str(current_user["user_id"])

        # Session riêng: lỗi thông báo chỉ rollback session này, không ảnh hưởng session nghiệp vụ.
        async with AsyncSessionLocal() as notification_db:
            async with notification_db.begin():
                # Danh sách người nhận là tập hợp các user_id từ target_user_ids
                recipient_user_ids: list[str] = [str(u) for u in (target_user_ids or [])]

                # Thêm toàn bộ admin của tenant vào danh sách người nhận (nếu cần).
                if include_tenant_admins:
                    admin_user_ids = await get_admin_user_ids(notification_db, tenant_id=tenant_id)
                    recipient_user_ids.extend(str(u) for u in admin_user_ids)

                # Loại trùng nhưng giữ thứ tự.
                recipient_user_ids = list(dict.fromkeys(u for u in recipient_user_ids if u.strip()))

                # Danh sách stream đích (NotificationStreamTarget) để gửi thông báo tới.
                targets: list[NotificationStreamTarget] = [
                    NotificationStreamTarget.user(tenant_id=tenant_id, user_id=user_id)
                    for user_id in recipient_user_ids
                ]
                if send_to_tenant_dashboard:
                    targets.append(NotificationStreamTarget.tenant(tenant_id=tenant_id))
                if send_to_global:
                    targets.append(NotificationStreamTarget.global_scope())
                if job_id:
                    targets.append(NotificationStreamTarget.job(tenant_id=tenant_id, job_id=job_id))

                # Loại bỏ trùng lặp stream đích.
                targets = deduplicate_targets(targets)

                if not targets:
                    raise ValueError("Thông báo không có stream đích nào, vui lòng chỉ định ít nhất 1 user/tenant/global/job.")

                # Tạo NotificationEvent hoàn chỉnh, bao gồm notification_id, event_id, nội dung, actor, ...
                event = build_custom_notification_event(
                    tenant_id=tenant_id,
                    title=title,
                    body=body,
                    recipient_user_ids=recipient_user_ids,
                    actor_user_id=actor_user_id,
                    actor_username=current_user.get("user_name"),
                    notification_type=notification_type,
                    category=category,
                    severity=severity,
                    priority=priority,
                    data=data,
                    job_id=job_id,
                    correlation_id=current_user.get("correlation_id"),
                    trace_id=current_user.get("trace_id"),
                    short_body=short_body,
                    template_variables=template_variables,
                    resource=resource,
                    action=action,
                    deduplication_key=deduplication_key,
                    ttl_seconds=ttl_seconds,
                    causation_id=causation_id,
                    source_instance=source_instance,
                    actor_type=actor_type,
                )

                # Lưu thông tin của thông báo vào cơ sở dữ liệu: notifications, notification_recipients, notification_deliveries, notification_outbox.
                result = await add_notification_event_to_session(
                    db=notification_db,
                    event=event,
                    stream_targets=targets,
                )

        response: dict[str, Any] = {
            "status": "stored",
            "mode": mode.value,
            "notification_id": result.notification_id,
            "event_id": result.event_id,
            "outbox_ids": result.outbox_ids,
            "stream_count": len(targets),
            # Chỉ trả tối đa 10 stream để response không phình to khi gửi cho hàng nghìn người.
            "stream_keys": [t.get_stream_key() for t in targets[:10]],
            "recipient_count": result.recipient_count,
            "delivery_count": result.delivery_count,
            "created_at": result.created_at.isoformat(),
        }

        # DIRECT: publish ngay qua outbox (claim nguyên tử -> XADD -> published),
        # nên worker sẽ không publish trùng. Redis lỗi thì outbox quay lại pending, worker retry.
        publish_result = await _dispatch(result.outbox_ids, mode)
        if publish_result is not None:
            response["publish"] = publish_result

        return response

    except Exception as exc:  # pylint: disable=broad-except
        # Thay print bằng system_logger.exception trong production.
        print(
            f"Không thể tạo thông báo {notification_type}: "
            f"{type(exc).__name__}: {exc}",
            flush=True,
        )
        return {
            "status": "notification_failed",
            "mode": mode.value,
            "error_type": type(exc).__name__,
            "error_message": str(exc),
        }
