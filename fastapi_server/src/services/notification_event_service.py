"""
Dịch vụ tạo sự kiện thông báo (Notification Event)  
Mọi thông báo đều được dựng bằng build_custom_notification_event() (gọi qua send_custom_notification)
"""
from __future__ import annotations
from datetime import timedelta
from typing import Any

from schemas.noti_schema import (
    NotificationAction,
    NotificationActor,
    NotificationActorType,
    NotificationAudience,
    NotificationAudienceType,
    NotificationChannel,
    NotificationContent,
    NotificationCreateCommand,
    NotificationDeduplication,
    NotificationDeliveryPolicy,
    NotificationEvent,
    NotificationPriority,
    NotificationResource,
    NotificationSeverity,
    NotificationSource,
    NotificationTrace
)
from utils.utils import utc_now, create_public_id
from utils.constants import SCHEMA_VERSION

# ============================================================
# TẠO EVENT CHUNG
# ============================================================
def build_notification_event(command: NotificationCreateCommand, *, notification_id: str | None = None, event_id: str | None = None) -> NotificationEvent:
    """
    Chuyển NotificationCreateCommand thành NotificationEvent hoàn chỉnh.

    Hàm này:
    - sinh notification_id
    - sinh event_id
    - đặt created_at
    - tính available_at
    - tính expires_at theo ttl_seconds nếu chưa truyền
    """
    # Lấy thời gian hiện tại
    now = utc_now()
    # Tạo id cho thông báo
    resolved_notification_id = notification_id or create_public_id("noti")
    # Tạo id cho event tạo thông báo
    resolved_event_id = event_id or create_public_id("evt")
    # Lấy thời gian khả dụng, và thời gian hết hạn cho thông báo này
    available_at = command.available_at or now
    expires_at = command.expires_at
    # Nếu không có thời gian hết hạn thì tự động tính thời gian hết hạn
    if expires_at is None:
        expires_at = available_at + timedelta(seconds=command.delivery_policy.ttl_seconds)

    # Chuyển command thành dict để làm payload
    payload = command.model_dump()
    # Thêm một số trường cho payload
    payload.update(
        {
            "schema_version": SCHEMA_VERSION,
            "notification_id": resolved_notification_id,
            "event_id": resolved_event_id,
            "created_at": now,
            "available_at": available_at,
            "expires_at": expires_at,
        }
    )

    # Chạy kiểm tra các trường trong Notification, cùng kiểu dữ liệu của từng trường
    # Sau đó chạy thêm các hàm validator của class nếu nó được khai báo
    return NotificationEvent.model_validate(payload)

def build_custom_notification_event(
    *,
    tenant_id: str,
    title: str,
    body: str,
    recipient_user_ids: list[str],
    actor_user_id: str,
    actor_username: str | None,
    notification_type: str = "system.message",
    category: str = "system",
    severity: NotificationSeverity = NotificationSeverity.INFO,
    priority: NotificationPriority = NotificationPriority.NORMAL,
    data: dict[str, Any] | None = None,
    job_id: str | None = None,
    correlation_id: str | None = None,
    trace_id: str | None = None,
    # ---- Các tham số tùy chọn để dùng cho thông báo nghiệp vụ (role.created, ...) ----
    short_body: str | None = None,
    template_variables: dict[str, Any] | None = None,
    resource: NotificationResource | None = None,
    action: NotificationAction | None = None,
    deduplication_key: str | None = None,
    deduplication_window_seconds: int = 300,
    ttl_seconds: int = 24 * 60 * 60,
    causation_id: str | None = None,
    source_instance: str = "notification-api",
    actor_type: NotificationActorType = NotificationActorType.USER,
) -> NotificationEvent:
    """
    Tạo một thông báo tùy chỉnh (title/body do người gọi truyền vào).

    Dùng cho:
    - API POST /notifications/send (kiểm thử toàn bộ luồng).
    - Thông báo nghiệp vụ khi người dùng thực hiện 1 tác vụ (tạo role, upload file...),
      truyền thêm resource / action / deduplication_key ... để có đầy đủ metadata.
    """
    # Sắp xếp và lọc danh sách recipient_user_ids, loại bỏ các giá trị rỗng hoặc chỉ chứa khoảng trắng
    recipient_ids = sorted({str(u).strip() for u in recipient_user_ids if u and str(u).strip()})
    # Nếu có danh sách recipient_user_ids thì audience là USERS, nếu không thì audience là TENANT (tất cả user trong tenant).
    audience = (
        NotificationAudience(type=NotificationAudienceType.USERS, user_ids=recipient_ids)
        if recipient_ids else NotificationAudience(type=NotificationAudienceType.TENANT)
    )

    # Tạo command để build nội dung thông báo, sau đó build thành NotificationEvent hoàn chỉnh
    command = NotificationCreateCommand(
        tenant_id=tenant_id,
        notification_type=notification_type,
        category=category,
        source=NotificationSource(
            service="api-server",
            instance=source_instance,
            environment="test",
        ),
        actor=NotificationActor(
            type=actor_type,
            id=actor_user_id,
            display_name=actor_username,
        ),
        resource=(
            resource
            or (NotificationResource(type="job", id=job_id) if job_id else None)
        ),
        audience=audience,
        channels=[
            NotificationChannel.IN_APP,
            NotificationChannel.WEBSOCKET,
        ],
        content=NotificationContent(
            template_key=notification_type,
            locale="vi-VN",
            title=title,
            body=body,
            short_body=(short_body or body)[:500],
            variables=dict(template_variables or {}),
        ),
        action=action or NotificationAction(),
        data=dict(data or {}, **({"job_id": job_id} if job_id else {})),
        priority=priority,
        severity=severity,
        deduplication=(
            NotificationDeduplication(
                key=deduplication_key,
                window_seconds=deduplication_window_seconds,
            )
            if deduplication_key
            else None
        ),
        delivery_policy=NotificationDeliveryPolicy(
            ttl_seconds=ttl_seconds,
            max_attempts=5,
            retry_delay_seconds=10,
            require_ack=False,
        ),
        trace=NotificationTrace(
            correlation_id=correlation_id,
            trace_id=trace_id,
            causation_id=causation_id or "notification_api:send",
        ),
    )

    return build_notification_event(command)
