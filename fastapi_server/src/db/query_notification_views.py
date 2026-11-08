from __future__ import annotations

import json
from typing import Any

from sqlalchemy import case, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from models.notification_model import (
    NotificationDeliveries,
    NotificationOutbox,
    NotificationRecipients,
    Notifications,
)


def safe_json_loads(
    value: str | None,
    default: Any,
) -> Any:
    if not value:
        return default

    try:
        return json.loads(value)
    except json.JSONDecodeError:
        return default


async def get_user_notification_feed(
    db: AsyncSession,
    *,
    tenant_id: str,
    user_id: str,
    state: str | None = None,
    category: str | None = None,
    only_important: bool = False,
    limit: int = 30,
    offset: int = 0,
) -> list[dict[str, Any]]:
    """
    Query thực tiễn nhất cho màn hình trung tâm thông báo.

    Trả:
    - nội dung
    - trạng thái đọc
    - action
    - priority/severity
    - thời gian
    """

    statement = (
        select(
            Notifications.notification_id,
            Notifications.notification_type,
            Notifications.category,
            Notifications.title,
            Notifications.body,
            Notifications.short_body,
            Notifications.image_url,
            Notifications.priority,
            Notifications.severity,
            Notifications.action_type,
            Notifications.action_target,
            Notifications.action_label,
            Notifications.action_method,
            Notifications.action_open_new_tab,
            Notifications.data_json,
            Notifications.created_at,
            Notifications.expires_at,

            NotificationRecipients.state,
            NotificationRecipients.is_pinned,
            NotificationRecipients.delivered_at,
            NotificationRecipients.seen_at,
            NotificationRecipients.read_at,
            NotificationRecipients.archived_at,
        )
        .join(
            NotificationRecipients,
            NotificationRecipients.notification_id
            == Notifications.notification_id,
        )
        .where(
            Notifications.tenant_id == tenant_id,
            NotificationRecipients.tenant_id == tenant_id,
            NotificationRecipients.recipient_user_id == user_id,
            Notifications.status == "active",
            NotificationRecipients.deleted_at.is_(None),
        )
    )

    if state:
        statement = statement.where(
            NotificationRecipients.state == state,
        )

    if category:
        statement = statement.where(
            Notifications.category == category,
        )

    if only_important:
        statement = statement.where(
            Notifications.priority.in_(["high", "urgent"])
        )

    priority_order = case(
        (Notifications.priority == "urgent", 1),
        (Notifications.priority == "high", 2),
        (Notifications.priority == "normal", 3),
        else_=4,
    )

    statement = (
        statement
        .order_by(
            NotificationRecipients.is_pinned.desc(),
            priority_order.asc(),
            Notifications.created_at.desc(),
        )
        .offset(offset)
        .limit(limit)
    )

    result = await db.execute(statement)

    output: list[dict[str, Any]] = []

    for row in result.mappings():
        item = dict(row)

        item["data"] = safe_json_loads(
            item.pop("data_json"),
            {},
        )

        item["action"] = {
            "type": item.pop("action_type"),
            "target": item.pop("action_target"),
            "label": item.pop("action_label"),
            "method": item.pop("action_method"),
            "open_in_new_tab": item.pop(
                "action_open_new_tab"
            ),
        }

        output.append(item)

    return output


async def get_user_notification_detail(
    db: AsyncSession,
    *,
    tenant_id: str,
    user_id: str,
    notification_id: str,
) -> dict[str, Any] | None:
    """
    Chi tiết thông báo của 1 user
    """
    statement = (
        select(
            Notifications,
            NotificationRecipients,
        )
        .join(
            NotificationRecipients,
            NotificationRecipients.notification_id
            == Notifications.notification_id,
        )
        .where(
            Notifications.tenant_id == tenant_id,
            Notifications.notification_id == notification_id,
            NotificationRecipients.tenant_id == tenant_id,
            NotificationRecipients.recipient_user_id == user_id,
            NotificationRecipients.deleted_at.is_(None),
        )
    )

    result = await db.execute(statement)
    row = result.first()

    if row is None:
        return None

    notification, recipient = row

    return {
        "notification_id": notification.notification_id,
        "event_id": notification.event_id,
        "type": notification.notification_type,
        "category": notification.category,

        "content": {
            "title": notification.title,
            "body": notification.body,
            "short_body": notification.short_body,
            "image_url": notification.image_url,
        },

        "actor": {
            "type": notification.actor_type,
            "id": notification.actor_id,
            "name": notification.actor_name,
            "avatar_url": notification.actor_avatar_url,
        },

        "resource": {
            "type": notification.resource_type,
            "id": notification.resource_id,
            "version": notification.resource_version,
        },

        "action": {
            "type": notification.action_type,
            "target": notification.action_target,
            "label": notification.action_label,
            "method": notification.action_method,
            "open_in_new_tab": (
                notification.action_open_new_tab
            ),
        },

        "data": safe_json_loads(
            notification.data_json,
            {},
        ),

        "priority": notification.priority,
        "severity": notification.severity,

        "recipient_state": {
            "state": recipient.state,
            "is_pinned": recipient.is_pinned,
            "delivered_at": recipient.delivered_at,
            "seen_at": recipient.seen_at,
            "read_at": recipient.read_at,
            "archived_at": recipient.archived_at,
        },

        "created_at": notification.created_at,
        "expires_at": notification.expires_at,
    }

async def get_unread_count_by_category(
    db: AsyncSession,
    *,
    tenant_id: str,
    user_id: str,
) -> list[dict[str, Any]]:
    statement = (
        select(
            Notifications.category,
            func.count(
                NotificationRecipients.id
            ).label("unread_count"),
        )
        .join(
            NotificationRecipients,
            NotificationRecipients.notification_id
            == Notifications.notification_id,
        )
        .where(
            Notifications.tenant_id == tenant_id,
            NotificationRecipients.tenant_id == tenant_id,
            NotificationRecipients.recipient_user_id == user_id,
            NotificationRecipients.state == "unread",
            NotificationRecipients.deleted_at.is_(None),
            Notifications.status == "active",
        )
        .group_by(Notifications.category)
        .order_by(
            func.count(
                NotificationRecipients.id
            ).desc()
        )
    )

    result = await db.execute(statement)

    return [
        dict(row)
        for row in result.mappings().all()
    ]

async def get_delivery_failure_summary(
    db: AsyncSession,
    *,
    tenant_id: str,
) -> list[dict[str, Any]]:
    statement = (
        select(
            NotificationDeliveries.channel,
            NotificationDeliveries.error_code,
            func.count(
                NotificationDeliveries.id
            ).label("failure_count"),
            func.max(
                NotificationDeliveries.failed_at
            ).label("last_failed_at"),
        )
        .where(
            NotificationDeliveries.tenant_id == tenant_id,
            NotificationDeliveries.status == "failed",
        )
        .group_by(
            NotificationDeliveries.channel,
            NotificationDeliveries.error_code,
        )
        .order_by(
            func.count(
                NotificationDeliveries.id
            ).desc()
        )
    )

    result = await db.execute(statement)

    return [
        dict(row)
        for row in result.mappings().all()
    ]

async def get_outbox_health_summary(
    db: AsyncSession,
) -> list[dict[str, Any]]:
    statement = (
        select(
            NotificationOutbox.status,
            func.count(
                NotificationOutbox.id
            ).label("total"),
            func.min(
                NotificationOutbox.created_at
            ).label("oldest_created_at"),
            func.max(
                NotificationOutbox.attempt_count
            ).label("max_attempt_count"),
        )
        .group_by(NotificationOutbox.status)
        .order_by(NotificationOutbox.status)
    )

    result = await db.execute(statement)

    return [
        dict(row)
        for row in result.mappings().all()
    ]