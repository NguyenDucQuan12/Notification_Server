from __future__ import annotations

from collections.abc import Iterable
from datetime import datetime

from sqlalchemy import func, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from models.notification_model import NotificationRecipients
from utils.utils import utc_now


async def add_notification_recipients( db: AsyncSession, recipients: Iterable[NotificationRecipients], ) -> int:
    recipient_rows = list(recipients)

    if not recipient_rows:
        return 0

    db.add_all(recipient_rows)
    await db.flush()

    return len(recipient_rows)


async def get_recipient_state( db: AsyncSession, *, tenant_id: str, notification_id: str, recipient_user_id: str, ) -> NotificationRecipients | None:
    statement = select(NotificationRecipients).where(
        NotificationRecipients.tenant_id == tenant_id,
        NotificationRecipients.notification_id == notification_id,
        NotificationRecipients.recipient_user_id == recipient_user_id,
    )

    result = await db.execute(statement)

    return result.scalar_one_or_none()


async def count_unread_notifications( db: AsyncSession, *, tenant_id: str, recipient_user_id: str, ) -> int:
    statement = select(
        func.count(NotificationRecipients.id)
    ).where(
        NotificationRecipients.tenant_id == tenant_id,
        NotificationRecipients.recipient_user_id == recipient_user_id,
        NotificationRecipients.state == "unread",
        NotificationRecipients.deleted_at.is_(None),
    )

    result = await db.execute(statement)

    return int(result.scalar_one() or 0)


async def mark_notification_seen( db: AsyncSession, *, tenant_id: str, notification_id: str, recipient_user_id: str, ) -> bool:
    now = utc_now()

    statement = (
        update(NotificationRecipients)
        .where(
            NotificationRecipients.tenant_id == tenant_id,
            NotificationRecipients.notification_id == notification_id,
            NotificationRecipients.recipient_user_id == recipient_user_id,
            NotificationRecipients.deleted_at.is_(None),
            NotificationRecipients.state == "unread",
        )
        .values(
            state="seen",
            seen_at=now,
            updated_at=now,
        )
    )

    result = await db.execute(statement)

    return bool(result.rowcount)


async def mark_notification_read( db: AsyncSession, *, tenant_id: str, notification_id: str, recipient_user_id: str, ) -> bool:
    now = utc_now()

    statement = (
        update(NotificationRecipients)
        .where(
            NotificationRecipients.tenant_id == tenant_id,
            NotificationRecipients.notification_id == notification_id,
            NotificationRecipients.recipient_user_id == recipient_user_id,
            NotificationRecipients.deleted_at.is_(None),
        )
        .values(
            state="read",
            seen_at=now,
            read_at=now,
            updated_at=now,
        )
    )

    result = await db.execute(statement)

    return bool(result.rowcount)


async def mark_all_notifications_read( db: AsyncSession, *, tenant_id: str, recipient_user_id: str, ) -> int:
    now = utc_now()

    statement = (
        update(NotificationRecipients)
        .where(
            NotificationRecipients.tenant_id == tenant_id,
            NotificationRecipients.recipient_user_id == recipient_user_id,
            NotificationRecipients.state.in_(["unread", "seen"]),
            NotificationRecipients.deleted_at.is_(None),
        )
        .values(
            state="read",
            seen_at=now,
            read_at=now,
            updated_at=now,
        )
    )

    result = await db.execute(statement)

    return int(result.rowcount or 0)


async def archive_notification( db: AsyncSession, *, tenant_id: str, notification_id: str, recipient_user_id: str, ) -> bool:
    now = utc_now()

    statement = (
        update(NotificationRecipients)
        .where(
            NotificationRecipients.tenant_id == tenant_id,
            NotificationRecipients.notification_id == notification_id,
            NotificationRecipients.recipient_user_id == recipient_user_id,
            NotificationRecipients.deleted_at.is_(None),
        )
        .values(
            state="archived",
            archived_at=now,
            updated_at=now,
        )
    )

    result = await db.execute(statement)

    return bool(result.rowcount)


async def soft_delete_notification_for_user( db: AsyncSession, *, tenant_id: str, notification_id: str, recipient_user_id: str, ) -> bool:
    now = utc_now()

    statement = (
        update(NotificationRecipients)
        .where(
            NotificationRecipients.tenant_id == tenant_id,
            NotificationRecipients.notification_id == notification_id,
            NotificationRecipients.recipient_user_id == recipient_user_id,
        )
        .values(
            state="deleted",
            deleted_at=now,
            updated_at=now,
        )
    )

    result = await db.execute(statement)

    return bool(result.rowcount)