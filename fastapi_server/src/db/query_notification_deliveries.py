from __future__ import annotations

from collections.abc import Iterable
from datetime import datetime
from typing import Sequence

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from models.notification_model import NotificationDeliveries
from utils.utils import utc_now


async def add_notification_deliveries( db: AsyncSession, deliveries: Iterable[NotificationDeliveries], ) -> int:
    delivery_rows = list(deliveries)

    if not delivery_rows:
        return 0

    db.add_all(delivery_rows)
    await db.flush()

    return len(delivery_rows)


async def mark_notification_deliveries_sent( db: AsyncSession, *, notification_id: str, channel: str, provider: str, provider_message_id: str | None = None, ) -> int:
    """
    Redis XADD thành công chỉ nên xem là sent.

    Chưa nên đánh dấu delivered vì chưa chắc client WebSocket
    đã thực sự nhận được message.
    """

    now = utc_now()

    statement = (
        update(NotificationDeliveries)
        .where(
            NotificationDeliveries.notification_id == notification_id,
            NotificationDeliveries.channel == channel,
            NotificationDeliveries.status.in_(
                ["pending", "queued", "sending", "failed"]
            ),
        )
        .values(
            status="sent",
            provider=provider,
            provider_message_id=provider_message_id,
            sent_at=now,
            error_code=None,
            error_message=None,
            updated_at=now,
        )
    )

    result = await db.execute(statement)

    return int(result.rowcount or 0)


async def mark_delivery_delivered( db: AsyncSession, *, delivery_id: str, ) -> bool:
    """
    Dùng khi Notification Server đã gửi message tới WebSocket client
    hoặc nhận ACK từ client.
    """

    now = utc_now()

    statement = (
        update(NotificationDeliveries)
        .where(
            NotificationDeliveries.delivery_id == delivery_id,
        )
        .values(
            status="delivered",
            delivered_at=now,
            updated_at=now,
        )
    )

    result = await db.execute(statement)

    return bool(result.rowcount)


async def mark_notification_deliveries_failed( db: AsyncSession, *, notification_id: str, channel: str, error_code: str, error_message: str, next_retry_at: datetime | None, ) -> int:
    now = utc_now()

    statement = (
        update(NotificationDeliveries)
        .where(
            NotificationDeliveries.notification_id == notification_id,
            NotificationDeliveries.channel == channel,
        )
        .values(
            status="failed",
            attempt_count=NotificationDeliveries.attempt_count + 1,
            next_retry_at=next_retry_at,
            error_code=error_code,
            error_message=error_message[:4000],
            failed_at=now,
            updated_at=now,
        )
    )

    result = await db.execute(statement)

    return int(result.rowcount or 0)


async def get_retryable_deliveries( db: AsyncSession, *, channel: str | None = None, limit: int = 100, ) -> Sequence[NotificationDeliveries]:
    now = utc_now()

    statement = select(NotificationDeliveries).where(
        NotificationDeliveries.status == "failed",
        NotificationDeliveries.attempt_count
        < NotificationDeliveries.max_attempts,
        NotificationDeliveries.next_retry_at.is_not(None),
        NotificationDeliveries.next_retry_at <= now,
    )

    if channel:
        statement = statement.where(
            NotificationDeliveries.channel == channel,
        )

    statement = (
        statement
        .order_by(NotificationDeliveries.next_retry_at.asc())
        .limit(limit)
    )

    result = await db.execute(statement)

    return result.scalars().all()