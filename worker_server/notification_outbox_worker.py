from __future__ import annotations

import asyncio
import os
import socket
from typing import Any
from uuid import uuid4

from db.database import AsyncSessionLocal
from db.query_notification_deliveries import (
    mark_notification_deliveries_failed,
    mark_notification_deliveries_sent,
)
from db.query_notification_outbox import (
    claim_pending_outbox_batch,
    mark_outbox_published,
    mark_outbox_retry,
    recover_stale_processing_outbox,
)
from services.notification_publisher import (
    publish_notification_payload,
)
from utils.utils import utc_now


WORKER_POLL_SECONDS = float(
    os.getenv("NOTIFICATION_WORKER_POLL_SECONDS", "1")
)

WORKER_BATCH_SIZE = int(
    os.getenv("NOTIFICATION_WORKER_BATCH_SIZE", "50")
)


def build_worker_id() -> str:
    hostname = socket.gethostname()

    return (
        f"noti-worker:{hostname}:"
        f"{uuid4().hex[:8]}"
    )


async def process_outbox_item(
    item: dict[str, Any],
) -> None:
    """
    Publish một item đã được claim.

    Mỗi lần cập nhật DB dùng transaction riêng.
    """

    try:
        stream_id = await publish_notification_payload(
            stream_key=item["stream_key"],
            event_id=item["event_id"],
            notification_id=item["notification_id"],
            tenant_id=item["tenant_id"],
            event_type=item["event_type"],
            schema_version="1.0",
            payload_json=item["payload_json"],
        )

    except Exception as exc:
        async with AsyncSessionLocal() as db:
            async with db.begin():
                await mark_outbox_retry(
                    db,
                    outbox_id=item["outbox_id"],
                    error_message=(
                        f"{type(exc).__name__}: {exc}"
                    ),
                    attempt_count=item["attempt_count"],
                    max_attempts=item["max_attempts"],
                )

                await mark_notification_deliveries_failed(
                    db,
                    notification_id=item["notification_id"],
                    channel="websocket",
                    error_code=type(exc).__name__,
                    error_message=str(exc),
                    next_retry_at=utc_now(),
                )

        print(
            "Publish notification thất bại: "
            f"outbox_id={item['outbox_id']}, "
            f"error={exc}",
            flush=True,
        )

        return

    async with AsyncSessionLocal() as db:
        async with db.begin():
            await mark_outbox_published(
                db,
                outbox_id=item["outbox_id"],
                stream_id=stream_id,
            )

            await mark_notification_deliveries_sent(
                db,
                notification_id=item["notification_id"],
                channel="websocket",
                provider="redis-stream",
                provider_message_id=stream_id,
            )

    print(
        "Đã publish notification: "
        f"outbox_id={item['outbox_id']}, "
        f"stream_id={stream_id}",
        flush=True,
    )


async def run_worker() -> None:
    worker_id = build_worker_id()

    print(
        f"Notification outbox worker started: {worker_id}",
        flush=True,
    )

    while True:
        try:
            async with AsyncSessionLocal() as db:
                async with db.begin():
                    # Khôi phục item bị kẹt do worker crash.
                    await recover_stale_processing_outbox(
                        db,
                        stale_after_seconds=300,
                    )

                    items = await claim_pending_outbox_batch(
                        db,
                        worker_id=worker_id,
                        batch_size=WORKER_BATCH_SIZE,
                    )

            if not items:
                await asyncio.sleep(WORKER_POLL_SECONDS)
                continue

            # Xử lý song song trong batch.
            await asyncio.gather(
                *[
                    process_outbox_item(item)
                    for item in items
                ],
                return_exceptions=True,
            )

        except asyncio.CancelledError:
            print(
                "Notification worker đang dừng...",
                flush=True,
            )
            raise

        except Exception as exc:
            print(
                "Notification worker loop error: "
                f"{type(exc).__name__}: {exc}",
                flush=True,
            )

            await asyncio.sleep(3)


if __name__ == "__main__":
    asyncio.run(run_worker())