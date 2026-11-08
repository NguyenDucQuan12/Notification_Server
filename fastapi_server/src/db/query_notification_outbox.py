
from __future__ import annotations

from datetime import timedelta
from typing import Any

from sqlalchemy import text, update
from sqlalchemy.ext.asyncio import AsyncSession

from models.notification_model import (
    NotificationDeliveries,
    NotificationOutbox,
)
from utils.utils import utc_now


# ============================================================
# 1. CLAIM MỘT OUTBOX
# ============================================================

async def claim_outbox_by_id(
    db: AsyncSession,
    *,
    outbox_id: str,
    worker_id: str,
) -> dict[str, Any] | None:
    """
    Nhận quyền xử lý một bản ghi Outbox.

    Hàm thực hiện nguyên tử:

        pending → processing

    Chỉ một process được phép chuyển bản ghi từ pending
    sang processing.

    Vì vậy, nếu Direct Publisher và Worker cùng chạy:

    - Process claim trước sẽ nhận được dữ liệu Outbox.
    - Process claim sau sẽ nhận None.
    - Event không bị publish đồng thời hai lần.

    Tham số:
    ----------
    db:
        SQLAlchemy AsyncSession.

    outbox_id:
        ID public của Outbox cần xử lý.

    worker_id:
        ID process đang claim.

        Ví dụ:
            direct:api-container-01:ab1289
            worker:noti-worker-01:cd4592

    Kết quả:
    --------
    Trả dict chứa dữ liệu Outbox nếu claim thành công.

    Trả None nếu:
    - Outbox không tồn tại.
    - Outbox không còn pending.
    - Chưa đến available_at.
    - Đã hết số lần thử.
    """

    # SQL Server UPDATE ... OUTPUT giúp:
    #
    # 1. Update trạng thái.
    # 2. Lấy dữ liệu vừa update.
    #
    # trong cùng một câu SQL nguyên tử.
    statement = text(
        """
        UPDATE notification_outbox
            WITH (ROWLOCK, UPDLOCK)
        SET
            status = 'processing',
            locked_at = SYSUTCDATETIME(),
            locked_by = :worker_id,
            attempt_count = attempt_count + 1,
            updated_at = SYSUTCDATETIME()
        OUTPUT
            inserted.outbox_id,
            inserted.event_id,
            inserted.tenant_id,

            inserted.aggregate_id AS notification_id,
            inserted.event_type,

            inserted.stream_scope,
            inserted.stream_key,

            inserted.target_tenant_id,
            inserted.target_user_id,
            inserted.target_job_id,

            inserted.payload_json,

            inserted.attempt_count,
            inserted.max_attempts
        WHERE
            outbox_id = :outbox_id
            AND status = 'pending'
            AND available_at <= SYSUTCDATETIME()
            AND attempt_count < max_attempts;
        """
    )

    result = await db.execute(
        statement,
        {
            "outbox_id": outbox_id,
            "worker_id": worker_id,
        },
    )

    # mappings() trả từng dòng dưới dạng mapping/dict-like object.
    row = result.mappings().first()

    if row is None:
        return None

    return dict(row)


# ============================================================
# 2. ĐÁNH DẤU OUTBOX ĐÃ PUBLISH
# ============================================================

async def mark_outbox_published(
    db: AsyncSession,
    *,
    outbox_id: str,
    worker_id: str,
    stream_id: str,
) -> bool:
    """
    Đánh dấu Outbox đã publish Redis thành công.

    Chỉ process đã claim bản ghi mới được cập nhật.

    Điều kiện:

        status = processing
        locked_by = worker_id

    Điều này ngăn process khác cập nhật nhầm Outbox.
    """

    now = utc_now()

    statement = (
        update(NotificationOutbox)
        .where(
            NotificationOutbox.outbox_id == outbox_id,
            NotificationOutbox.status == "processing",
            NotificationOutbox.locked_by == worker_id,
        )
        .values(
            status="published",

            # Redis Stream ID do XADD trả về.
            published_stream_id=stream_id,
            published_at=now,

            # Giải phóng khóa logic.
            locked_at=None,
            locked_by=None,

            # Xóa lỗi cũ nếu trước đây từng gửi thất bại.
            last_error=None,

            updated_at=now,
        )
    )

    result = await db.execute(statement)

    return bool(result.rowcount)


# ============================================================
# 3. ĐƯA OUTBOX VỀ PENDING HOẶC FAILED
# ============================================================

async def mark_outbox_retry(
    db: AsyncSession,
    *,
    outbox_id: str,
    worker_id: str,
    error_message: str,
    attempt_count: int,
    max_attempts: int,
) -> dict[str, Any]:
    """
    Xử lý Outbox sau khi publish Redis thất bại.

    Nếu còn lượt thử:
        processing → pending

    Nếu đã hết lượt thử:
        processing → failed

    Hàm sử dụng exponential backoff:

        lần 1 → khoảng 2 giây
        lần 2 → khoảng 4 giây
        lần 3 → khoảng 8 giây
        lần 4 → khoảng 16 giây
        ...

    Thời gian chờ tối đa là 300 giây.
    """

    now = utc_now()

    # attempt_count đã được tăng khi claim.
    if attempt_count >= max_attempts:
        next_status = "failed"

        # Không còn retry.
        next_available_at = now

    else:
        next_status = "pending"

        # Exponential backoff.
        #
        # attempt_count = 1 → 2 giây
        # attempt_count = 2 → 4 giây
        # attempt_count = 3 → 8 giây
        #
        # Tối đa 300 giây.
        backoff_seconds = min(
            300,
            2 ** min(attempt_count, 8),
        )

        next_available_at = (
            now + timedelta(seconds=backoff_seconds)
        )

    statement = (
        update(NotificationOutbox)
        .where(
            NotificationOutbox.outbox_id == outbox_id,
            NotificationOutbox.status == "processing",
            NotificationOutbox.locked_by == worker_id,
        )
        .values(
            status=next_status,

            # Nếu pending, worker chỉ được xử lý lại
            # sau thời điểm này.
            available_at=next_available_at,

            # Giải phóng khóa logic.
            locked_at=None,
            locked_by=None,

            # Lưu lỗi gần nhất để debug.
            last_error=error_message[:4000],

            updated_at=now,
        )
    )

    result = await db.execute(statement)

    return {
        "updated": bool(result.rowcount),
        "status": next_status,
        "available_at": next_available_at,
    }


# ============================================================
# 4. CẬP NHẬT DELIVERY USER STREAM THÀNH SENT
# ============================================================

async def mark_user_websocket_delivery_sent(
    db: AsyncSession,
    *,
    notification_id: str,
    recipient_user_id: str,
    stream_id: str,
) -> int:
    """
    Đánh dấu WebSocket delivery của một user đã được đưa lên Redis.

    Chỉ nên gọi hàm này khi Outbox có:

        stream_scope = user
        target_user_id có giá trị

    XADD thành công chỉ nên được xem là:

        status = sent

    Không nên đánh dấu delivered ngay, vì:

    - Redis đã nhận event.
    - Nhưng chưa chắc WebSocket client đã nhận.

    delivered nên được cập nhật khi:
    - Notification Server gửi thành công xuống socket;
    - hoặc client gửi ACK.
    """

    now = utc_now()

    statement = (
        update(NotificationDeliveries)
        .where(
            NotificationDeliveries.notification_id
            == notification_id,

            NotificationDeliveries.recipient_user_id
            == recipient_user_id,

            NotificationDeliveries.channel == "websocket",

            NotificationDeliveries.status.in_(
                [
                    "pending",
                    "queued",
                    "sending",
                    "failed",
                ]
            ),
        )
        .values(
            status="sent",

            provider="redis-stream",

            # Redis Stream ID có thể dùng để trace và replay.
            provider_message_id=stream_id,

            sent_at=now,

            # Khi gửi lại thành công, xóa lỗi cũ.
            error_code=None,
            error_message=None,
            next_retry_at=None,

            updated_at=now,
        )
    )

    result = await db.execute(statement)

    return int(result.rowcount or 0)


# ============================================================
# 5. CẬP NHẬT DELIVERY USER STREAM THẤT BẠI
# ============================================================

async def mark_user_websocket_delivery_failed(
    db: AsyncSession,
    *,
    notification_id: str,
    recipient_user_id: str,
    error_code: str,
    error_message: str,
    next_retry_at,
) -> int:
    """
    Đánh dấu WebSocket delivery của một user đã publish thất bại.

    Chỉ áp dụng trực tiếp cho user stream.

    Tenant/global stream là broadcast route nên không thể ánh xạ
    chính xác thành một recipient duy nhất tại bước này.
    """

    now = utc_now()

    statement = (
        update(NotificationDeliveries)
        .where(
            NotificationDeliveries.notification_id
            == notification_id,

            NotificationDeliveries.recipient_user_id
            == recipient_user_id,

            NotificationDeliveries.channel == "websocket",
        )
        .values(
            status="failed",

            attempt_count=(
                NotificationDeliveries.attempt_count + 1
            ),

            next_retry_at=next_retry_at,

            error_code=error_code[:100],
            error_message=error_message[:4000],

            failed_at=now,
            updated_at=now,
        )
    )

    result = await db.execute(statement)

    return int(result.rowcount or 0)