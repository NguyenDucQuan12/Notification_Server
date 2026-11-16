
from __future__ import annotations

import json as _json
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

# ============================================================
# 6. TÌM OUTBOX ĐẾN HẠN XỬ LÝ (dùng cho Outbox Worker)
# ============================================================

async def find_due_outbox_ids(
    db: AsyncSession,
    *,
    limit: int,
) -> list[str]:
    """
    Lấy danh sách outbox_id đang chờ publish và đã đến hạn.

    Hàm này CHỈ ĐỌC ID, không khóa/không đổi trạng thái. Việc "nhận quyền xử lý"
    vẫn do claim_outbox_by_id() làm nguyên tử (pending -> processing). Nhờ vậy:
    - Chạy nhiều worker cùng lúc vẫn an toàn: 2 worker cùng thấy 1 ID thì chỉ
      worker claim trước được xử lý, worker sau nhận None và bỏ qua.
    - Worker và Direct Publisher của API không bao giờ publish trùng.

    READPAST: bỏ qua các dòng đang bị transaction khác khóa thay vì đứng chờ,
    giúp worker không bị treo khi API đang ghi outbox.

    ORDER BY available_at: xử lý outbox cũ nhất trước (công bằng, tránh "đói").
    """

    statement = text(
        """
        SELECT TOP (:limit) outbox_id
        FROM notification_outbox WITH (READPAST)
        WHERE
            status = 'pending'
            AND available_at <= SYSUTCDATETIME()
            AND attempt_count < max_attempts
        ORDER BY available_at ASC;
        """
    )

    result = await db.execute(statement, {"limit": int(limit)})
    return [str(row[0]) for row in result.all()]


# ============================================================
# 7. KHÔI PHỤC OUTBOX BỊ KẸT Ở TRẠNG THÁI PROCESSING
# ============================================================

async def recover_stale_processing_outbox(
    db: AsyncSession,
    *,
    stale_after_seconds: int,
) -> int:
    """
    Trả các outbox bị kẹt ở 'processing' quá lâu về 'pending' (hoặc 'failed').

    Khi nào bị kẹt? Process đã claim (pending -> processing) rồi bị crash / bị kill /
    mất kết nối DB trước khi kịp đánh dấu published hoặc retry. Nếu không khôi
    phục, notification đó sẽ không bao giờ được gửi.

    - Còn lượt thử  -> 'pending' (worker sẽ xử lý lại).
    - Hết lượt thử  -> 'failed'  (cần người kiểm tra, xem last_error).

    Lưu ý: chọn stale_after_seconds đủ lớn (vd 300s) so với thời gian xử lý bình
    thường, để không "cướp" outbox của process vẫn đang chạy chậm. Trường hợp xấu
    nhất là event được publish 2 lần; client đã tự chống trùng nhờ event_id.

    Trả về số bản ghi đã khôi phục.
    """

    statement = text(
        """
        UPDATE notification_outbox
        SET
            status = CASE
                WHEN attempt_count >= max_attempts THEN 'failed'
                ELSE 'pending'
            END,
            available_at = SYSUTCDATETIME(),
            locked_at = NULL,
            locked_by = NULL,
            last_error = CONCAT('Recovered stale processing lock from ', locked_by),
            updated_at = SYSUTCDATETIME()
        WHERE
            status = 'processing'
            AND locked_at < DATEADD(SECOND, -:stale_after_seconds, SYSUTCDATETIME());
        """
    )

    result = await db.execute(statement, {"stale_after_seconds": int(stale_after_seconds)})
    return int(result.rowcount or 0)


# ============================================================
# XỬ LÝ THEO LÔ (BATCH) - dùng khi gửi cho số lượng lớn người nhận
# ============================================================
#
# Bản xử lý từng outbox (claim_outbox_by_id + mark_outbox_published) tốn
# 2 transaction SQL cho MỖI người nhận. Gửi 1 thông báo cho 2.000 user
# => ~4.000 transaction, ~6.000 round-trip tới SQL Server.
#
# Các hàm dưới đây xử lý cả lô trong 1 câu lệnh SQL:
# - claim_outbox_batch: UPDATE ... OUTPUT trên CTE có TOP + READPAST
#   (mẫu "SQL Server queue" chuẩn, nhiều worker chạy song song không giẫm chân nhau).
# - mark_*_batch: truyền danh sách dạng JSON và JOIN với OPENJSON
#   (SQL Server 2016+, compatibility level >= 130), không bị giới hạn 2.100 tham số.

_OUTBOX_OUTPUT_COLUMNS = """
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
"""


async def claim_outbox_batch(
    db: AsyncSession,
    *,
    worker_id: str,
    limit: int,
    outbox_ids: list[str] | None = None,
) -> list[dict[str, Any]]:
    """
    Claim nguyên tử tối đa `limit` outbox: pending -> processing, trả về dữ liệu các dòng đã claim.

    outbox_ids:
        None  -> lấy các outbox đến hạn cũ nhất (dùng cho Outbox Worker).
        list  -> chỉ claim trong danh sách này (dùng cho DIRECT publish ngay sau khi tạo).

    ROWLOCK + UPDLOCK + READPAST: dòng đang bị process khác khóa sẽ bị BỎ QUA thay vì chờ,
    nên nhiều worker / API chạy song song mỗi bên nhận một phần khác nhau, không publish trùng.
    """
    if outbox_ids is not None and not outbox_ids:
        return []

    if outbox_ids is None:
        # Outbox Worker: lấy các dòng đến hạn cũ nhất theo index (status, available_at).
        # READPAST bỏ qua dòng đang bị process khác khóa => nhiều worker chạy song song an toàn.
        statement = text(
            f"""
            WITH due AS (
                SELECT TOP (:limit) o.*
                FROM notification_outbox AS o WITH (ROWLOCK, UPDLOCK, READPAST)
                WHERE
                    o.status = 'pending'
                    AND o.available_at <= SYSUTCDATETIME()
                    AND o.attempt_count < o.max_attempts
                ORDER BY o.available_at ASC, o.id ASC
            )
            UPDATE due
            SET
                status = 'processing',
                locked_at = SYSUTCDATETIME(),
                locked_by = :worker_id,
                attempt_count = attempt_count + 1,
                updated_at = SYSUTCDATETIME()
            OUTPUT {_OUTBOX_OUTPUT_COLUMNS};
            """
        )
        result = await db.execute(statement, {"limit": int(limit), "worker_id": worker_id})
        return [dict(row) for row in result.mappings().all()]

    # DIRECT: claim đúng danh sách outbox vừa tạo.
    # Đi từ danh sách JSON -> SEEK theo unique index outbox_id (LOOP JOIN + FORCESEEK).
    # KHÔNG được để SQL Server quét bảng: với UPDLOCK, mọi dòng bị quét đều bị giữ khóa tới hết
    # transaction => chặn các request khác đang publish (lỗi treo khi nhiều request đồng thời).
    statement = text(
        f"""
        UPDATE o
        SET
            status = 'processing',
            locked_at = SYSUTCDATETIME(),
            locked_by = :worker_id,
            attempt_count = o.attempt_count + 1,
            updated_at = SYSUTCDATETIME()
        OUTPUT {_OUTBOX_OUTPUT_COLUMNS}
        FROM OPENJSON(:ids) WITH (outbox_id varchar(64) '$') AS j
        INNER LOOP JOIN notification_outbox AS o WITH (ROWLOCK, UPDLOCK, READPAST, FORCESEEK)
            ON o.outbox_id = j.outbox_id
        WHERE
            o.status = 'pending'
            AND o.available_at <= SYSUTCDATETIME()
            AND o.attempt_count < o.max_attempts;
        """
    )
    result = await db.execute(
        statement,
        {"ids": _json.dumps(list(outbox_ids)[: int(limit)]), "worker_id": worker_id},
    )
    return [dict(row) for row in result.mappings().all()]


async def mark_outbox_batch_published(
    db: AsyncSession,
    *,
    worker_id: str,
    items: list[tuple[str, str]],
) -> int:
    """
    Đánh dấu published cho cả lô trong 1 câu lệnh.

    items: [(outbox_id, stream_id), ...]
    Chỉ cập nhật dòng vẫn đang 'processing' và do chính worker_id này khóa.
    """
    if not items:
        return 0

    statement = text(
        """
        UPDATE o
        SET
            status = 'published',
            published_stream_id = j.stream_id,
            published_at = SYSUTCDATETIME(),
            locked_at = NULL,
            locked_by = NULL,
            last_error = NULL,
            updated_at = SYSUTCDATETIME()
        FROM OPENJSON(:items)
            WITH (outbox_id varchar(64) '$.o', stream_id varchar(100) '$.s') AS j
        -- Đi từ danh sách -> seek theo unique index outbox_id, chỉ chạm đúng các dòng của lô này.
        INNER LOOP JOIN notification_outbox AS o WITH (ROWLOCK, FORCESEEK)
            ON o.outbox_id = j.outbox_id
        WHERE
            o.status = 'processing'
            AND o.locked_by = :worker_id;
        """
    )
    payload = _json.dumps([{"o": o, "s": s} for o, s in items])
    result = await db.execute(statement, {"items": payload, "worker_id": worker_id})
    return int(result.rowcount or 0)


async def mark_user_websocket_deliveries_sent_batch(
    db: AsyncSession,
    *,
    items: list[tuple[str, str, str]],
) -> int:
    """
    Đánh dấu delivery kênh websocket = 'sent' cho cả lô.

    items: [(notification_id, recipient_user_id, stream_id), ...]
    Dùng unique index (notification_id, recipient_user_id, channel) nên mỗi dòng là 1 seek.
    """
    if not items:
        return 0

    statement = text(
        """
        UPDATE d
        SET
            status = 'sent',
            provider = 'redis-stream',
            provider_message_id = j.stream_id,
            sent_at = SYSUTCDATETIME(),
            error_code = NULL,
            error_message = NULL,
            next_retry_at = NULL,
            updated_at = SYSUTCDATETIME()
        FROM OPENJSON(:items)
            WITH (
                notification_id varchar(64) '$.n',
                recipient_user_id varchar(64) '$.u',
                stream_id varchar(255) '$.s'
            ) AS j
        -- Seek theo unique index (notification_id, recipient_user_id, channel).
        INNER LOOP JOIN notification_deliveries AS d WITH (ROWLOCK, FORCESEEK)
            ON d.notification_id = j.notification_id
            AND d.recipient_user_id = j.recipient_user_id
            AND d.channel = 'websocket'
        WHERE
            d.status IN ('pending', 'queued', 'sending', 'failed');
        """
    )
    payload = _json.dumps([{"n": n, "u": u, "s": s} for n, u, s in items])
    result = await db.execute(statement, {"items": payload})
    return int(result.rowcount or 0)
