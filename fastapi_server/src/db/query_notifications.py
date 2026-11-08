from __future__ import annotations

from datetime import datetime, timedelta
from typing import Sequence
from typing import Any
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from schemas.noti_schema import NotificationEvent, NotificationCreateResult
from models.notification_model import NotificationDeliveries, NotificationOutbox, NotificationRecipients, Notifications
from services.notification_publisher import NotificationStreamTarget, deduplicate_targets
from utils.utils import utc_now, create_public_id, enum_value, json_dumps


async def add_notification(
    db: AsyncSession,
    notification: Notifications,
) -> Notifications:
    """
    Thêm notification vào session nhưng không commit.

    Transaction được điều khiển bởi service/controller bên ngoài.
    """

    db.add(notification)
    await db.flush()

    return notification


async def get_notification_by_id( db: AsyncSession, *, notification_id: str, tenant_id: str | None = None, ) -> Notifications | None:
    statement = select(Notifications).where(
        Notifications.notification_id == notification_id,
    )

    if tenant_id is not None:
        statement = statement.where(
            Notifications.tenant_id == tenant_id,
        )

    result = await db.execute(statement)

    return result.scalar_one_or_none()


async def find_recent_notification_by_deduplication_key( db: AsyncSession, *, tenant_id: str, deduplication_key: str, window_seconds: int, ) -> Notifications | None:
    """
    Tìm notification trùng trong khoảng thời gian chống lặp.

    Ví dụ:
    deduplication_key = role:tenant_01:manager:created
    window_seconds = 300
    """

    from_time = utc_now() - timedelta(seconds=window_seconds)

    statement = (
        select(Notifications)
        .where(
            Notifications.tenant_id == tenant_id,
            Notifications.deduplication_key == deduplication_key,
            Notifications.created_at >= from_time,
            Notifications.status == "active",
        )
        .order_by(Notifications.created_at.desc())
        .limit(1)
    )

    result = await db.execute(statement)

    return result.scalar_one_or_none()


async def list_notifications( db: AsyncSession, *, tenant_id: str, notification_type: str | None = None, category: str | None = None, severity: str | None = None, status: str | None = "active", limit: int = 50, offset: int = 0, ) -> Sequence[Notifications]:
    statement = select(Notifications).where(
        Notifications.tenant_id == tenant_id,
    )

    if notification_type:
        statement = statement.where(
            Notifications.notification_type == notification_type,
        )

    if category:
        statement = statement.where(
            Notifications.category == category,
        )

    if severity:
        statement = statement.where(
            Notifications.severity == severity,
        )

    if status:
        statement = statement.where(
            Notifications.status == status,
        )

    statement = (
        statement
        .order_by(Notifications.created_at.desc())
        .offset(offset)
        .limit(limit)
    )

    result = await db.execute(statement)

    return result.scalars().all()


async def cancel_notification( db: AsyncSession, *, notification_id: str, tenant_id: str, ) -> bool:
    statement = (
        update(Notifications)
        .where(
            Notifications.notification_id == notification_id,
            Notifications.tenant_id == tenant_id,
            Notifications.status == "active",
        )
        .values(
            status="cancelled",
            updated_at=utc_now(),
        )
    )

    result = await db.execute(statement)

    return bool(result.rowcount)


async def expire_old_notifications( db: AsyncSession, *, now: datetime | None = None, ) -> int:
    current_time = now or utc_now()

    statement = (
        update(Notifications)
        .where(
            Notifications.status == "active",
            Notifications.expires_at.is_not(None),
            Notifications.expires_at <= current_time,
        )
        .values(
            status="expired",
            updated_at=current_time,
        )
    )

    result = await db.execute(statement)

    return int(result.rowcount or 0)

async def add_notification_event_to_session(
    *,
    db: AsyncSession,
    event: NotificationEvent,
    stream_targets: list[NotificationStreamTarget],
    outbox_max_attempts: int = 10,
) -> NotificationCreateResult:
    """
    Chuyển một NotificationEvent thành các bản ghi database.

    Hàm sẽ thêm dữ liệu vào bốn bảng:

    1. notifications
       Lưu nội dung chung của notification.

    2. notification_recipients
       Lưu trạng thái riêng của từng người nhận.

    3. notification_deliveries
       Lưu trạng thái gửi của từng user theo từng channel.

    4. notification_outbox
       Lưu các Redis Stream target cần publish.

    QUAN TRỌNG:
    - Hàm này chỉ db.add() và db.flush().
    - Hàm không tự db.commit().
    - Caller chịu trách nhiệm mở transaction.
    - Nếu flush lỗi, transaction bên ngoài phải rollback.
    - Hàm này phải chạy bằng một AsyncSession riêng.
    """
    # Việc đặt biến ngắn giúp phần ánh xạ xuống ORM dễ đọc hơn.
    actor = event.actor
    resource = event.resource
    action = event.action
    deduplication = event.deduplication
    trace = event.trace
    policy = event.delivery_policy

    # ========================================================
    # TẠO BẢN GHI NOTIFICATIONS
    # ========================================================
    #
    # Mỗi NotificationEvent chỉ tạo đúng một dòng notifications.
    #
    # Dòng này chứa:
    # - nội dung;
    # - actor;
    # - resource;
    # - action;
    # - policy;
    # - trace;
    # - metadata;
    # - thời gian hiệu lực.

    notification_row = Notifications(
        # ----------------------------------------------------
        # Định danh Noti này
        # ----------------------------------------------------
        notification_id=event.notification_id,
        event_id=event.event_id,
        schema_version=event.schema_version,

        # ----------------------------------------------------
        # Tenant và loại thông báo
        # ----------------------------------------------------
        tenant_id=event.tenant_id,
        notification_type=event.notification_type,
        category=event.category,

        # ----------------------------------------------------
        # Nguồn phát sinh event
        # ----------------------------------------------------
        source_service=event.source.service,
        source_instance=event.source.instance,
        environment=event.source.environment,

        # ----------------------------------------------------
        # Actor
        # ----------------------------------------------------
        # Actor có thể None, ví dụ notification không xác định rõ người hoặc service thực hiện hành động thì sẽ để None, hoặc ghi là system cũng được
        actor_type=(
            enum_value(actor.type)
            if actor
            else None    # "system"
        ),
        actor_id=actor.id if actor else None,
        actor_name=actor.display_name if actor else None,
        actor_avatar_url=actor.avatar_url if actor else None,

        # ----------------------------------------------------
        # Resource
        # ----------------------------------------------------
        resource_type=resource.type if resource else None,
        resource_id=resource.id if resource else None,
        resource_version=resource.version if resource else None,

        # ----------------------------------------------------
        # Audience
        # ----------------------------------------------------
        audience_type=enum_value(event.audience.type),

        # Lưu snapshot audience ban đầu dưới dạng JSON.
        # Bảng notification_recipients phía dưới mới là danh sách người nhận thực tế đã được mở rộng.
        audience_json=json_dumps(
            event.audience.model_dump(mode="json")
        ),

        # Lưu danh sách transport channel.
        #
        # Ví dụ:
        # ["in_app", "websocket"] sẽ trả về chuỗi: '["in_app","websocket"]'
        channels_json=json_dumps(
            [
                enum_value(channel)
                for channel in event.channels
            ]
        ),

        # ----------------------------------------------------
        # Nội dung của thông báo
        # ----------------------------------------------------
        template_key=event.content.template_key,
        template_version=event.content.template_version,
        locale=event.content.locale,

        title=event.content.title,
        body=event.content.body,
        short_body=event.content.short_body,
        image_url=event.content.image_url,

        # Các biến dùng để render template.
        variables_json=json_dumps(
            event.content.variables
        ),

        # ----------------------------------------------------
        # Action
        # ----------------------------------------------------
        action_type=enum_value(action.type),
        action_target=action.target,
        action_label=action.label,
        action_method=action.method,
        action_open_new_tab=action.open_in_new_tab,

        # ----------------------------------------------------
        # Dữ liệu nghiệp vụ mở rộng
        # ----------------------------------------------------
        data_json=json_dumps(event.data),
        priority=enum_value(event.priority),
        severity=enum_value(event.severity),

        # ----------------------------------------------------
        # Chống trùng
        # ----------------------------------------------------
        deduplication_key=(
            deduplication.key
            if deduplication
            else None
        ),
        deduplication_window_seconds=(
            deduplication.window_seconds
            if deduplication
            else None
        ),

        # ----------------------------------------------------
        # Chính sách gửi
        # ----------------------------------------------------
        delivery_ttl_seconds=policy.ttl_seconds,
        max_attempts=policy.max_attempts,
        retry_delay_seconds=policy.retry_delay_seconds,
        require_ack=policy.require_ack,

        # ----------------------------------------------------
        # Trace
        # ----------------------------------------------------
        correlation_id=trace.correlation_id,
        trace_id=trace.trace_id,
        causation_id=trace.causation_id,

        # ----------------------------------------------------
        # Trạng thái vòng đời và thời gian
        # ----------------------------------------------------
        status=enum_value(event.status),

        available_at=event.available_at,
        expires_at=event.expires_at,
        created_at=event.created_at,
        updated_at=event.created_at,
    )

    # Chỉ đưa object vào SQLAlchemy session.
    # Chưa INSERT ngay và chưa commit.
    db.add(notification_row)

    # ========================================================
    # XÁC ĐỊNH NGƯỜI NHẬN THỰC TẾ
    # ========================================================

    # Tạo 1 set user bị loại khỏi audience. Set sẽ tự loại bỏ các giá trị trùng lặp nhau và chỉ giữ lại 1 bản
    # 1. Khởi tạo một set trống để chứa các ID hợp lệ
    # excluded_user_ids = set()
    # 2. Vòng lặp duyệt qua từng ID trong danh sách gốc
    # for user_id in event.audience.exclude_user_ids:
    #     # 3. Kiểm tra nếu ID không bị None/Rỗng VÀ không phải chuỗi chứa toàn khoảng trắng
    #     if user_id and user_id.strip():
    #         # 4. Làm sạch khoảng trắng ở hai đầu chuỗi
    #         cleaned_user_id = user_id.strip()
    #         # 5. Thêm ID đã làm sạch vào set (set sẽ tự loại bỏ nếu ID này đã tồn tại)
    #         excluded_user_ids.add(cleaned_user_id)

    # Hàm rút gọn
    excluded_user_ids = {
        user_id.strip()
        for user_id in event.audience.exclude_user_ids
        if user_id and user_id.strip()
    }

    # Tạo danh sách người nhận thực tế bằng hàm sort nhận vào 1 set và trả về 1 list
    #
    # Các xử lý:
    # - loại chuỗi rỗng;
    # - strip khoảng trắng;
    # - loại user nằm trong exclude_user_ids;
    # - dùng set để loại ID trùng;
    # - sorted để thứ tự ổn định, dễ test và debug.
    recipient_user_ids = sorted(
        {
            user_id.strip()
            for user_id in event.audience.user_ids
            if (
                user_id
                and user_id.strip()
                and user_id.strip() not in excluded_user_ids
            )
        }
    )

    # ========================================================
    # LOẠI CHANNEL TRÙNG
    # ========================================================
    # Ví dụ event vô tình có:
    # ["websocket", "websocket", "in_app"]
    # thì chỉ tạo delivery cho:
    # ["websocket", "in_app"]

    unique_channels: list[Any] = []
    channel_names_seen: set[str] = set()

    for channel in event.channels:
        # Lấy từng giá trị trong event.channels
        channel_name = enum_value(channel)
        if channel_name in channel_names_seen:
            continue   # Bỏ qua channel này nếu nó đã tồn tại trong channel_names_seen

        channel_names_seen.add(channel_name)
        unique_channels.append(channel)

    # ========================================================
    # TẠO RECIPIENT VÀ DELIVERY
    # ========================================================

    recipient_rows: list[NotificationRecipients] = []
    delivery_rows: list[NotificationDeliveries] = []

    # Duyệt danh sách và xử lý từng người nhận
    for recipient_user_id in recipient_user_ids:
        # Một người nhận tạo một dòng notification_recipients.
        recipient_rows.append(
            NotificationRecipients(
                notification_id=event.notification_id,
                tenant_id=event.tenant_id,
                recipient_user_id=recipient_user_id,
                # Khi vừa tạo, user chưa xem notification.
                state="unread",
                created_at=event.created_at,
                updated_at=event.created_at,
            )
        )

        # ----------------------------------------------------
        # Một user tạo một delivery cho mỗi channel.
        # ----------------------------------------------------
        #
        # Ví dụ:
        # User admin_02
        # Channels = in_app + websocket (Có nghĩa là thông báo được truyền đi bằng 2 cách, 1 là websocket, 1 là trong app)
        #
        # sẽ tạo:
        # - admin_02 + in_app
        # - admin_02 + websocket
        for channel in unique_channels:
            delivery_rows.append(
                NotificationDeliveries(
                    delivery_id=create_public_id("delivery"),

                    notification_id=event.notification_id,
                    tenant_id=event.tenant_id,
                    recipient_user_id=recipient_user_id,

                    channel=enum_value(channel),

                    # Chưa được worker/provider xử lý. Có nghĩa là chưa gửi đi
                    status="pending",

                    attempt_count=0,
                    max_attempts=policy.max_attempts,

                    created_at=event.created_at,
                    updated_at=event.created_at,
                )
            )

    # Chỉ add_all khi list có dữ liệu.
    if recipient_rows:
        db.add_all(recipient_rows)

    if delivery_rows:
        db.add_all(delivery_rows)

    # ========================================================
    # TẠO OUTBOX CHO TỪNG REDIS STREAM TARGET
    # ========================================================

    # Loại target trùng dựa trên stream_key.
    #
    # Ví dụ cùng user được thêm hai lần thì vẫn chỉ có một Outbox cho user stream đó.
    unique_stream_targets = deduplicate_targets(
        stream_targets
    )

    # Serialize event một lần duy nhất.
    #
    # Không nên gọi json_dumps(event...) trong mỗi vòng lặp vì:
    # - tốn CPU;
    # - kết quả hoàn toàn giống nhau;
    # - số target càng nhiều thì chi phí càng lớn.
    event_payload_json = json_dumps(
        event.model_dump(mode="json")
    )

    outbox_rows: list[NotificationOutbox] = []
    outbox_ids: list[str] = []

    for target in unique_stream_targets:
        outbox_id = create_public_id("outbox")
        stream_key = target.get_stream_key()

        outbox_rows.append(
            NotificationOutbox(
                # ID riêng của bản ghi Outbox.
                outbox_id=outbox_id,

                # Cùng một event_id có thể xuất hiện ở nhiều stream.
                event_id=event.event_id,

                tenant_id=event.tenant_id,

                # Aggregate là notification.
                aggregate_type="notification",
                aggregate_id=event.notification_id,

                event_type=event.notification_type,

                # Phạm vi Redis Stream:
                # user, tenant, job hoặc global.
                stream_scope=enum_value(target.scope),

                # Redis Stream key thực tế.
                stream_key=stream_key,

                # Thông tin target để audit và query.
                target_tenant_id=target.tenant_id,
                target_user_id=target.user_id,
                target_job_id=target.job_id,

                # Snapshot event hoàn chỉnh.
                #
                # Worker có thể publish độc lập mà không cần join lại bảng notifications.
                payload_json=event_payload_json,

                # Worker sẽ claim các record pending.
                status="pending",

                attempt_count=0,
                max_attempts=outbox_max_attempts,

                # Cho phép gửi ngay hoặc lên lịch trong tương lai.
                available_at=event.available_at,

                created_at=event.created_at,
                updated_at=event.created_at,
            )
        )

        outbox_ids.append(outbox_id)

    if outbox_rows:
        db.add_all(outbox_rows)

    # ========================================================
    #  FLUSH DỮ LIỆU
    # ========================================================
    #
    # flush() có nghĩa:
    # - SQLAlchemy phát các câu INSERT xuống SQL Server;
    # - SQL Server kiểm tra unique/index/nullable;
    # - các ID tự tăng được sinh;
    # - dữ liệu vẫn chưa commit.
    #
    # Nếu flush lỗi:
    # - transaction chuyển sang trạng thái failed;
    # - caller phải rollback;
    # - async with db.begin() sẽ tự rollback khi exception thoát ra.
    await db.flush()

    # ========================================================
    # TRẢ KẾT QUẢ
    # ========================================================

    return NotificationCreateResult(
        notification_id=event.notification_id,
        event_id=event.event_id,
        outbox_ids=outbox_ids,
        recipient_count=len(recipient_rows),
        delivery_count=len(delivery_rows),
        stream_target_count=len(unique_stream_targets),
        created_at=event.created_at,
    )
