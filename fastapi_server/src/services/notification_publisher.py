from __future__ import annotations

from typing import Any

from services.redis_client import get_redis_stream
from utils.events import event_to_redis_fields
from utils.redis_keys import job_stream_key, tenant_stream_key, user_stream_key


# Kết nối Redis dùng chung cho toàn bộ notification service, có thể tái sử dụng để subscribe event nếu cần.
redis_client = get_redis_stream()


async def publish_event_to_stream(
    *,
    stream_key: str,
    event: dict[str, Any],
) -> str:
    """
    Ghi một event vào Redis Stream bằng XADD.

    Redis trả về stream_id, ví dụ:
    1713000000000-0

    stream_id này rất quan trọng:
    - notification service gửi về client
    - client lưu lại làm last_event_id
    - khi reconnect, client gửi last_event_id để replay event bị lỡ
    """
    # Chuyển event dict thành fields để lưu vào Redis Stream phù hợp với Redis Stream (field-value đều là string).
    fields = event_to_redis_fields(event)

    # Ghi event vào Redis Stream với XADD, đồng thời giới hạn số event giữ trong stream để tránh Redis Stream phình quá lớn.
    # maxlen=settings.STREAM_MAXLEN và approximate=True giúp Redis tự động xóa bớt event cũ khi stream đạt đến giới hạn, nhưng không đảm bảo chính xác từng event, điều này giúp tăng hiệu suất.
    # Áp dụng cho tất cả stream user stream, tenant stream, job stream để tránh Redis Stream phình quá lớn.
    event_id = await redis_client.xadd(
        stream_key,
        fields,
        maxlen= 10000,
        approximate=True,
    )

    return str(event_id)


async def publish_job_event(
    *,
    event: dict[str, Any],
    send_to_user: bool = True,
    send_to_tenant: bool = True,
    send_to_job: bool = True,
    send_to_global: bool = True,
) -> dict[str, str]:
    """
    Publish job event vào nhiều stream khác nhau.

    Một event job nên được ghi vào:
    1. user stream:
       - để user tạo job nhận realtime.

    2. tenant dashboard stream:
       - để admin tenant xem toàn bộ job.

    3. job stream:
       - để màn hình chi tiết job nhận event riêng job đó.
    
    4. global stream:
       - để admin/superuser xem toàn bộ event của toàn hệ thống, phục vụ mục đích monitoring/logging.

    Trả về dict các stream_id đã ghi.
    """

    tenant_id = str(event["tenant_id"])
    user_id = str(event["user_id"])
    job_id = str(event["job_id"])

    result: dict[str, str] = {}

    if send_to_user:
        key = user_stream_key(tenant_id, user_id)
        result["user_event_id"] = await publish_event_to_stream(
            stream_key=key,
            event=event,
        )

    if send_to_tenant:
        key = tenant_stream_key(tenant_id)
        result["tenant_event_id"] = await publish_event_to_stream(
            stream_key=key,
            event=event,
        )

    if send_to_job:
        key = job_stream_key(tenant_id, job_id)
        result["job_event_id"] = await publish_event_to_stream(
            stream_key=key,
            event=event,
        )

    if send_to_global:
        key = "global:events"
        result["global_event_id"] = await publish_event_to_stream(
            stream_key=key,
            event=event,
        )

    return result

async def close_notification_publisher() -> None:
    """
    Đóng Redis connection khi app shutdown.
    """

    await redis_client.close()