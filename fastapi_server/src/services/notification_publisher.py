"""
Dịch vụ publish thông báo (Notification Publisher) vào Redis Stream.
"""
from __future__ import annotations
import asyncio
from dataclasses import dataclass
from typing import Any, Iterable
import json
import logging
import os
import socket
from uuid import uuid4
from pydantic import BaseModel

from config.config import settings
from services.redis_client import get_redis_fast
from utils.events import event_to_redis_fields
from utils.redis_keys import (
    OUTBOX_WAKEUP_KEY,
    global_stream_key,
    job_stream_key,
    tenant_stream_key,
    user_stream_key,
)
from db.database import AsyncSessionLocal
from db.query_notification_outbox import (
    claim_outbox_batch,
    claim_outbox_by_id,
    mark_outbox_batch_published,
    mark_outbox_published,
    mark_outbox_retry,
    mark_user_websocket_deliveries_sent_batch,
    mark_user_websocket_delivery_failed,
    mark_user_websocket_delivery_sent,
)
from schemas.noti_schema import NotificationStreamScope

# XADD là lệnh nhanh, không phải lệnh blocking.
# Không dùng get_redis_stream() vì client đó chủ yếu dành cho XREAD BLOCK.
redis_client = get_redis_fast()


# Số event tối đa được giữ gần đúng trong mỗi Redis Stream (tenant dashboard, global, job).
STREAM_MAX_LENGTH = settings.stream_max_length
# Mỗi user có 1 stream riêng. 2.000 user x 10.000 event x ~2KB = ~40GB RAM. 
# Stream cá nhân chỉ cần giữ đủ để client reconnect để lấy lại phần bị lỡ.
STREAM_MAX_LENGTH_USER = int(os.getenv("STREAM_MAX_LENGTH_USER", "500"))

# Stream user/job không có event mới trong khoảng này sẽ tự xóa (EXPIRE được gia hạn mỗi lần XADD).
# User nghỉ việc / job đã xong không chiếm RAM mãi mãi. 0 = không đặt TTL.
STREAM_TTL_SECONDS = int(os.getenv("STREAM_TTL_SECONDS", str(7 * 24 * 3600)))

# Số outbox tối đa xử lý trong 1 lô (1 câu SQL claim + 1 Redis pipeline + 1 câu SQL cập nhật).
OUTBOX_BATCH_SIZE = int(os.getenv("OUTBOX_BATCH_SIZE", "500"))

logger = logging.getLogger("noti.publisher")

# ============================================================
# MÔ TẢ MỘT ĐỐI TƯỢNG CẦN PUBLISH
# ============================================================

@dataclass(frozen=True, slots=True)
class NotificationStreamTarget:
    """
    Tạo 1 class định nghĩa một đối tượng publish  
    - `frozen` sẽ ngăn cấm người dùng sửa dữ liệu sau khi khai báo  
    Ví dụ sau sẽ báo lỗi:  
    ```
    target = NotificationStreamTarget.user(
        tenant_id="company_a",
        user_id="user_01",
    )

    target.user_id = "user_02"
    ```
    - `slots` sẽ ngăn người dùng tạo các thuộc tính mới ngoài các thuộc tính đã định nghĩa  
    Ví dụ sau sẽ báo lỗi:  
    ```
    target.new_field = "abc"
    ```
    """
    scope: NotificationStreamScope
    tenant_id: str | None = None 
    user_id: str | None = None
    job_id: str | None = None

    @classmethod   # Classmethod làm cho method nhận class qua tham số cls
    def user( cls, *, tenant_id: str, user_id: str) -> "NotificationStreamTarget":
        """
        Tạo target cho stream riêng của user.  
        - cls (class): Là đối tượng class, tương tự như self là đối trượng cụ thể, thì cls tự truyền class chính nó  
        Hàm này trả về:
        ```
        NotificationStreamTarget(
            scope=NotificationStreamScope.USER, 
            tenant_id="cong_ty_A", 
            user_id="nhan_vien_01", 
            job_id=None # Tự động mặc định là None
        )
        ```
        """
        return cls(scope=NotificationStreamScope.USER, tenant_id=tenant_id, user_id=user_id,)

    @classmethod
    def tenant(cls, *, tenant_id: str) -> "NotificationStreamTarget":
        """
        Tạo target cho dashboard tenant.
        """
        return cls(scope=NotificationStreamScope.TENANT, tenant_id=tenant_id,)

    @classmethod
    def job( cls, *, tenant_id: str, job_id: str, ) -> "NotificationStreamTarget":
        """
        Tạo target cho stream riêng của job.
        """
        return cls(scope=NotificationStreamScope.JOB, tenant_id=tenant_id, job_id=job_id, )

    @classmethod
    def global_scope(cls) -> "NotificationStreamTarget":
        """
        Tạo target cho stream toàn hệ thống.
        """
        return cls(scope=NotificationStreamScope.GLOBAL)

    def get_stream_key(self) -> str:
        """
        Sinh Redis Stream key tương ứng với đối tượng.  
        Đồng thời validate các ID bắt buộc của đối tượng.
        """
        # Đối với user
        if self.scope == NotificationStreamScope.USER:
            if not self.tenant_id or not self.tenant_id.strip():
                raise ValueError("Mã tổ chức là bắt buộc đối với từng người dùng nhận thông báo")

            if not self.user_id :
                raise ValueError("Mã người dùng không được cung cấp khi gửi thông báo")

            # Nếu cung cấp đủ thông tin thì tạo key để gửi vào redis stream
            return user_stream_key(self.tenant_id, self.user_id,)

        if self.scope == NotificationStreamScope.TENANT:
            if not self.tenant_id or not self.tenant_id.strip():
                raise ValueError("Mã tổ chức là bắt buộc đối với tenant stream")

            return tenant_stream_key(self.tenant_id,)

        if self.scope == NotificationStreamScope.JOB:
            if not self.tenant_id or not self.tenant_id.strip():
                raise ValueError("Mã tổ chức là bắt buộc đối với job stream")

            if not self.job_id:
                raise ValueError("Mã công việc là bắt buộc đối với job stream")

            return job_stream_key( self.tenant_id, self.job_id )

        if self.scope == NotificationStreamScope.GLOBAL:
            return global_stream_key()

        raise ValueError(f"Không hỗ trợ phạm vi thông báo: {self.scope}, vui lòng thử lại")

    def to_dict(self) -> dict[str, Any]:
        """
        Chuyển thông tin đối tượng thành dict để log hoặc trả response.
        """
        return {
            "scope": self.scope.value,
            "tenant_id": self.tenant_id,
            "user_id": self.user_id,
            "job_id": self.job_id,
            "stream_key": self.get_stream_key(),
        }


# ============================================================
# CHUẨN HÓA EVENT
# ============================================================
def create_direct_publisher_id() -> str:
    """
    Tạo ID định danh cho Direct Publisher.  
    Ví dụ: `direct:api-container-01:a8c19f0b`  
    ID này được ghi vào `notification_outbox.locked_by`. Mục đích:
    - biết process nào đang xử lý;
    - chỉ process đó được cập nhật kết quả;
    - hỗ trợ debug Outbox bị kẹt.
    """
    # Lấy hostname của container hoặc server đang chạy API.
    hostname = socket.gethostname()
    return (
        f"direct:{hostname}:"
        f"{uuid4().hex[:8]}"
    )

def stream_id_to_str(stream_id: Any) -> str:
    """
    Chuyển `Redis Stream ID` về chuỗi "1781685000123-0".  
    `redis_fast_client` dùng `decode_responses=False` nên `XADD` trả về bytes `(b'1781685000123-0')`   
    Nếu dùng `str(bytes)` sẽ ra `"b'1781685000123-0'"` (sai), vì vậy phải decode.
    """
    if isinstance(stream_id, (bytes, bytearray)):
        return stream_id.decode("utf-8")
    return str(stream_id)

def normalize_event(event: dict[str, Any] | BaseModel) -> dict[str, Any]:
    """
    Hỗ trợ cả hai loại event:

    - Event dạng Pydantic Notification.  
    ```
    notification_event = NotificationEvent(
        notification_id="noti_001",
        event_id="evt_001",
        title="Có phiếu nhập kho mới",
        created_at=datetime(
            2026,
            6,
            23,
            8,
            30,
            15,
            245000,
            tzinfo=timezone.utc,
        ),
        recipient_ids=[
            "user_001",
            "user_002",
        ],
    )
    ```
    - Event dạng dict.
    ```
    {
        "notification_id": "noti_001",
        "event_id": "evt_001",
        "title": "Có phiếu nhập kho mới",
        "created_at": "2026-06-23T08:30:15.245000Z",
        "recipient_ids": [
            "user_001",
            "user_002",
        ],
    }
    ```

    Sẽ đều được trả về:  
    ```
    {
        "notification_id": "noti_001",
        "event_id": "evt_001",
        "title": "Có phiếu nhập kho mới",
        "created_at": "2026-06-23T08:30:15.245000Z",
        "recipient_ids": [
            "user_001",
            "user_002",
        ],
    }
    ```

    Redis publisher luôn làm việc với dict cuối cùng.
    """
    # Đầu tiên nếu là BaseModel thì chuyển nó sang dạng phù hợp với json
    if isinstance(event, BaseModel):
        return event.model_dump(mode="json")

    # Nếu là dict thì tạo 1 bản copy của dict (dict lồng dict) để trả về
    if isinstance(event, dict):
        return dict(event)

    raise TypeError("Event thông báo phải là dict hoặc Pydantic BaseModel")

def deduplicate_targets(targets: Iterable[NotificationStreamTarget]) -> list[NotificationStreamTarget]:
    """
    Loại bỏ các đối tượng nhận thông báo có cùng Redis Stream key.  
    Ví dụ `admin_user_ids` bị trùng thì event vẫn chỉ được ghi một lần vào `user stream` tương ứng.
    """
    # Tạo dict chứa các đối tượng nhận thông báo
    unique_targets: dict[str, NotificationStreamTarget] = {}

    for target in targets:
        stream_key = target.get_stream_key()
        # Tạo 1 dict có dạng: stream_key: target
        # "stream:noti:tenant:company_a:user:user_01": user_target
        unique_targets[stream_key] = target
    # Chuyển thành list
    return list(unique_targets.values())

# ============================================================
# PUBLISH MỘT EVENT VÀO MỘT KEY STREAM
# ============================================================
async def publish_event_to_stream(*, target: NotificationStreamTarget, event: dict[str, Any] | BaseModel) -> str:
    """
    Ghi một event vào đúng một Redis Stream.  
    Redis trả về stream_id dạng: `1781685000123-0`  
    `stream_id` này được dùng để:  
    - trả về WebSocket client
    - client lưu làm `last_stream_id` để replay event khi client reconnect.  
    """
    # Lấy key để stream từ target
    stream_key = target.get_stream_key()
    # Chuẩn hóa lại event
    event_dict = normalize_event(event)

    # Chuyển toàn bộ event thành field-value dạng string.
    fields = event_to_redis_fields(event_dict)

    # Ghi thêm thông tin routing để khi debug Redis có thể biết event đang nằm trong loại stream nào.
    fields["stream_scope"] = target.scope.value
    fields["stream_key"] = stream_key

    # Lấy các id của đối tượng trong event
    if target.tenant_id:
        fields["target_tenant_id"] = target.tenant_id

    if target.user_id:
        fields["target_user_id"] = target.user_id

    if target.job_id:
        fields["target_job_id"] = target.job_id

    # Tiến hành gửi event vào Redis Stream với key tương ứng
    stream_id = await redis_client.xadd(
        stream_key,
        fields,
        maxlen=STREAM_MAX_LENGTH,
        approximate=True,
    )

    # Trả về stream id để client có thể dùng nó lấy các event trong Redis Stream
    return stream_id_to_str(stream_id)

# ============================================================
# PUBLISH MỘT NOTIFICATION
# ============================================================
async def publish_notification_event(*, event: dict[str, Any] | BaseModel, targets: Iterable[NotificationStreamTarget], max_concurrency: int = 20) -> dict[str, Any]:
    """
    Publish cùng một `notification event` vào nhiều `Redis Stream`  
    Hàm không dừng toàn bộ chỉ vì một stream bị lỗi  
    Ví dụ:  
    - user stream thành công;
    - tenant stream thành công;
    - global stream lỗi;

    Kết quả vẫn ghi rõ stream nào thành công và stream nào thất bại.
    """
    # Loại bỏ các target trùng nhau (cùng stream key)
    resolved_targets = deduplicate_targets(targets)

    if not resolved_targets:
        raise ValueError("Phải có ít nhất một đối tượng nhận thông báo (target) để publish thông báo")

    # Giới hạn số lượng target được publish đồng thời để tránh quá tải Redis hoặc quá nhiều kết nối TCP.
    semaphore = asyncio.Semaphore(max_concurrency)

    async def publish_one(target: NotificationStreamTarget) -> dict[str, Any]:
        """
        Publish một target và giữ exception bên trong kết quả.
        """
        async with semaphore:
            # Lấy stream key để log và trả về kết quả
            stream_key = target.get_stream_key()
            try:
                stream_id = await publish_event_to_stream(target=target, event=event)

                return {
                    "success": True,
                    "scope": target.scope.value,
                    "stream_key": stream_key,
                    "stream_id": stream_id,
                    "tenant_id": target.tenant_id,
                    "user_id": target.user_id,
                    "job_id": target.job_id,
                    "error_type": None,
                    "error_message": None,
                }

            except Exception as exc: # pylint: disable=broad-except
                return {
                    "success": False,
                    "scope": target.scope.value,
                    "stream_key": stream_key,
                    "stream_id": None,
                    "tenant_id": target.tenant_id,
                    "user_id": target.user_id,
                    "job_id": target.job_id,
                    "error_type": type(exc).__name__,
                    "error_message": str(exc),
                }

    # Dùng asyncio.gather để publish đồng thời nhiều target, nhưng vẫn giữ kết quả của từng target.
    target_results = await asyncio.gather(
        *[
            publish_one(target)
            for target in resolved_targets
        ]
    )

    # Danh sách các target publish thành công
    published = [
        item
        for item in target_results
        if item["success"]
    ]
    # Danh sách các target publish thất bại
    failed = [
        item
        for item in target_results
        if not item["success"]
    ]

    return {
        # True khi toàn bộ target đều thành công.
        "success": len(failed) == 0,

        # Có ít nhất một target được publish thành công.
        "partially_published": (
            len(published) > 0
            and len(failed) > 0
        ),

        "target_count": len(target_results),
        "published_count": len(published),
        "failed_count": len(failed),

        "published": published,
        "failed": failed,
    }

async def publish_claimed_outbox_to_redis(*, outbox: dict[str, Any]) -> str:
    """
    Publish một Outbox đã claim vào Redis Stream.

    Tham số outbox phải chứa tối thiểu:
    - outbox_id
    - event_id
    - notification_id
    - event_type
    - stream_scope
    - stream_key
    - payload_json
    """
    # Lấy nội dung payload_json từ outbox, đây là JSON string chứa thông tin event.
    payload_json = outbox["payload_json"]

    try:
        payload = json.loads(payload_json)
    except json.JSONDecodeError as exc:
        raise ValueError("payload_json của Outbox không phải JSON hợp lệ") from exc

    if not isinstance(payload, dict):
        raise ValueError("payload_json của Outbox phải là JSON object")

    # Chuyển payload thành field-value dạng string để ghi vào Redis Stream.
    fields = event_to_redis_fields(payload)

    # Thêm metadata của Outbox để debug.
    fields["outbox_id"] = str(outbox["outbox_id"])
    fields["event_id"] = str(outbox["event_id"])
    fields["notification_id"] = str(outbox["notification_id"])
    fields["event_type"] = str(outbox["event_type"])
    fields["stream_scope"] = str(outbox["stream_scope"])

    # Thêm thông tin target nếu có.
    if outbox.get("target_tenant_id"):
        fields["target_tenant_id"] = str(outbox["target_tenant_id"])

    if outbox.get("target_user_id"):
        fields["target_user_id"] = str(outbox["target_user_id"])

    if outbox.get("target_job_id"):
        fields["target_job_id"] = str(outbox["target_job_id"])

    # # XADD là lệnh nhanh, không dùng client dành riêng cho XREAD BLOCK.
    # redis_client = get_redis_fast()

    # Ghi event vào Redis Stream với key tương ứng.
    stream_id = await redis_client.xadd(
        str(outbox["stream_key"]),
        fields,
        # Giới hạn độ dài gần đúng của Redis Stream.
        maxlen=STREAM_MAX_LENGTH,
        approximate=True,
    )

    return stream_id_to_str(stream_id)

async def publish_one_outbox_immediately(*, outbox_id: str, worker_id: str | None = None) -> dict[str, Any]:
    """
    Claim và publish ngay một Outbox vào Redis Stream.  
    Hàm này được sử dụng trong chế độ `DIRECT`:  
        1. `Notification` đã được commit vào `SQL Server`.  
        2. `Outbox` đã tồn tại với `status=pending`.  
        3. API thử publish ngay thay vì chờ worker.  

    Nguyên tắc an toàn:
    -------------------
    - Dùng database session riêng.
    - Claim Outbox trước khi gọi Redis.
    - Không giữ SQL transaction trong lúc chờ Redis.
    - Redis lỗi không raise ra RoleController.
    - Outbox lỗi được đưa lại pending để worker retry.
    - Direct Publisher và Worker không xử lý cùng một Outbox.

    Kết quả có thể là:
    ------------------
    published:
        Publish Redis thành công và Outbox đã được cập nhật.

    not_claimed:
        Outbox đã được process khác claim/xử lý.

    publish_failed:
        Redis lỗi, Outbox được đưa lại pending hoặc failed.

    published_state_update_failed:
        Redis XADD thành công nhưng cập nhật DB thất bại.

    internal_error:
        Lỗi khi mở session, claim hoặc thao tác SQL Server.
    """

    # API (Direct Publisher) không truyền worker_id -> tự tạo "direct:...".
    # Outbox worker truyền ID của nó ("worker:...") để log/locked_by biết ai đã xử lý.
    # Cả hai dùng chung đúng 1 logic claim -> XADD -> đánh dấu, không bị lệch nhau.
    worker_id = worker_id or create_direct_publisher_id()

    try:
        # ====================================================
        # MỞ SESSION RIÊNG CHO QUÁ TRÌNH PUBLISH ĐỂ KHÔNG ẢNH HƯỞNG NGHIỆP VỤ KHÁC
        # ====================================================
        async with AsyncSessionLocal() as db:

            # =================================================
            # BƯỚC 1: CLAIM OUTBOX
            # =================================================
            async with db.begin():
                claimed_outbox = await claim_outbox_by_id(db, outbox_id=outbox_id, worker_id=worker_id)

            # Nếu claim không thành công, không được publish.
            if claimed_outbox is None:
                return {
                    "status": "not_claimed",
                    "outbox_id": outbox_id,
                    "worker_id": worker_id,
                    "message": (
                        "Outbox không còn pending, chưa đến thời gian "
                        "xử lý hoặc đã được process khác claim"
                    ),
                }

            # =================================================
            # BƯỚC 2: PUBLISH REDIS
            # =================================================
            # Đặc biệt không đặt XADD bên trong SQL transaction.
            # Nếu giữ transaction trong khi gọi mạng:
            # - SQL connection bị chiếm lâu;
            # - giữ lock lâu;
            # - tăng nguy cơ deadlock;
            # - Redis chậm sẽ làm transaction SQL chậm theo.
            try:
                stream_id = await publish_claimed_outbox_to_redis(outbox=claimed_outbox)
            except Exception as publish_exc: # pylint: disable=broad-except
                # =============================================
                # REDIS PUBLISH THẤT BẠI
                # =============================================
                #
                # Mở một transaction mới để:
                # - đưa Outbox về pending;
                # - hoặc chuyển failed nếu hết lượt;
                # - ghi lại lỗi;
                # - cập nhật delivery cho user stream.
                async with db.begin():
                    retry_result = await mark_outbox_retry(
                        db,
                        outbox_id=outbox_id,
                        worker_id=worker_id,
                        error_message=(
                            f"{type(publish_exc).__name__}: "
                            f"{publish_exc}"
                        ),
                        attempt_count=claimed_outbox["attempt_count"],
                        max_attempts=claimed_outbox["max_attempts"],
                    )

                    # Chỉ user stream ánh xạ trực tiếp tới một recipient cụ thể.
                    # Tenant/global stream là broadcast route, không nên cập nhật tất cả recipient ở đây.
                    if claimed_outbox["stream_scope"] == "user" and claimed_outbox.get("target_user_id"):
                        await mark_user_websocket_delivery_failed(
                            db,
                            notification_id= claimed_outbox["notification_id"],
                            recipient_user_id=claimed_outbox["target_user_id"],
                            error_code=type(publish_exc).__name__,
                            error_message=str(publish_exc),
                            next_retry_at=retry_result["available_at"],
                        )

                return {
                    "status": "publish_failed",
                    "outbox_id": outbox_id,
                    "worker_id": worker_id,
                    "stream_scope": claimed_outbox["stream_scope"],
                    "stream_key": claimed_outbox["stream_key"],
                    # pending hoặc failed.
                    "outbox_status": retry_result["status"],
                    "next_retry_at": retry_result["available_at"].isoformat(),
                    "error_type": type(publish_exc).__name__,
                    "error_message": str(publish_exc),
                }

            # =================================================
            # BƯỚC 3: REDIS THÀNH CÔNG, CẬP NHẬT DATABASE
            # =================================================
            try:
                async with db.begin():
                    outbox_updated = await mark_outbox_published(db, outbox_id=outbox_id, worker_id=worker_id, stream_id=stream_id)

                    # Với user stream, có thể xác định chính xác WebSocket delivery của user nào.
                    if (
                        claimed_outbox["stream_scope"] == "user"
                        and claimed_outbox.get("target_user_id")
                    ):
                        await mark_user_websocket_delivery_sent(
                            db,
                            notification_id=claimed_outbox["notification_id"],
                            recipient_user_id=claimed_outbox["target_user_id"],
                            stream_id=stream_id,
                        )

            except Exception as state_update_exc: # pylint: disable=broad-except
                # Redis đã nhận event nhưng SQL Server chưa cập nhật trạng thái published.
                # Khi worker recovery xử lý lại, event có khả năng được XADD thêm lần nữa.
                # Vì vậy client cần chống trùng bằng event_id.
                return {
                    "status": "published_state_update_failed",
                    "outbox_id": outbox_id,
                    "worker_id": worker_id,
                    "stream_scope": claimed_outbox["stream_scope"],
                    "stream_key": claimed_outbox["stream_key"],
                    "stream_id": stream_id,
                    "error_type": type(state_update_exc).__name__,
                    "error_message": str(state_update_exc),
                    "message": "Redis đã nhận event nhưng SQL Server không cập nhật được trạng thái Outbox",
                }

            if not outbox_updated:
                return {
                    "status": "published_state_not_updated",
                    "outbox_id": outbox_id,
                    "worker_id": worker_id,
                    "stream_scope": claimed_outbox["stream_scope"],
                    "stream_key": claimed_outbox["stream_key"],
                    "stream_id": stream_id,
                    "message": "Redis đã nhận event nhưng bản ghi Outbox không còn thuộc quyền xử lý của process này",
                }

            # =================================================
            # BƯỚC 4: HOÀN THÀNH
            # =================================================
            return {
                "status": "published",
                "outbox_id": outbox_id,
                "worker_id": worker_id,
                "event_id": claimed_outbox["event_id"],
                "notification_id": claimed_outbox["notification_id"],
                "stream_scope": claimed_outbox["stream_scope"],
                "stream_key": claimed_outbox["stream_key"],
                "target_tenant_id": claimed_outbox.get("target_tenant_id"),
                "target_user_id": claimed_outbox.get("target_user_id"),
                "target_job_id": claimed_outbox.get("target_job_id"),
                # Redis XADD ID.
                "stream_id": stream_id,
            }

    except Exception as exc: # pylint: disable=broad-except
        # Lỗi ở mức session, claim hoặc SQL Server.
        # Không raise lại để không ảnh hưởng nghiệp vụ role.
        return {
            "status": "internal_error",
            "outbox_id": outbox_id,
            "worker_id": worker_id,
            "error_type": type(exc).__name__,
            "error_message": str(exc),
        }

# ============================================================
# PUBLISH THEO LÔ
# ============================================================

def _stream_limits(stream_scope: str) -> tuple[int, int]:
    """
    Trả về (maxlen, ttl_seconds) theo loại stream.  
    `ttl=0` nghĩa là không đặt TTL.
    """
    if stream_scope == NotificationStreamScope.USER.value:
        return STREAM_MAX_LENGTH_USER, STREAM_TTL_SECONDS
    if stream_scope == NotificationStreamScope.JOB.value:
        return STREAM_MAX_LENGTH, STREAM_TTL_SECONDS
    return STREAM_MAX_LENGTH, 0

def _outbox_to_redis_fields(outbox: dict[str, Any]) -> dict[str, str]:
    """
    Chuyển 1 dòng outbox lấy được từ DB thành `field-value` cho `XADD`.
    """
    payload = json.loads(outbox["payload_json"])
    if not isinstance(payload, dict):
        raise ValueError("payload_json của Outbox phải là JSON object")

    fields = event_to_redis_fields(payload)
    fields["outbox_id"] = str(outbox["outbox_id"])
    fields["event_id"] = str(outbox["event_id"])
    fields["notification_id"] = str(outbox["notification_id"])
    fields["event_type"] = str(outbox["event_type"])
    fields["stream_scope"] = str(outbox["stream_scope"])
    for key in ("target_tenant_id", "target_user_id", "target_job_id"):
        if outbox.get(key):
            fields[key] = str(outbox[key])
    return fields

async def publish_outbox_batch(*, worker_id: str, limit: int = OUTBOX_BATCH_SIZE, outbox_ids: list[str] | None = None) -> dict[str, Any]:
    """
    Claim + publish + cập nhật trạng thái cho tối đa `limit` outbox trong 3 bước:

        1. SQL  : claim_outbox_batch            (1 câu lệnh, pending -> processing)
        2. Redis: pipeline XADD (+ EXPIRE)      (1 round-trip cho cả lô)
        3. SQL  : mark_outbox_batch_published   (1 câu lệnh)
                  + mark_user_websocket_deliveries_sent_batch (1 câu lệnh)

    Outbox publish lỗi được đưa lại pending (backoff) từng cái - đây là đường hiếm gặp.
    Nếu bước 3 lỗi, outbox kẹt ở 'processing' và sẽ được recover_stale_processing_outbox()
    trả về pending sau STALE_AFTER_SECONDS (có thể publish lặp; client chống trùng bằng notification_id).
    """
    # Lấy danh sách outbox claim được trong một transaction SQL.
    async with AsyncSessionLocal() as db:
        async with db.begin():
            claimed = await claim_outbox_batch(db, worker_id=worker_id, limit=limit, outbox_ids=outbox_ids)

    result: dict[str, Any] = {
        "claimed_count": len(claimed),
        "published_count": 0,
        "failed": [],
    }

    # Nếu không claim được outbox nào, trả về ngay.
    if not claimed:
        return result

    # Chuẩn bị dữ liệu để gửi vào Redis Stream.
    prepared: list[dict[str, Any]] = []
    failures: list[tuple[dict[str, Any], str]] = []
    # Danh sách các outbox đã publish thành công (row, stream_id).
    published: list[tuple[dict[str, Any], str]] = []

    for row in claimed:
        try:
            prepared.append(
                {
                    "row": row,
                    "fields": _outbox_to_redis_fields(row)
                }
            )
        except Exception as exc:  # pylint: disable=broad-except
            failures.append((row, f"{type(exc).__name__}: {exc}"))

    if prepared:
        try:
            # transaction=False: chỉ gom lệnh để giảm round-trip, không cần MULTI/EXEC.
            pipe = redis_client.pipeline(transaction=False)
            for item in prepared:
                # Lấy dữ liệu gốc từ DB để truy cập các thông tin như stream_scope, stream_key.
                row = item["row"]
                # Lấy giới hạn maxlen và ttl theo loại stream.
                maxlen, ttl = _stream_limits(str(row["stream_scope"]))
                # Thêm một entry vào Redis Stream với key tương ứng, maxlen và ttl.
                pipe.xadd(str(row["stream_key"]), item["fields"], maxlen=maxlen, approximate=True)
                if ttl > 0:
                    pipe.expire(str(row["stream_key"]), ttl)

            # Thực hiện tất cả lệnh trong pipeline và lấy kết quả.
            replies = await pipe.execute(raise_on_error=False)

            # Lấy kết quả XADD của từng outbox (bỏ qua kết quả EXPIRE xen giữa).
            reply_iter = iter(replies)
            for item in prepared:
                row = item["row"]
                xadd_reply = next(reply_iter)
                # Bỏ qua kết quả EXPIRE
                if _stream_limits(str(row["stream_scope"]))[1] > 0:
                    next(reply_iter)
                if isinstance(xadd_reply, Exception):
                    failures.append((row, f"{type(xadd_reply).__name__}: {xadd_reply}"))
                else:
                    published.append((row, stream_id_to_str(xadd_reply)))
        except Exception as exc:  # pylint: disable=broad-except
            error = f"{type(exc).__name__}: {exc}"
            # Coi như tất cả các outbox trong lô này đều thất bại nếu pipeline gặp lỗi.
            failures.extend((item["row"], error) for item in prepared)

    # Cập nhật trạng thái của các outbox đã publish thành công vào database.
    if published:
        async with AsyncSessionLocal() as db:
            async with db.begin():
                updated = await mark_outbox_batch_published(
                    db,
                    worker_id=worker_id,
                    items=[(str(row["outbox_id"]), sid) for row, sid in published],
                )
                # Với user stream, đánh dấu các delivery đã gửi thành công.
                await mark_user_websocket_deliveries_sent_batch(
                    db,
                    items=[
                        (str(row["notification_id"]), str(row["target_user_id"]), sid)
                        for row, sid in published
                        if row["stream_scope"] == NotificationStreamScope.USER.value
                        and row.get("target_user_id")
                    ],
                )

        # Cập nhật số lượng outbox đã publish thành công vào kết quả trả về.
        result["published_count"] = len(published)
        if updated != len(published):
            logger.warning(
                "Published %s outbox nhưng chỉ cập nhật được %s dòng (bị recover/khóa bởi process khác?)",
                len(published), updated,
            )

    # Nếu có outbox publish thất bại, đưa chúng về pending hoặc failed và ghi lại lỗi.
    if failures:
        async with AsyncSessionLocal() as db:
            async with db.begin():
                for row, error in failures:
                    retry = await mark_outbox_retry(
                        db,
                        outbox_id=str(row["outbox_id"]),
                        worker_id=worker_id,
                        error_message=error,
                        attempt_count=row["attempt_count"],
                        max_attempts=row["max_attempts"],
                    )
                    if row["stream_scope"] == NotificationStreamScope.USER.value and row.get("target_user_id"):
                        await mark_user_websocket_delivery_failed(
                            db,
                            notification_id=str(row["notification_id"]),
                            recipient_user_id=str(row["target_user_id"]),
                            error_code=error.split(":", 1)[0][:100],
                            error_message=error,
                            next_retry_at=retry["available_at"],
                        )
                    result["failed"].append({
                        "status": "publish_failed",
                        "outbox_id": row["outbox_id"],
                        "stream_key": row["stream_key"],
                        "outbox_status": retry["status"],
                        "error_message": error,
                    })
    return result

async def publish_multiple_outboxes_immediately(*, outbox_ids: list[str], worker_id: str | None = None) -> dict[str, Any]:
    """
    Đẩy ngay các thông báo vừa tạo (chế độ DIRECT), xử lý theo lô `OUTBOX_BATCH_SIZE`.

    Outbox không claim được (đã được worker/process khác xử lý, hoặc chưa đến hạn)
    được trả về với status "not_claimed" - đây không phải lỗi.
    """
    # Tạo một worker_id định danh cho Direct Publisher nếu không có.
    worker_id = worker_id or create_direct_publisher_id()

    # Các biến thống kê kết quả
    published_count = 0
    claimed_count = 0
    errored_count = 0
    failed: list[dict[str, Any]] = []

    # Chia outbox_ids thành các lô nhỏ để claim + publish, tránh quá tải Redis hoặc SQL.
    for start in range(0, len(outbox_ids), OUTBOX_BATCH_SIZE):
        # Lấy một lô outbox_ids
        chunk = list(outbox_ids[start:start + OUTBOX_BATCH_SIZE])
        try:
            batch = await publish_outbox_batch(worker_id=worker_id, limit=len(chunk), outbox_ids=chunk)
        except Exception as exc:  # pylint: disable=broad-except
            # lỗi SQL: outbox vẫn pending, worker sẽ xử lý
            errored_count += len(chunk)
            failed.append({
                "status": "Hệ thống lỗi khi claim/publish thông báo theo lô",
                "outbox_count": len(chunk),
                "error_type": type(exc).__name__,
                "error_message": str(exc),
            })
            continue

        # Cập nhật thống kê từ batch
        claimed_count += batch["claimed_count"]
        published_count += batch["published_count"]
        failed.extend(batch["failed"])

    # Tính số lượng outbox không claim được (đã được worker/process khác xử lý, hoặc chưa đến hạn)
    not_claimed = len(outbox_ids) - claimed_count - errored_count
    if not_claimed > 0:
        failed.append({"status": "not_claimed", "count": not_claimed})

    real_failures = [f for f in failed if f.get("status") != "not_claimed"]
    return {
        "success": not real_failures,
        "partially_published": published_count > 0 and bool(real_failures),
        "requested_count": len(outbox_ids),
        "published_count": published_count,
        "failed_count": len(real_failures),
        "failed": failed,
    }

async def wake_outbox_worker() -> None:
    """
    Báo Outbox Worker có việc mới để nó xử lý ngay (worker đang chờ bằng BLPOP trên key này).
    Chỉ là tín hiệu tối ưu độ trễ: gửi lỗi cũng không sao, worker vẫn tự quét theo chu kỳ poll.
    """
    try:
        # Dùng pipeline để giảm round-trip, không cần MULTI/EXEC.
        pipe = redis_client.pipeline(transaction=False)
        # Ghi giá trị "1" vào list Redis để đánh thức worker.
        pipe.lpush(OUTBOX_WAKEUP_KEY, "1")
        # Giữ tối đa 10 phần tử đầu tiên của List, từ vị trí 0 đến 9
        # TRánh trường hợp chứa vô hạn phần tử nếu worker không kịp xử lý.
        pipe.ltrim(OUTBOX_WAKEUP_KEY, 0, 9)
        await pipe.execute()
    except Exception:  # pylint: disable=broad-except
        pass
