from __future__ import annotations

import asyncio
from dataclasses import dataclass
from enum import StrEnum
from typing import Any, Iterable
import json
import socket
from uuid import uuid4
from pydantic import BaseModel

from db.database import AsyncSessionLocal
from config.config import settings
from services.redis_client import get_redis_fast
from utils.events import event_to_redis_fields
from utils.redis_keys import (
    global_stream_key,
    job_stream_key,
    tenant_stream_key,
    user_stream_key,
)
from db.query_notification_outbox import (
    claim_outbox_by_id,
    mark_outbox_published,
    mark_outbox_retry,
    mark_user_websocket_delivery_failed,
    mark_user_websocket_delivery_sent,
)

# XADD là lệnh nhanh, không phải lệnh blocking.
# Không dùng get_redis_stream() vì client đó chủ yếu dành cho XREAD BLOCK.
redis_client = get_redis_fast()


# Số event tối đa được giữ gần đúng trong mỗi Redis Stream.
STREAM_MAX_LENGTH = settings.stream_max_length


# ============================================================
# PHẠM VI REDIS STREAM
# ============================================================

class NotificationStreamScope(StrEnum):
    """
    Xác định event sẽ được ghi vào loại Redis Stream nào.
    """

    USER = "user"           # Stream riêng tới ngươi dùng
    TENANT = "tenant"       # Stream riêng tới tổ chức, công ty
    JOB = "job"             # Stream riêng cho 1 công việc cụ thể
    GLOBAL = "global"       # Stream cho toàn bộ hệ thống (mục đích cho dashbord, ...)

# ============================================================
# MÔ TẢ MỘT ĐỐI TƯỢNG CẦN PUBLISH
# ============================================================

@dataclass(frozen=True, slots=True)
class NotificationStreamTarget:
    """
    Tạo 1 class định nghĩa một đối tượng publish  
    - frozen sẽ ngăn cấm người dùng sửa dữ liệu sau khi khai báo  
    Ví dụ sau sẽ báo lỗi:  
    ```
    target = NotificationStreamTarget.user(
        tenant_id="company_a",
        user_id="user_01",
    )

    target.user_id = "user_02"
    ```
    - slots sẽ ngăn người dùng tạo các thuộc tính mới ngoài các thuộc tính đã định nghĩa  
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
                raise ValueError(
                    "Mã tổ chức là bắt buộc đối với từng người dùng nhận thông báo"
                )

            if not self.user_id :
                raise ValueError(
                    "Mã người dùng không được cung cấp khi gửi thông báo"
                )

            # Nếu cung cấp đủ thông tin thì tạo key để gửi vào redis stream
            return user_stream_key(self.tenant_id, self.user_id,)

        if self.scope == NotificationStreamScope.TENANT:
            if not self.tenant_id or not self.tenant_id.strip():
                raise ValueError(
                    "Mã tổ chức là bắt buộc đối với tenant stream"
                )

            return tenant_stream_key(self.tenant_id,)

        if self.scope == NotificationStreamScope.JOB:
            if not self.tenant_id or not self.tenant_id.strip():
                raise ValueError(
                    "Mã người dùng là bắt buộc đối với job stream"
                )

            if not self.job_id:
                raise ValueError(
                    "MÃ công việc là bắt buộc đối với job stream"
                )

            return job_stream_key( self.tenant_id, self.job_id )

        if self.scope == NotificationStreamScope.GLOBAL:
            return global_stream_key()

        raise ValueError(
            f"Không hỗ trợ stream scope: {self.scope}, vui lòng thử lại"
        )

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

def normalize_event(
    event: dict[str, Any] | BaseModel,
) -> dict[str, Any]:
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
    # Đầu tiên nếu là BaseModel thì chuyển nó sang dạng phù hợp với json, như chuyển các kiểu datetime sang chuỗi
    if isinstance(event, BaseModel):
        return event.model_dump(mode="json")
    # Nếu là dict thì tạo 1 bản copy của dict (dict lồng dict) để trả về
    if isinstance(event, dict):
        return dict(event)

    raise TypeError(
        "event phải là dict hoặc Pydantic BaseModel"
    )

# ============================================================
# LOẠI BỎ TARGET TRÙNG
# ============================================================

def deduplicate_targets(
    targets: Iterable[NotificationStreamTarget],
) -> list[NotificationStreamTarget]:
    """
    Loại bỏ các target có cùng Redis Stream key.

    Ví dụ admin_user_ids bị trùng thì event vẫn chỉ được
    ghi một lần vào user stream tương ứng.
    """
    # Tạo dict chứa các target
    unique_targets: dict[
        str,
        NotificationStreamTarget,
    ] = {}

    for target in targets:
        stream_key = target.get_stream_key()
        # Tạo 1 dict có dạng: stream_key: target
        # "stream:noti:tenant:company_a:user:user_01": user_target
        unique_targets[stream_key] = target
    # Chuyển thành list
    return list(unique_targets.values())


# ============================================================
# HELPER TẠO TARGET (XÁC ĐỊNH NOTI NÀY ĐƯỢC GỬI TỚI CÁC KEY REDIS STREAM NÀO)
# ============================================================

def build_stream_targets(
    *,
    tenant_id: str,
    admin_user_ids: list[str],
    target_user_id: str | None,

    # Admin theo dõi qua tenant dashboard stream.
    send_to_admin_dashboard: bool = True,

    # Superuser/monitoring theo dõi qua global stream.
    send_to_global: bool = True,

    # Gửi tới user stream cá nhân của target user.
    send_to_target_user: bool = True,

    # Nếu True, mỗi admin còn nhận trên user stream cá nhân.
    #
    # Nếu False, admin chỉ nhận realtime khi đang mở
    # tenant dashboard WebSocket.
    send_to_admin_personal_streams: bool = False,

    # Có thể loại người thực hiện khỏi user stream cá nhân.
    actor_user_id: str | None = None,
) -> list[NotificationStreamTarget]:
    """
    Xây dựng danh sách Redis Stream target cho role.updated.

    Trường hợp mặc định tạo ba nhóm target:

    1. Tenant dashboard stream:
       stream:noti:tenant:{tenant_id}:dashboard

    2. Global stream:
       stream:noti:global

    3. User stream:
       stream:noti:tenant:{tenant_id}:user:{target_user_id}

    Nếu send_to_admin_personal_streams=True:
       tạo thêm user stream cho từng admin.
    """
    # Tạo list rỗng để chứa danh sách những người nhận thông báo
    targets: list[NotificationStreamTarget] = []

    # --------------------------------------------------------
    # 1. ADMIN DASHBOARD STREAM
    # --------------------------------------------------------
    # Các admin đang kết nối endpoint tenant dashboard sẽ nhận event này.
    if send_to_admin_dashboard:
        targets.append(
            NotificationStreamTarget.tenant(
                tenant_id=tenant_id,
            )
        )

    # --------------------------------------------------------
    # 2. GLOBAL STREAM
    # --------------------------------------------------------
    # Superuser hoặc màn hình monitoring toàn hệ thống nhận event.
    if send_to_global:
        targets.append(
            NotificationStreamTarget.global_scope()
        )

    # --------------------------------------------------------
    # 3. USER STREAM CỦA MỘT USER CỤ THỂ
    # --------------------------------------------------------
    if (
        send_to_target_user
        and target_user_id
        and target_user_id.strip()
    ):
        targets.append(
            NotificationStreamTarget.user(
                tenant_id=tenant_id,
                user_id=target_user_id.strip(),
            )
        )

    # --------------------------------------------------------
    # 4. USER STREAM CÁ NHÂN CỦA ADMIN
    # --------------------------------------------------------
    #
    # Phần này là tùy chọn.
    #
    # Nếu admin chỉ cần xem tenant dashboard thì không cần bật.
    #
    # Nếu admin cần nhận notification trên biểu tượng chuông cá nhân,
    # nên bật để publish vào user stream của từng admin.
    if send_to_admin_personal_streams:
        for admin_user_id in admin_user_ids:
            admin_user_id = str(admin_user_id).strip()

            if not admin_user_id:
                continue

            # Không gửi lại user stream cho actor nếu không cần.
            if (
                actor_user_id
                and admin_user_id == actor_user_id.strip()
            ):
                continue

            targets.append(
                NotificationStreamTarget.user(
                    tenant_id=tenant_id,
                    user_id=admin_user_id,
                )
            )

    # Loại stream target trùng.
    #
    # Ví dụ target_user_id đồng thời là một admin.
    # Khi đó user stream chỉ được tạo một lần.
    return deduplicate_targets(targets)

# ============================================================
# PUBLISH MỘT EVENT VÀO MỘT KEY STREAM
# ============================================================

async def publish_event_to_stream(
    *,
    target: NotificationStreamTarget,
    event: dict[str, Any] | BaseModel,
) -> str:
    """
    Ghi một event vào đúng một Redis Stream.

    Redis trả về stream_id dạng:

        1781685000123-0

    stream_id này được dùng để:
    - trả về WebSocket client;
    - client lưu làm last_event_id;
    - replay event khi client reconnect.
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
    return str(stream_id)

# ============================================================
# PUBLISH MỘT NOTIFICATION
# ============================================================

async def publish_notification_event(
    *,
    event: dict[str, Any] | BaseModel,
    targets: Iterable[NotificationStreamTarget],
    max_concurrency: int = 20,
) -> dict[str, Any]:
    """
    Publish cùng một notification event vào nhiều Redis Stream.

    Hàm không dừng toàn bộ chỉ vì một stream bị lỗi.

    Ví dụ:
    - user stream thành công;
    - tenant stream thành công;
    - global stream lỗi;

    kết quả vẫn ghi rõ stream nào thành công và stream nào thất bại.

    Điều này giúp:
    - kiểm soát từng kênh;
    - retry riêng từng stream;
    - tránh mất toàn bộ event chỉ vì một target lỗi.
    """

    resolved_targets = deduplicate_targets(targets)

    if not resolved_targets:
        raise ValueError(
            "Phải có ít nhất một Redis Stream target"
        )

    semaphore = asyncio.Semaphore(max_concurrency)

    async def publish_one(
        target: NotificationStreamTarget,
    ) -> dict[str, Any]:
        """
        Publish một target và giữ exception bên trong kết quả.
        """

        async with semaphore:
            stream_key = target.get_stream_key()

            try:
                stream_id = await publish_event_to_stream(
                    target=target,
                    event=event,
                )

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

            except Exception as exc:
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

    target_results = await asyncio.gather(
        *[
            publish_one(target)
            for target in resolved_targets
        ]
    )

    published = [
        item
        for item in target_results
        if item["success"]
    ]

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

async def publish_claimed_outbox_to_redis(
    *,
    outbox: dict[str, Any],
) -> str:
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

    payload_json = outbox["payload_json"]

    try:
        payload = json.loads(payload_json)

    except json.JSONDecodeError as exc:
        raise ValueError(
            "payload_json của Outbox không phải JSON hợp lệ"
        ) from exc

    if not isinstance(payload, dict):
        raise ValueError(
            "payload_json của Outbox phải là JSON object"
        )

    # Hàm hiện có của bạn chuyển object lồng nhau
    # thành Redis field-value dạng string.
    fields = event_to_redis_fields(payload)

    # Thêm metadata của Outbox để debug.
    fields["outbox_id"] = str(outbox["outbox_id"])
    fields["event_id"] = str(outbox["event_id"])
    fields["notification_id"] = str(
        outbox["notification_id"]
    )
    fields["event_type"] = str(outbox["event_type"])
    fields["stream_scope"] = str(
        outbox["stream_scope"]
    )

    # Thêm thông tin target nếu có.
    if outbox.get("target_tenant_id"):
        fields["target_tenant_id"] = str(
            outbox["target_tenant_id"]
        )

    if outbox.get("target_user_id"):
        fields["target_user_id"] = str(
            outbox["target_user_id"]
        )

    if outbox.get("target_job_id"):
        fields["target_job_id"] = str(
            outbox["target_job_id"]
        )

    # XADD là lệnh nhanh, không dùng client dành riêng
    # cho XREAD BLOCK.
    redis_client = get_redis_fast()

    stream_id = await redis_client.xadd(
        str(outbox["stream_key"]),
        fields,

        # Giới hạn độ dài gần đúng của Redis Stream.
        maxlen=STREAM_MAX_LENGTH,
        approximate=True,
    )

    return str(stream_id)


def create_direct_publisher_id() -> str:
    """
    Tạo ID định danh cho Direct Publisher.

    Ví dụ:

        direct:api-container-01:a8c19f0b

    ID này được ghi vào notification_outbox.locked_by.

    Mục đích:
    - biết process nào đang xử lý;
    - chỉ process đó được cập nhật kết quả;
    - hỗ trợ debug Outbox bị kẹt.
    """

    hostname = socket.gethostname()

    return (
        f"direct:{hostname}:"
        f"{uuid4().hex[:8]}"
    )

async def publish_one_outbox_immediately(
    *,
    outbox_id: str,
) -> dict[str, Any]:
    """
    Claim và publish ngay một Outbox vào Redis Stream.

    Hàm này được sử dụng trong chế độ DIRECT:

        1. Notification đã được commit vào SQL Server.
        2. Outbox đã tồn tại với status=pending.
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

    worker_id = create_direct_publisher_id()

    try:
        # ====================================================
        # MỞ SESSION RIÊNG CHO QUÁ TRÌNH PUBLISH
        # ====================================================
        #
        # Session này không liên quan session tạo/cập nhật role.
        #
        # Vì vậy mọi lỗi trong hàm không rollback role.
        async with AsyncSessionLocal() as db:

            # =================================================
            # BƯỚC 1: CLAIM OUTBOX
            # =================================================
            #
            # Transaction chỉ dùng để:
            # pending → processing
            #
            # Sau khi thoát khỏi db.begin(), trạng thái processing
            # được commit trước khi gọi Redis.
            async with db.begin():
                claimed_outbox = await claim_outbox_by_id(
                    db,
                    outbox_id=outbox_id,
                    worker_id=worker_id,
                )

            # Nếu claim không thành công, không được publish.
            #
            # Các nguyên nhân:
            # - Worker khác đã claim.
            # - Outbox đã published.
            # - Outbox đang processing.
            # - Outbox failed.
            # - Chưa đến available_at.
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
            #
            # Đặc biệt không đặt XADD bên trong SQL transaction.
            #
            # Nếu giữ transaction trong khi gọi mạng:
            # - SQL connection bị chiếm lâu;
            # - giữ lock lâu;
            # - tăng nguy cơ deadlock;
            # - Redis chậm sẽ làm transaction SQL chậm theo.
            try:
                stream_id = (
                    await publish_claimed_outbox_to_redis(
                        outbox=claimed_outbox,
                    )
                )

            except Exception as publish_exc:
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
                        attempt_count=(
                            claimed_outbox["attempt_count"]
                        ),
                        max_attempts=(
                            claimed_outbox["max_attempts"]
                        ),
                    )

                    # Chỉ user stream ánh xạ trực tiếp tới
                    # một recipient cụ thể.
                    #
                    # Tenant/global stream là broadcast route,
                    # không nên cập nhật tất cả recipient ở đây.
                    if (
                        claimed_outbox["stream_scope"] == "user"
                        and claimed_outbox.get(
                            "target_user_id"
                        )
                    ):
                        await mark_user_websocket_delivery_failed(
                            db,
                            notification_id=(
                                claimed_outbox[
                                    "notification_id"
                                ]
                            ),
                            recipient_user_id=(
                                claimed_outbox[
                                    "target_user_id"
                                ]
                            ),
                            error_code=(
                                type(publish_exc).__name__
                            ),
                            error_message=str(publish_exc),
                            next_retry_at=(
                                retry_result["available_at"]
                            ),
                        )

                return {
                    "status": "publish_failed",
                    "outbox_id": outbox_id,
                    "worker_id": worker_id,

                    "stream_scope": (
                        claimed_outbox["stream_scope"]
                    ),
                    "stream_key": (
                        claimed_outbox["stream_key"]
                    ),

                    # pending hoặc failed.
                    "outbox_status": (
                        retry_result["status"]
                    ),

                    "next_retry_at": (
                        retry_result["available_at"].isoformat()
                    ),

                    "error_type": (
                        type(publish_exc).__name__
                    ),
                    "error_message": str(publish_exc),
                }

            # =================================================
            # BƯỚC 3: REDIS THÀNH CÔNG, CẬP NHẬT DATABASE
            # =================================================
            try:
                async with db.begin():
                    outbox_updated = (
                        await mark_outbox_published(
                            db,
                            outbox_id=outbox_id,
                            worker_id=worker_id,
                            stream_id=stream_id,
                        )
                    )

                    # Với user stream, có thể xác định chính xác
                    # WebSocket delivery của user nào.
                    if (
                        claimed_outbox["stream_scope"] == "user"
                        and claimed_outbox.get(
                            "target_user_id"
                        )
                    ):
                        await mark_user_websocket_delivery_sent(
                            db,
                            notification_id=(
                                claimed_outbox[
                                    "notification_id"
                                ]
                            ),
                            recipient_user_id=(
                                claimed_outbox[
                                    "target_user_id"
                                ]
                            ),
                            stream_id=stream_id,
                        )

            except Exception as state_update_exc:
                # Redis đã nhận event nhưng SQL Server chưa cập nhật
                # trạng thái published.
                #
                # Khi worker recovery xử lý lại, event có khả năng
                # được XADD thêm lần nữa.
                #
                # Vì vậy client cần chống trùng bằng event_id.
                return {
                    "status": "published_state_update_failed",
                    "outbox_id": outbox_id,
                    "worker_id": worker_id,

                    "stream_scope": (
                        claimed_outbox["stream_scope"]
                    ),
                    "stream_key": (
                        claimed_outbox["stream_key"]
                    ),
                    "stream_id": stream_id,

                    "error_type": (
                        type(state_update_exc).__name__
                    ),
                    "error_message": (
                        str(state_update_exc)
                    ),

                    "message": (
                        "Redis đã nhận event nhưng SQL Server "
                        "không cập nhật được trạng thái Outbox"
                    ),
                }

            if not outbox_updated:
                return {
                    "status": "published_state_not_updated",
                    "outbox_id": outbox_id,
                    "worker_id": worker_id,

                    "stream_scope": (
                        claimed_outbox["stream_scope"]
                    ),
                    "stream_key": (
                        claimed_outbox["stream_key"]
                    ),
                    "stream_id": stream_id,

                    "message": (
                        "Redis đã nhận event nhưng bản ghi Outbox "
                        "không còn thuộc quyền xử lý của process này"
                    ),
                }

            # =================================================
            # BƯỚC 4: HOÀN THÀNH
            # =================================================

            return {
                "status": "published",

                "outbox_id": outbox_id,
                "worker_id": worker_id,

                "event_id": claimed_outbox["event_id"],
                "notification_id": (
                    claimed_outbox["notification_id"]
                ),

                "stream_scope": (
                    claimed_outbox["stream_scope"]
                ),
                "stream_key": (
                    claimed_outbox["stream_key"]
                ),

                "target_tenant_id": (
                    claimed_outbox.get(
                        "target_tenant_id"
                    )
                ),
                "target_user_id": (
                    claimed_outbox.get(
                        "target_user_id"
                    )
                ),
                "target_job_id": (
                    claimed_outbox.get(
                        "target_job_id"
                    )
                ),

                # Redis XADD ID.
                "stream_id": stream_id,
            }

    except Exception as exc:
        # Lỗi ở mức session, claim hoặc SQL Server.
        #
        # Không raise lại để không ảnh hưởng nghiệp vụ role.
        return {
            "status": "internal_error",
            "outbox_id": outbox_id,
            "worker_id": worker_id,
            "error_type": type(exc).__name__,
            "error_message": str(exc),
        }
    
async def publish_multiple_outboxes_immediately(
    *,
    outbox_ids: list[str],
    max_concurrency: int = 10,
) -> dict[str, Any]:
    """
    Publish nhiều Outbox sau khi notification đã commit.

    Mỗi Outbox tương ứng một Redis Stream target.

    Ví dụ:
    - tenant dashboard;
    - global;
    - một user stream.

    Hàm xử lý song song nhưng giới hạn concurrency để tránh
    tạo quá nhiều kết nối/lệnh Redis cùng lúc.
    """
    # Tạo 1 semaphore để gọi đa luồng trong giới hạn 
    semaphore = asyncio.Semaphore(max_concurrency)

    async def publish_one(
        outbox_id: str,
    ) -> dict[str, Any]:
        async with semaphore:
            try:
                # Hàm này phải claim Outbox atomically:
                #
                # pending → processing
                #
                # Nhờ đó direct publisher và worker
                # không cùng publish một Outbox.
                return await publish_one_outbox_immediately(
                    outbox_id=outbox_id,
                )

            except Exception as exc:
                # Giữ lỗi riêng từng Outbox.
                # Không để một stream lỗi làm dừng các stream khác.
                return {
                    "status": "publish_failed",
                    "outbox_id": outbox_id,
                    "error_type": type(exc).__name__,
                    "error_message": str(exc),
                }

    results = await asyncio.gather(
        *[
            publish_one(outbox_id)
            for outbox_id in outbox_ids
        ]
    )

    published = [
        item
        for item in results
        if item.get("status") == "published"
    ]

    failed = [
        item
        for item in results
        if item.get("status") != "published"
    ]

    return {
        "success": len(failed) == 0,
        "partially_published": (
            len(published) > 0
            and len(failed) > 0
        ),
        "published_count": len(published),
        "failed_count": len(failed),
        "published": published,
        "failed": failed,
    }