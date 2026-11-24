"""
Schema định nghĩa các thông báo (notification) và sự kiện (event) trong hệ thống.
Một NotificationEvent hoàn chỉnh bao gồm các thành phần:
{
    "schema_version": "1.0",                # Phiên bản schema của thông báo
    "event_id": "evt_...",                  # ID duy nhất của sự kiện thông báo
    "notification_id": "noti_...",          # ID duy nhất của thông báo dùng để lưu trong cơ sở dữ liệu và liên kết các bảng khác
    "created_at": "2026-06-17T08:30:00Z",   # Thời điểm tạo thông báo

    "tenant_id": "company_a",               # ID của tenant (công ty) mà thông báo này thuộc về
    "notification_type": "role.created",    # Loại sự kiện thông báo, ví dụ role.created, user.deleted, order.shipped, ...
    "category": "security",                 # Nhóm thông báo, ví dụ security, order, system, ...

    "source": {},                           # Nguồn phát sinh thông báo, ví dụ service, instance, environment, process_id, ... được khai báo tại NotificationSource.
    "actor": {},                            # Đối tượng thực hiện hành động, ví dụ user, system, service, ... được khai báo tại NotificationActor.
    "resource": {},                         # Đối tượng bị tác động khi tạo thông báo, ví dụ role, user, order, ... được khai báo tại NotificationResource.
    "audience": {},                         # Đối tượng nhận thông báo, ví dụ user, users, role, roles, tenant, global, ... được khai báo tại NotificationAudience.
    "channels": [],                         # Danh sách các kênh gửi thông báo, ví dụ in_app, websocket, email, push, sms, ... được khai báo tại NotificationChannel.

    "content": {},                          # Nội dung thông báo, bao gồm template_key, template_version, locale, title, body, short_body, image_url, variables, ... được khai báo tại NotificationContent.
    "action": {},                           # Hành động diễn ra khi click vào thông báo này, ví dụ navigate, open_url, open_modal, download_file, call_api, ... được khai báo tại NotificationAction.
    "data": {},                             # Dữ liệu bổ sung, có thể chứa bất kỳ thông tin nào mà người gửi muốn đính kèm với thông báo.

    "priority": "normal",                   # Mức độ ưu tiên khi gửi thông báo, ví dụ low, normal, high, urgent, ... được khai báo tại NotificationPriority.
    "severity": "success",                  # Mức độ ưu tiên của nội dung thông báo, ví dụ info, success, warning, error, critical, ... được khai báo tại NotificationSeverity.
    "deduplication": {},                    # Thông tin giúp ngăn nhiều service hoặc nhiều lần retry tạo ra cùng một thông báo trong thời gian ngắn, được khai báo tại NotificationDeduplication.
    "delivery_policy": {},                  # Chính sách gửi thông báo, ví dụ số lần retry, thời gian sống của thông báo, ... được khai báo tại NotificationDeliveryPolicy.
    "trace": {},                            # Thông tin theo dõi request, ví dụ correlation_id, trace_id, causation_id, ... được khai báo tại NotificationTrace.

"status": "active",                         # Trạng thái vòng đời của thông báo, ví dụ active, cancelled, expired, ... được khai báo tại NotificationLifecycleStatus.
    "available_at": "2026-06-17T08:30:00Z", # Thời điểm thông báo có thể được gửi đi, nếu không có thì mặc định là thời điểm tạo thông báo.
    "expires_at": "2026-06-24T08:30:00Z"    # Thời điểm thông báo hết hạn, nếu không có thì mặc định là 7 ngày kể từ thời điểm tạo thông báo.
}
"""
from datetime import datetime, timezone
from enum import StrEnum
from typing import  Any, Literal
from pydantic import BaseModel, ConfigDict, Field, model_validator


# ============================================================
# ENUM
# ============================================================
class NotificationDispatchMode(StrEnum):
    """
    WORKER:
        Chỉ lưu notification + outbox.
        Worker sẽ tự động truy vấn các outbox có trạng thái "pending" và đẩy lên redis

    DIRECT:
        Lưu notification + outbox trước.
        Sau đó thử publish ngay.

        Nếu publish Redis lỗi:
        - Không throw lỗi ra RoleController.
        - Outbox được đưa lại pending.
        - Worker có thể retry sau.
    """
    DIRECT = "direct"
    WORKER = "worker"

class NotificationStreamScope(StrEnum):
    """
    Xác định event sẽ được ghi vào loại Redis Stream nào.
    """
    USER = "user"           # Stream riêng tới người dùng
    TENANT = "tenant"       # Stream riêng tới tổ chức, công ty
    JOB = "job"             # Stream riêng cho 1 công việc cụ thể
    GLOBAL = "global"       # Stream cho toàn bộ hệ thống (mục đích cho dashboard, ...)
class NotificationChannel(StrEnum):
    """
    Các kênh gửi noti như trong app, hay realtime qua websocket, gửi email, ...
    """
    IN_APP = "in_app"
    WEBSOCKET = "websocket"
    EMAIL = "email"
    PUSH = "push"
    SMS = "sms"

class NotificationPriority(StrEnum):
    """
    Mức độ ưu tiên thông báo
    """
    LOW = "low"
    NORMAL = "normal"
    HIGH = "high"
    URGENT = "urgent"

class NotificationSeverity(StrEnum):
    """
    Trạng thái thông báo
    """
    INFO = "info"
    SUCCESS = "success"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"

class NotificationAudienceType(StrEnum):
    """
    Đối tượng nhận thông báo, có thể là một user, nhiều users, một role, nhiều roles, một tenant hay toàn bộ hệ thống (global).
    """
    USER = "user"       # Một user duy nhất
    USERS = "users"     # Nhiều user, danh sách user_ids
    ROLE = "role"       # Những người thuộc role này
    ROLES = "roles"     # Những người thuộc các role này
    TENANT = "tenant"   # Tất cả user trong tenant
    GLOBAL = "global"   # Tất cả user trong toàn bộ hệ thống

class NotificationActorType(StrEnum):
    """
    Người thực hiện hành động
    """
    USER = "user"
    SYSTEM = "system"
    SERVICE = "service"

class NotificationActionType(StrEnum):
    """
    Hành động diễn ra khi click vào thông báo này
    """
    NONE = "none"                   # Không có hành động gì, chỉ hiển thị thông báo
    NAVIGATE = "navigate"           # Điều hướng tới một trang trong app, ví dụ /orders/1234
    OPEN_URL = "open_url"           # Mở một URL trong trình duyệt, ví dụ https://example.com/orders/1234
    OPEN_MODAL = "open_modal"       # Mở một modal, ví dụ để xem chi tiết đơn hàng
    DOWNLOAD_FILE = "download_file" # Tải xuống một tệp tin, ví dụ file PDF của đơn hàng
    CALL_API = "call_api"           # Gọi một API, ví dụ để cập nhật trạng thái đơn hàng

class NotificationRecipientState(StrEnum):
    """
    Trạng thái của thông báo khi đã tới người đọc
    """
    UNREAD = "unread"
    SEEN = "seen"
    READ = "read"
    ARCHIVED = "archived"
    DELETED = "deleted"

class NotificationDeliveryStatus(StrEnum):
    """
    Trạng thái của thông báo khi gửi tới người đọc
    """
    PENDING = "pending"         # Chưa gửi, đang chờ worker gửi
    QUEUED = "queued"           # Đã gửi vào Redis Stream, đang chờ Notification Server đọc và đẩy tới client
    SENDING = "sending"         # Đang gửi tới client, chưa nhận được ACK (Acknowledgement: là client đã nhận được thông báo)
    SENT = "sent"               # Đã gửi tới client, đã nhận được ACK
    DELIVERED = "delivered"     # Đã gửi tới client, đã nhận được ACK, và client đã hiển thị thông báo
    FAILED = "failed"           # Gửi tới client thất bại, có thể do client offline, hoặc lỗi mạng, hoặc lỗi server
    EXPIRED = "expired"         # Thông báo đã hết hạn, không còn giá trị, không gửi tới client nữa
    CANCELLED = "cancelled"     # Thông báo đã bị hủy, không còn giá trị, không gửi tới client nữa

class NotificationLifecycleStatus(StrEnum):
    """
    Trạng thái vòng đời của thông báo
    """
    ACTIVE = "active"
    CANCELLED = "cancelled"
    EXPIRED = "expired"

# ============================================================
# BASE SCHEMA
# ============================================================
class NotificationBaseSchema(BaseModel):
    """
    Không cho phép client hoặc service gửi thêm trường không khai báo.  
    extra="forbid" giúp phát hiện payload sai ngay từ đầu, thay vì âm thầm bỏ qua dữ liệu.
    """
    model_config = ConfigDict(
        extra="forbid",
        str_strip_whitespace=True,
        from_attributes=True,
    )

# ============================================================
# THÀNH PHẦN CỦA EVENT
# ============================================================
class NotificationSource(NotificationBaseSchema):
    """
    Nguồn phát sinh thông báo  
    Ví dụ:  
    ```
    "source": {
        "service": "order-service",
        "instance": "order-api-01",
        "environment": "production"
        }
    ```
    """
    service: str = Field( min_length=1, max_length=100, )
    instance: str | None = Field( default=None, max_length=100, )
    environment: str = Field(default="production",max_length=32,)

class NotificationActor(NotificationBaseSchema):
    """
    Đối tượng thực hiện hành động  
    ```
    "actor": {
        "type": "user",
        "id": "admin_01",
        "display_name": "Nguyễn Văn A"
    }
    ```
    """
    type: NotificationActorType
    id: str = Field( min_length=1, max_length=64, )
    display_name: str | None = Field( default=None, max_length=255, )
    avatar_url: str | None = Field(default=None,max_length=2048,)

class NotificationResource(NotificationBaseSchema):
    """
    Đối tượng bị tác động khi tạo thông báo  
    Ví dụ:  
    ```
    "resource": {
        "type": "role",
        "id": "warehouse_manager",
        "version": 1
    }
    ```
    """
    type: str = Field( min_length=1, max_length=64, )
    id: str = Field( min_length=1, max_length=128, )
    version: int | None = Field( default=None, ge=1, )

class NotificationAudience(NotificationBaseSchema):
    """
    Đối tượng nhận thông báo  
    Ví dụ:  
    ```
    "audience": {
        "type": "roles",
        "user_ids": [],
        "role_ids": ["system_admin"],
        "exclude_user_ids": ["admin_001"]
        }
    ```
    Nên loại người tạo thông báo ra, không cần gửi thông báo cho chính họ
    """
    type: NotificationAudienceType
    user_ids: list[str] = Field(default_factory=list)
    role_ids: list[str] = Field(default_factory=list)
    exclude_user_ids: list[str] = Field(default_factory=list)

    @model_validator(mode="after")
    def validate_audience(self) -> "NotificationAudience":
        """
        Kiểm tra tính hợp lệ của audience dựa trên type:
        - Nếu type là user hoặc users thì user_ids không được rỗng.
        - Nếu type là role hoặc roles thì role_ids không được rỗng.
        - Nếu type là group hoặc groups thì group_ids không được rỗng.
        """
        if self.type in {NotificationAudienceType.USER, NotificationAudienceType.USERS}:
            if not self.user_ids:
                raise ValueError("Không được rỗng user_ids khi đối tượng nhận thông báo là người dùng hoặc nhiều người dùng")

        if self.type in {NotificationAudienceType.ROLE, NotificationAudienceType.ROLES}:
            if not self.role_ids:
                raise ValueError("Danh sách role_ids không được rỗng khi đối tượng nhận thông báo là role hoặc nhiều role")

        return self

class NotificationContent(NotificationBaseSchema):
    """
    Nội dung thông báo  
    Ví dụ:  
    ```
    "content": {
        "template_key": "order.delivery_started",
        "template_version": "1",
        "locale": "vi-VN",
        "title": "Đơn hàng đang được giao",
        "body": "Đơn hàng #9988 của bạn đang trên đường vận chuyển.",
        "variables": {
            "order_id": "9988"
        }
    }
    ```
    variable dùng trong trường hợp cần biến để đưa vào template
    """
    template_key: str | None = Field( default=None, max_length=128, )
    template_version: str | None = Field( default=None, max_length=32, )
    locale: str = Field( default="vi-VN", max_length=20, )
    title: str = Field( min_length=1, max_length=255, )
    body: str = Field( min_length=1, max_length=5000, )
    short_body: str | None = Field( default=None, max_length=500, )
    image_url: str | None = Field( default=None, max_length=2048, )
    variables: dict[str, Any] = Field(default_factory=dict,)

class NotificationAction(NotificationBaseSchema):
    """
    Hành động cho thông báo này  
    Ví dụ:  
    ```
    "action": {
        "type": "navigate",
        "target": "/orders/9988",
        "label": "Xem chi tiết",
        "method": "GET",
        "open_in_new_tab": false
    }
    ```
    """
    type: NotificationActionType = NotificationActionType.NONE
    target: str | None = Field( default=None, max_length=2048, )
    label: str | None = Field( default=None, max_length=100, )
    method: Literal[ "GET", "POST", "PUT", "PATCH", "DELETE", ] = "GET"
    open_in_new_tab: bool = False

    @model_validator(mode="after")
    def validate_action(self) -> "NotificationAction":
        """
        Kiểm tra tính hợp lệ của action:
        - Nếu action.type khác NONE thì target là bắt buộc.
        - Nếu action.type là NONE thì target không được cung cấp.
        """
        if (self.type != NotificationActionType.NONE and not self.target):
            raise ValueError("Khi hành động cho thông báo khác NONE thì target là bắt buộc")

        if (self.type == NotificationActionType.NONE and self.target):
            raise ValueError("Khi hành động cho thông báo là NONE thì target không được cung cấp")

        return self

class NotificationDeduplication(NotificationBaseSchema):
    """
    Trường này giúp ngăn nhiều service hoặc nhiều lần retry tạo ra cùng một thông báo trong thời gian ngắn.  
    Ví dụ:  
    ```
    "deduplication": {
        "key": "order:9988:delivery_started",
        "window_seconds": 300
        }
    ```
    """
    key: str = Field( min_length=1, max_length=255, )               # Khóa duy nhất để xác định thông báo trùng lặp, ví dụ order:9988:delivery_started
    window_seconds: int = Field( default=300, ge=1, le=86400, )     # Thời gian (tính bằng giây) mà thông báo trùng lặp sẽ bị bỏ qua, ví dụ 300 giây = 5 phút

class NotificationDeliveryPolicy(NotificationBaseSchema):
    """
    Chính sách gửi thông báo, ví dụ số lần retry, thời gian sống của thông báo, ...  
    Ví dụ:  
    ```
    "delivery_policy": {
        "ttl_seconds": 604800,
        "max_attempts": 5,
        "retry_delay_seconds": 10,
        "require_ack": false
    }
    ```
    `ttl_seconds`: thời gian thông báo còn giá trị.  
    `max_attempts`: số lần gửi tối đa.  
    `retry_delay_seconds`: thời gian giữa các lần retry.  
    `require_ack`: client có phải xác nhận đã nhận hay không.  
    """
    ttl_seconds: int = Field( default=604800, ge=1, )
    max_attempts: int = Field(default=5,ge=1,le=20,)
    retry_delay_seconds: int = Field(default=10,ge=0,le=3600,)
    require_ack: bool = False

class NotificationTrace(NotificationBaseSchema):
    """
    Thông tin theo dõi request, ví dụ correlation_id, trace_id, causation_id, ...    
    Ví dụ:  
    ```
    "trace": {
        "correlation_id": "corr_...",
        "trace_id": "trace_...",
        "causation_id": "order_event_..."
    }
    ```
    """
    correlation_id: str | None = Field( default=None, max_length=128, )
    trace_id: str | None = Field( default=None, max_length=128, )
    causation_id: str | None = Field(default=None,max_length=128,)

# ============================================================
# COMMAND TẠO THÔNG BÁO
# ============================================================
class NotificationCreateCommand(NotificationBaseSchema):
    """
    Đây là nội dung mà client hoặc service gửi lên API để tạo thông báo.  
    """
    tenant_id: str = Field( min_length=1, max_length=64, )
    notification_type: str = Field( min_length=3, max_length=128, pattern=r"^[a-z][a-z0-9_]*(\.[a-z][a-z0-9_]*)+$", )      # Regex hiện tại yêu cầu ít nhất một dấu chấm: role.create
    category: str = Field( min_length=1, max_length=64, )                                                                  # Nhóm thông báo
    source: NotificationSource                                                                                             # Nguồn phát sing ra thông báo này
    actor: NotificationActor | None = None
    resource: NotificationResource | None = None
    audience: NotificationAudience
    channels: list[NotificationChannel] = Field( min_length=1, )
    content: NotificationContent
    action: NotificationAction = Field( default_factory=NotificationAction, )
    data: dict[str, Any] = Field( default_factory=dict, )
    priority: NotificationPriority = ( NotificationPriority.NORMAL )                                                      # Mức độ ưu tiên khi gửi
    severity: NotificationSeverity = ( NotificationSeverity.INFO )                                                        # Mức độ ueu tiên của nội dung
    deduplication: NotificationDeduplication | None = None                                                                # Ngắn tạo nội dung trùng
    delivery_policy: NotificationDeliveryPolicy = Field( default_factory=NotificationDeliveryPolicy, )                    # Chính sách gửi
    trace: NotificationTrace = Field( default_factory=NotificationTrace, )                                                # Theo dõi requesst
    status: NotificationLifecycleStatus = ( NotificationLifecycleStatus.ACTIVE )
    available_at: datetime | None = None
    expires_at: datetime | None = None

    # Validator chạy sau khi Pydantic đã đọc và chuyển đổi các trường
    @model_validator(mode="after")
    def validate_times(self) -> "NotificationCreateCommand":
        """
        Kiểm tra tính hợp lệ của `available_at` và `expires_at`:
        - `expires_at` phải lớn hơn thời điểm hiện tại.
        - `available_at` phải nhỏ hơn `expires_at``.
        """
        now = datetime.now(timezone.utc)
        # Không cho phép tạo thông báo đã hết hạn.
        if self.expires_at and self.expires_at <= now:
            raise ValueError("Thời gian hết hạn thông báo phải lớn hơn thời điểm hiện tại")

        # Thông báo phải được phép gửi trước khi hết hạn.
        if (self.available_at and self.expires_at and self.available_at >= self.expires_at):
            raise ValueError("Thời gian thông báo có thể gửi phải nhỏ hơn thời gian hết hạn")   

        return self

# ============================================================
# EVENT HOÀN CHỈNH
# ============================================================
class NotificationEvent(NotificationCreateCommand):
    """
    Đây là nội dung đầy đủ được lưu và publish Redis.  
    NotificationEvent kế thừa NotificationCreateCommand  
    Bổ sung thêm các thông tin:
    - định danh
    - phiên bản
    - mốc thời gian

    Ví dụ một thông báo hoàn chỉnh như sau:  
    ```
    {
        "schema_version": "1.0",
        "event_id": "evt_66849c09d1df4567a6f1d18aa0e1c602",
        "notification_id": "noti_50a614d6b4714e58bc7582f45d43fc70",

        "tenant_id": "company_a",
        "notification_type": "role.created",
        "category": "security",

        "source": {
            "service": "api-server",
            "instance": "role-controller",
            "environment": "production"
        },

        "actor": {
            "type": "user",
            "id": "admin_001",
            "display_name": "superadmin",
            "avatar_url": null
        },

        "resource": {
            "type": "role",
            "id": "warehouse_manager",
            "version": 1
        },

        "audience": {
            "type": "users",
            "user_ids": [
                "admin_001",
                "admin_002",
                "admin_003"
            ],
            "role_ids": [],
            "exclude_user_ids": [
                "admin_001"
            ]
        },

        "channels": [
            "in_app",
            "websocket"
        ],

        "content": {
            "template_key": "role.created",
            "template_version": "1",
            "locale": "vi-VN",
            "title": "Role mới đã được tạo",
            "body": "Role 'Warehouse Manager' đã được tạo bởi superadmin.",
            "short_body": "Role 'Warehouse Manager' vừa được tạo.",
            "image_url": null,
            "variables": {
                "role_id": "warehouse_manager",
                "role_name": "Warehouse Manager",
                "actor_user_id": "admin_001",
                "actor_username": "superadmin"
            }
        },

        "action": {
            "type": "navigate",
            "target": "/roles/warehouse_manager",
            "label": "Xem chi tiết",
            "method": "GET",
            "open_in_new_tab": false
        },

        "data": {
            "role_id": "warehouse_manager",
            "role_name": "Warehouse Manager",
            "permissions": [
                "warehouse:read",
                "warehouse:update",
                "job:read"
            ]
        },

        "priority": "normal",
        "severity": "success",

        "deduplication": {
            "key": "role:company_a:warehouse_manager:created",
            "window_seconds": 300
        },

        "delivery_policy": {
            "ttl_seconds": 604800,
            "max_attempts": 5,
            "retry_delay_seconds": 10,
            "require_ack": false
        },

        "trace": {
            "correlation_id": "corr_f84c21",
            "trace_id": "trace_87a19d",
            "causation_id": "role_create:warehouse_manager"
        },

        "status": "active",
        "available_at": "2026-06-17T08:30:00Z",
        "expires_at": "2026-06-24T08:30:00Z",
        "created_at": "2026-06-17T08:30:00Z"
    }
    ```
    """
    schema_version: str = Field( default="1.0", max_length=20, )
    event_id: str = Field( min_length=1, max_length=64, )
    notification_id: str = Field( min_length=1, max_length=64, )
    created_at: datetime
    available_at: datetime

# ============================================================
# KẾT QUẢ TẠO THÔNG BÁO
# ============================================================
class NotificationCreateResult(NotificationBaseSchema):
    """
    Là kết quả sau khi lưu thông báo trong hệ thống.  
    Đây là nội dung trả về sau khi tạo thông báo.  
    Ví dụ: 
    ```
    {
        "notification_id": "noti_123",
        "event_id": "evt_123",
        "outbox_ids": [
            "outbox_user_admin002",
            "outbox_user_admin003",
            "outbox_tenant",
            "outbox_global"
        ],
        "recipient_count": 2,
        "delivery_count": 4,
        "stream_target_count": 4,
        "created_at": "2026-06-17T10:30:00Z"
    }
    ```
    """
    notification_id: str
    event_id: str
    outbox_ids: list[str]
    recipient_count: int
    delivery_count: int
    stream_target_count: int
    created_at: datetime

class NotificationSendRequest(BaseModel):
    """
    Body của API POST /notifications/send.
    """
    title: str = Field(min_length=1, max_length=255, examples=["Xin chào"])
    body: str = Field(min_length=1, max_length=5000, examples=["Đây là thông báo thử nghiệm"])

    # Danh sách user_id nhận thông báo (cùng tenant). Rỗng = gửi cho chính mình.
    target_user_ids: list[str] = Field(default_factory=list, max_length=20000)

    # True = gửi cho MỌI tài khoản đang hoạt động của tenant (bỏ qua target_user_ids).
    send_to_all_users: bool = False
    # Gửi thêm vào stream dashboard của tenant / stream global / stream của 1 job.
    send_to_tenant_dashboard: bool = False
    send_to_global: bool = False
    job_id: str | None = Field(default=None, max_length=128)

    severity: NotificationSeverity = NotificationSeverity.INFO
    # Phải có dạng "a.b" (vd system.message, job.progress).
    notification_type: str = Field(
        default="system.message",
        pattern=r"^[a-z][a-z0-9_]*(\.[a-z][a-z0-9_]*)+$",
    )
    data: dict[str, Any] = Field(default_factory=dict)

    # direct: publish Redis ngay; worker: chỉ ghi outbox, Outbox Worker sẽ publish.
    mode: NotificationDispatchMode = NotificationDispatchMode.DIRECT
