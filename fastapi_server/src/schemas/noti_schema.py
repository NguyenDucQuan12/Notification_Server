from datetime import datetime, timezone
from typing import  Any, Literal
from pydantic import BaseModel, ConfigDict, Field, model_validator
from enum import StrEnum


# ============================================================
# ENUM
# ============================================================

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
    Mức độ nhận thông báo  
    chỉ một user nhất định hay nhiều users, ...  
    """
    USER = "user"
    USERS = "users"
    ROLE = "role"
    ROLES = "roles"
    TENANT = "tenant"
    GLOBAL = "global"


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
    NONE = "none"
    NAVIGATE = "navigate"
    OPEN_URL = "open_url"
    OPEN_MODAL = "open_modal"
    DOWNLOAD_FILE = "download_file"
    CALL_API = "call_api"


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
    PENDING = "pending"
    QUEUED = "queued"
    SENDING = "sending"
    SENT = "sent"
    DELIVERED = "delivered"
    FAILED = "failed"
    EXPIRED = "expired"
    CANCELLED = "cancelled"


class NotificationLifecycleStatus(StrEnum):
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
        if self.type in {
            NotificationAudienceType.USER,
            NotificationAudienceType.USERS,
        }:
            if not self.user_ids:
                raise ValueError(
                    "user_ids là bắt buộc khi audience.type là user hoặc users"
                )

        if self.type in {
            NotificationAudienceType.ROLE,
            NotificationAudienceType.ROLES,
        }:
            if not self.role_ids:
                raise ValueError(
                    "role_ids là bắt buộc khi audience.type là role hoặc roles"
                )

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
        if (
            self.type != NotificationActionType.NONE
            and not self.target
        ):
            raise ValueError(
                "target là bắt buộc khi action.type không phải none"
            )

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
    key: str = Field( min_length=1, max_length=255, )
    window_seconds: int = Field( default=300, ge=1, le=86400, )


class NotificationDeliveryPolicy(NotificationBaseSchema):
    """
    Chính sách  
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
    Tác vụ nào dẫn đến event này  
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
    tenant_id: str = Field( min_length=1, max_length=64, )
    # Ví dụ role.created.
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
        now = datetime.now(timezone.utc)
        # Không cho phép tạo thông báo đã hết hạn.
        if self.expires_at and self.expires_at <= now:
            raise ValueError(
                "expires_at phải lớn hơn thời điểm hiện tại"
            )

        # Thông báo phải được phép gửi trước khi hết hạn.
        if (
            self.available_at
            and self.expires_at
            and self.available_at >= self.expires_at
        ):
            raise ValueError(
                "available_at phải nhỏ hơn expires_at"
            )

        return self


# ============================================================
# EVENT HOÀN CHỈNH 
# ============================================================

class NotificationEvent(NotificationCreateCommand):
    """
    Đây là nội dung đầy đủ được lưu và publish Redis.  
    NotificationEvent kế thừa NotificationCreateCommand  
    Bổ sung thêm các thông tin định danh, phiên bản, và mốc thời gian để sẵn sàng phát (publish) vào các hệ thống như Redis  
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
    # Trong event hoàn chỉnh, available_at luôn có giá trị.
    available_at: datetime


# ============================================================
# KẾT QUẢ TẠO THÔNG BÁO
# ============================================================

class NotificationCreateResult(NotificationBaseSchema):
    """
    Là kết quả sau khi lưu thông báo  
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


# ============================================================
# SCHEMA CẬP NHẬT TRẠNG THÁI
# ============================================================

# class NotificationRecipientStateUpdate(
#     NotificationBaseSchema
# ):
#     state: NotificationRecipientState


# class NotificationDeliveryUpdate(NotificationBaseSchema):
#     status: NotificationDeliveryStatus
#     provider: str | None = None
#     provider_message_id: str | None = None
#     error_code: str | None = None
#     error_message: str | None = None
