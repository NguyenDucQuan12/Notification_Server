from __future__ import annotations
from datetime import timedelta
from typing import Any

from schemas.noti_schema import (
    NotificationAction,
    NotificationActionType,
    NotificationActor,
    NotificationActorType,
    NotificationAudience,
    NotificationAudienceType,
    NotificationChannel,
    NotificationContent,
    NotificationCreateCommand,
    NotificationDeduplication,
    NotificationDeliveryPolicy,
    NotificationEvent,
    NotificationPriority,
    NotificationResource,
    NotificationSeverity,
    NotificationSource,
    NotificationTrace
)
from utils.utils import utc_now, create_public_id, parse_permissions


# ============================================================
# HELPER
# ============================================================

def build_role_changes(
    *,
    before_role: dict[str, Any],
    after_role: dict[str, Any],
) -> dict[str, Any]:
    """
    So sánh dữ liệu role trước và sau cập nhật.

    Kết quả ví dụ:

        {
            "role_name": {
                "old": "Staff",
                "new": "Warehouse Staff"
            },
            "description": {
                "old": "Nhân viên",
                "new": "Nhân viên kho"
            },
            "permissions": {
                "added": ["warehouse:update"],
                "removed": ["job:delete"]
            }
        }
    """

    changes: dict[str, Any] = {}

    # --------------------------------------------------------
    # So sánh role_name
    # --------------------------------------------------------
    old_role_name = before_role.get("role_name")
    new_role_name = after_role.get("role_name")

    if old_role_name != new_role_name:
        changes["role_name"] = {
            "old": old_role_name,
            "new": new_role_name,
        }

    # --------------------------------------------------------
    # So sánh description
    # --------------------------------------------------------
    old_description = before_role.get("description")
    new_description = after_role.get("description")

    if old_description != new_description:
        changes["description"] = {
            "old": old_description,
            "new": new_description,
        }

    # --------------------------------------------------------
    # So sánh is_active
    # --------------------------------------------------------
    old_is_active = before_role.get("is_active")
    new_is_active = after_role.get("is_active")

    if old_is_active != new_is_active:
        changes["is_active"] = {
            "old": old_is_active,
            "new": new_is_active,
        }

    # --------------------------------------------------------
    # So sánh permission
    # --------------------------------------------------------
    old_permissions = set(
        parse_permissions(
            before_role.get("permissions_json")
        )
    )

    new_permissions = set(
        parse_permissions(
            after_role.get("permissions_json")
        )
    )

    added_permissions = sorted(
        new_permissions - old_permissions
    )

    removed_permissions = sorted(
        old_permissions - new_permissions
    )

    if added_permissions or removed_permissions:
        changes["permissions"] = {
            "added": added_permissions,
            "removed": removed_permissions,
        }

    return changes

# ============================================================
# TẠO EVENT CHUNG
# ============================================================

def build_notification_event(
    command: NotificationCreateCommand,
    *,
    notification_id: str | None = None,
    event_id: str | None = None,
) -> NotificationEvent:
    """
    Chuyển NotificationCreateCommand thành NotificationEvent hoàn chỉnh.

    Hàm này:
    - sinh notification_id
    - sinh event_id
    - đặt created_at
    - tính available_at
    - tính expires_at theo ttl_seconds nếu chưa truyền
    """
    # Lấy thời gian hiện tại
    now = utc_now()

    # Tạo id cho thông báo
    resolved_notification_id = (
        notification_id or create_public_id("noti")
    )

    # Tạo id cho event tạo thông báo
    resolved_event_id = (
        event_id or create_public_id("evt")
    )

    # Lấy thời gian khả dụng, và thời gian hết hạn cho thông báo này
    available_at = command.available_at or now
    expires_at = command.expires_at
    # Nếu không có thời gian hết hạn thì tư tính thời gian hết hạn
    if expires_at is None:
        expires_at = (
            available_at
            + timedelta(
                seconds=command.delivery_policy.ttl_seconds
            )
        )

    payload = command.model_dump()

    # Thêm một số trường cho payload
    payload.update(
        {
            "schema_version": "1.0",
            "notification_id": resolved_notification_id,
            "event_id": resolved_event_id,
            "created_at": now,
            "available_at": available_at,
            "expires_at": expires_at,
        }
    )

    # Chạy kiểm tra các trường trong Notification, cùng kiểu dữ liệu của từng trường
    # Sau đó chạy thêm các hàm validator của class nếu nó được khai báo
    return NotificationEvent.model_validate(payload)

# ============================================================
# HÀM TẠO CÁC EVENT CỤ THỂ
# ============================================================

def build_role_created_event(
    *,
    tenant_id: str,
    role_id: str,
    role_name: str,
    permissions: list[str],
    actor_user_id: str,
    actor_username: str | None,
    recipient_user_ids: list[str],
    correlation_id: str | None = None,
    trace_id: str | None = None,
) -> NotificationEvent:
    """
    Tạo event role.created.

    recipient_user_ids nên là danh sách admin/superuser
    đã được truy vấn trước từ bảng user_auth.
    """
    # Tạo nội dung chung cho thông báo
    command = NotificationCreateCommand(
        tenant_id=tenant_id,                                # Mã tenant
        notification_type="role.created",                   # Loại thông báo
        category="security",                                # Phân loại thông báo

        source=NotificationSource(                          # Nguồn tạo thông báo
            service="api-server",                           # Được tạo ra khi gọi từ api
            instance="role-controller",                     # Cụ thể là nằm ở role controller
            environment="test",                             # Môi trường test
        ),

        actor=NotificationActor(                            # Đối tượng thực hiện hàng động này
            type=NotificationActorType.USER,
            id=actor_user_id,
            display_name=actor_username,
        ),

        resource=NotificationResource(                      # Đối tượng ảnh hưởng khi tạo role, là chính role
            type="role",
            id=role_id,
            version=1,
        ),

        audience=NotificationAudience(                      # Đối tượng nhận thông báo này
            type=NotificationAudienceType.USERS,
            user_ids=recipient_user_ids,                    # Nên để dạng danh sách, và có thể loại trừ gửi thông báo cho chính người vừa tạo
            exclude_user_ids=[actor_user_id],
        ),

        channels=[                                          # Kênh thông báo
            NotificationChannel.IN_APP,
            NotificationChannel.WEBSOCKET,
        ],

        content=NotificationContent(                        # Nội dung chính của thông báo
            template_key="role.created",
            template_version="1",
            locale="vi-VN",
            title="Role mới đã được tạo",
            body=(
                f"Role '{role_name}' đã được tạo bởi "
                f"{actor_username or 'Quản trị viên'}."
            ),
            short_body=(
                f"Role '{role_name}' vừa được tạo."
            ),
            variables={
                "role_id": role_id,
                "role_name": role_name,
                "actor_user_id": actor_user_id,
                "actor_username": actor_username,
            },
        ),

        action=NotificationAction(                          # Hành động khi người dùng click vào thông báo
            type=NotificationActionType.NAVIGATE,
            target=f"/roles/{role_id}",
            label="Xem chi tiết",
            method="GET",
        ),

        data={                                              # Đính kèm data để xử lý khi cần
            "role_id": role_id,
            "role_name": role_name,
            "permissions": permissions,
        },

        priority=NotificationPriority.NORMAL,               # Mức độ ưu tiên của thông báo
        severity=NotificationSeverity.SUCCESS,              # Trạng thái thông báo

        deduplication=NotificationDeduplication(            # chống trùng lặp trong việc tạo nhiều thông báo
            key=f"role:{tenant_id}:{role_id}:created",
            window_seconds=300,
        ),

        delivery_policy=NotificationDeliveryPolicy(         # Chính sách thông báo
            ttl_seconds=7 * 24 * 60 * 60,
            max_attempts=5,
            retry_delay_seconds=10,
            require_ack=False,
        ),

        trace=NotificationTrace(                            # Tác vụ nào đãn đến thông báo này
            correlation_id=correlation_id,
            trace_id=trace_id,
            causation_id=f"role_create:{role_id}",
        ),
    )

    return build_notification_event(command)

def build_role_updated_event(
    *,
    tenant_id: str,
    before_role: dict[str, Any],
    after_role: dict[str, Any],

    actor_user_id: str,
    actor_username: str | None,

    admin_user_ids: list[str],
    target_user_id: str | None,

    correlation_id: str | None = None,
    trace_id: str | None = None,

    # Idempotency key được truyền từ request nếu có.
    # Nó giúp ngăn một request bị retry nhiều lần tạo notification trùng.
    idempotency_key: str | None = None,

    # True nghĩa là người vừa cập nhật role
    # không nhận notification trong hộp thư cá nhân.
    exclude_actor: bool = True,
) -> NotificationEvent:
    """
    Tạo event cho role.updated.

    Người nhận DB bao gồm:
    - Các admin.
    - Một user cụ thể nếu target_user_id có giá trị.

    Redis Stream đích được xây dựng ở hàm khác.
    """
    # Thông tin định danh về role
    role_id = str(after_role["role_id"])
    role_name = str(after_role["role_name"])

    # Tạo 1 set chứa danh sách người nhận (set tự động loại các giá trị trùng lặp)
    # admin_user_ids = [123, "123", 123] thì chuyển sang set recipient_ids = {"123"}
    recipient_ids = {
        str(user_id).strip()
        for user_id in admin_user_ids
        if user_id and str(user_id).strip()
    }

    # Nếu chỉ định người nhận cụ thể thì thêm vào danh sách
    if target_user_id and target_user_id.strip():
        recipient_ids.add(target_user_id.strip())

    # Có thể loại người vừa thực hiện cập nhật khỏi danh sách người nhận
    if exclude_actor:
        recipient_ids.discard(actor_user_id)

    if not recipient_ids:
        raise ValueError(
            "Không có người nhận phù hợp cho thông báo role.updated"
        )

    # Xác định các nội dung đã thay đổi.
    changes = build_role_changes(
        before_role=before_role,
        after_role=after_role,
    )

    # Nội dung ngắn dùng cho toast hoặc danh sách notification.
    short_body = (
        f"Role '{role_name}' vừa được cập nhật."
    )

    # Nội dung đầy đủ.
    body = (
        f"Role '{role_name}' đã được cập nhật bởi "
        f"{actor_username or 'Quản trị viên'}."
    )

    # Chỉ dùng deduplication khi request có idempotency key.
    #
    # Không nên dùng một key cố định như:
    # role:role_01:updated
    #
    # vì hai lần cập nhật hợp lệ trong 5 phút có thể bị coi là trùng.
    deduplication = None

    if idempotency_key:
        deduplication = NotificationDeduplication(
            key=(
                f"role:{tenant_id}:{role_id}:updated:"
                f"{idempotency_key}"
            ),
            window_seconds=300,
        )

    # Đưa các giá trị vào NotificatioinCreateCommand để tạo 1 notification hoàn chỉnh
    command = NotificationCreateCommand(
        # ----------------------------------------------------
        # Thông tin chung
        # ----------------------------------------------------
        tenant_id=tenant_id,
        notification_type="role.updated",
        category="security",

        # ----------------------------------------------------
        # Service phát sinh notification
        # ----------------------------------------------------
        source=NotificationSource(
            service="api-server",
            instance="role-controller",
            environment="test",
        ),

        # ----------------------------------------------------
        # Người cập nhật role
        # ----------------------------------------------------
        actor=NotificationActor(
            type=NotificationActorType.USER,
            id=actor_user_id,
            display_name=actor_username,
        ),

        # ----------------------------------------------------
        # Resource bị cập nhật
        # ----------------------------------------------------
        resource=NotificationResource(
            type="role",
            id=role_id,

            # Model Roles hiện tại chưa có version.
            # Có thể để None.
            version=None,
        ),

        # ----------------------------------------------------
        # Danh sách người nhận thực tế
        # ----------------------------------------------------
        audience=NotificationAudience(
            type=NotificationAudienceType.USERS,
            user_ids=sorted(recipient_ids),
            role_ids=[],

            # Đã loại actor ở trên nên có thể để rỗng.
            exclude_user_ids=[],
        ),

        # ----------------------------------------------------
        # Kênh gửi
        # ----------------------------------------------------
        #
        # IN_APP:
        # tạo trạng thái unread/read trong hệ thống.
        #
        # WEBSOCKET:
        # gửi realtime qua Redis Stream + Notification Server.
        channels=[
            NotificationChannel.IN_APP,
            NotificationChannel.WEBSOCKET,
        ],

        # ----------------------------------------------------
        # Nội dung hiển thị
        # ----------------------------------------------------
        content=NotificationContent(
            template_key="role.updated",
            template_version="1",
            locale="vi-VN",

            title="Role đã được cập nhật",
            body=body,
            short_body=short_body,

            variables={
                "role_id": role_id,
                "role_name": role_name,
                "actor_user_id": actor_user_id,
                "actor_username": actor_username,
                "changes": changes,
            },
        ),

        # ----------------------------------------------------
        # Hành động khi bấm notification
        # ----------------------------------------------------
        action=NotificationAction(
            type=NotificationActionType.NAVIGATE,
            target=f"/roles/{role_id}",
            label="Xem chi tiết",
            method="GET",
            open_in_new_tab=False,
        ),

        # ----------------------------------------------------
        # Dữ liệu nghiệp vụ cho frontend
        # ----------------------------------------------------
        data={
            "role_id": role_id,
            "role_name": role_name,
            "changes": changes,
            "updated_by": {
                "user_id": actor_user_id,
                "username": actor_username,
            },
        },

        priority=NotificationPriority.NORMAL,
        severity=NotificationSeverity.INFO,

        deduplication=deduplication,

        delivery_policy=NotificationDeliveryPolicy(
            # Có hiệu lực trong 7 ngày.
            ttl_seconds=7 * 24 * 60 * 60,

            # Retry tối đa 5 lần cho delivery.
            max_attempts=5,

            retry_delay_seconds=10,
            require_ack=False,
        ),

        trace=NotificationTrace(
            correlation_id=correlation_id,
            trace_id=trace_id,
            causation_id=f"role_update:{role_id}",
        ),
    )

    # Hàm này sinh:
    # - notification_id
    # - event_id
    # - created_at
    # - available_at
    # - expires_at
    return build_notification_event(command)
    