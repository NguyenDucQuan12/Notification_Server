from __future__ import annotations
from enum import StrEnum
from typing import Any

from db.database import AsyncSessionLocal
from db.query_users import get_admin_user_ids
from services.notification_event_service import build_role_created_event, build_role_updated_event
from db.query_notifications import add_notification_event_to_session
from services.notification_publisher import publish_notification_event, build_stream_targets, publish_multiple_outboxes_immediately


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

async def create_role_notification_safely(
    *,
    tenant_id: str,
    role_data: dict[str, Any],
    permissions: list[str],
    current_user: dict[str, Any],
    mode: NotificationDispatchMode,
) -> dict[str, Any]:
    """
    Tạo notification dành cho role.created.

    Hàm này luôn dùng session riêng.
    Không dùng session tạo role.

    Mọi exception đều được giữ bên trong hàm.
    Caller sẽ không nhận exception notification.
    """

    try:
        # Tạo 1 session riêng kết nối tới DB, nếu lỗi thì rollback session này, ko ảnh hưởng các session khác
        async with AsyncSessionLocal() as notification_db:
            #  Bắt đầu 1 transaction
            async with notification_db.begin():

                # Lấy danh sách các admin để thông báo tới họ, cái này chỉ thông báo tới admin
                # Hàm này phải trả về list[str]. 
                # Ví dụ: 
                #  [ 
                    # "admin_user_001", 
                    # "admin_user_002", 
                # ]
                
                admin_user_ids = await get_admin_user_ids(
                    notification_db,
                    tenant_id=tenant_id,
                )
                admin_user_ids = admin_user_ids["data"]

                if not admin_user_ids:
                    return {
                        "status": "skipped",
                        "mode": mode.value,
                        "reason": "Không tìm thấy admin nhận thông báo",
                        "recipient_count": 0,
                    }
                # Không gửi lại cho người vừa tạo role. 

                recipient_user_ids = [ user_id for user_id in admin_user_ids if user_id != current_user["user_id"] ]

                # Nếu người tạo role không cần nhận thông báo của chính mình, 
                # build_role_created_event sẽ đưa user này vào exclude_user_ids. 
                actor_user_id = current_user["user_id"]

                # event là NotificationEvent, chứa: 
                # - notification_id 
                # - event_id 
                # - nội dung 
                # - actor 
                # - resource 
                # - audience 
                # - channel 
                # - policy 
                # - trace 
                # - timestamps

                event = build_role_created_event(
                    tenant_id=tenant_id,
                    role_id=role_data["role_id"],
                    role_name=role_data["role_name"],
                    permissions=permissions,

                    actor_user_id= actor_user_id,
                    actor_username=current_user.get("username"),

                    recipient_user_ids=recipient_user_ids,

                    correlation_id=current_user.get(
                        "correlation_id"
                    ),
                    trace_id=current_user.get("trace_id"),
                )

                # ============================================ 
                #  GHI EVENT VÀO SQL SERVER 
                # ============================================ 
                #  Hàm này thường ghi: 
                #  1. notifications 
                #  Một bản ghi nội dung chung. 
                # 2. notification_recipients 
                #  Một bản ghi cho mỗi người nhận. 
                # 3. notification_deliveries 
                # - Một bản ghi cho mỗi người nhận × mỗi channel. 
                # 4. notification_outbox 
                # - Một bản ghi chờ publish Redis. 
                # Hàm chỉ flush, transaction bên ngoài sẽ commit.
                # Xác định event sẽ được ghi vào các Redis Stream nào. 
                stream_targets = build_stream_targets( tenant_id=tenant_id, recipient_user_ids=recipient_user_ids, 
                                                                    # Mỗi admin có một user stream riêng. 
                                                                    send_to_users=True, 
                                                                    # Bật khi muốn tenant dashboard cũng nhận event. 
                                                                    send_to_tenant_dashboard=True, 
                                                                    # Global stream phục vụ superuser/monitoring. 
                                                                    send_to_global=True, ) 

                result = await add_notification_event_to_session(
                    db=notification_db,
                    event=event,
                    stream_targets= stream_targets,
                )
                # Khi ra khỏi `async with notification_db.begin()`: 
                # - Không có exception -> transaction tự commit. 
                # - Có exception -> transaction tự rollback.

        # Đến đây notification + recipient + delivery + outbox
        # đã được commit thành công.
        response: dict[str, Any] = {
            "status": "stored",
            "mode": mode.value,
            # ID của nội dung notification
            "notification_id": result.notification_id,
            # ID của event được publish.
            "event_id": result.event_id,
            # ID của bản ghi outbox.
            "outbox_id": result.outbox_id,
            # Số user thực tế nhận thông báo.
            "recipient_count": result.recipient_count,
            # Số bản ghi delivery. 
            "delivery_count": result.delivery_count, 
            "created_at": result.created_at.isoformat(),
        }

        # Nếu mode là Direct thì tiến hành push ngay lên Redis
        if mode == NotificationDispatchMode.DIRECT:
            
            # Publish event vào từng stream tương ứng. 
            publish_result = await publish_notification_event( event=event, targets=stream_targets, )

            # publish_result = (
            #     await publish_one_outbox_immediately(
            #         outbox_id=result.outbox_id,
            #     )
            # )

            response["publish"] = publish_result

        return response

    except Exception as exc:
        # Thay print bằng system_logger.exception trong production.
        print(
            "Không thể tạo notification role.created: "
            f"{type(exc).__name__}: {exc}",
            flush=True,
        )

        return {
            "status": "notification_failed",
            "mode": mode.value,
            "error_type": type(exc).__name__,
            "error_message": str(exc),
        }

async def create_role_updated_notification_safely(
    *, tenant_id: str, before_role: dict[str, Any], after_role: dict[str, Any], target_user_id: str | None, 
    current_user: dict[str, Any], mode: NotificationDispatchMode, send_to_admin_personal_streams: bool = False, 
    idempotency_key: str | None = None, ) -> dict[str, Any]:
    """
    Tạo notification cho role.updated bằng session độc lập.

    Hàm không raise lỗi notification ra ngoài.

    Vì vậy:
    - role đã cập nhật vẫn giữ nguyên;
    - API update role vẫn có thể trả success;
    - lỗi notification chỉ được ghi vào kết quả/log.
    """

    try:
        # ====================================================
        # LƯU NOTIFICATION VÀ OUTBOX
        # ====================================================

        # Tạo 1 Session riêng để nếu rollback thì chỉ rollback ở session này, ko ảnh hưởng các session khác
        async with AsyncSessionLocal() as notification_db:
            async with notification_db.begin():
                # --------------------------------------------
                # Lấy danh sách admin của tenant để gửi thông báo tới họ
                # --------------------------------------------
                admin_user_ids = await get_admin_user_ids(
                    notification_db,
                    tenant_id=tenant_id,
                )
                # Lấy id của người đang thực hiện hành động
                actor_user_id = str(
                    current_user["user_id"]
                )

                # --------------------------------------------
                # Tạo NotificationEvent hoàn chỉnh.
                # --------------------------------------------
                event = build_role_updated_event(
                    tenant_id=tenant_id,

                    before_role=before_role,
                    after_role=after_role,

                    actor_user_id=actor_user_id,
                    actor_username=current_user.get(
                        "username"
                    ),

                    admin_user_ids=admin_user_ids,
                    target_user_id=target_user_id,

                    correlation_id=current_user.get(
                        "correlation_id"
                    ),
                    trace_id=current_user.get("trace_id"),

                    idempotency_key=idempotency_key,

                    # Người vừa cập nhật không nhận trong inbox cá nhân.
                    exclude_actor=False,
                )

                # --------------------------------------------
                # Xác định Redis Stream đích. Có nghĩa là Noti này sẽ được gửi tới những ai
                # --------------------------------------------
                stream_targets = (
                    build_stream_targets(
                        tenant_id=tenant_id,
                        admin_user_ids=admin_user_ids,
                        target_user_id=target_user_id,

                        send_to_admin_dashboard=True,
                        send_to_global=True,
                        send_to_target_user=True,

                        send_to_admin_personal_streams=(
                            send_to_admin_personal_streams
                        ),

                        actor_user_id=actor_user_id,
                    )
                )

                # --------------------------------------------
                # Ghi dữ liệu vào:
                #
                # - notifications
                # - notification_recipients
                # - notification_deliveries
                # - notification_outbox
                # --------------------------------------------
                result = await add_notification_event_to_session(
                    db=notification_db,
                    event=event,
                    stream_targets=stream_targets,
                )

            # Ra khỏi notification_db.begin():
            #
            # Không lỗi:
            #     transaction notification được commit.
            #
            # Có lỗi:
            #     transaction notification rollback.

        # ====================================================
        # TẠO RESPONSE SAU KHI DB ĐÃ COMMIT
        # ====================================================

        response: dict[str, Any] = {
            "status": "stored",
            "mode": mode.value,
            "notification_id": result.notification_id,
            "event_id": result.event_id,
            "outbox_ids": result.outbox_ids,
            "recipient_count": result.recipient_count,
            "delivery_count": result.delivery_count,
            "stream_target_count": (
                result.stream_target_count
            ),
            "created_at": result.created_at.isoformat(),
        }

        # ====================================================
        # DIRECT PUBLISH (GỬI NOTI TỚI NGƯỜI DÙNG)
        # ====================================================
        if mode == NotificationDispatchMode.DIRECT:
            publish_result = (
                await publish_multiple_outboxes_immediately(
                    outbox_ids=result.outbox_ids,
                )
            )

            response["publish"] = publish_result

        # WORKER mode không gọi Redis ở đây.
        # Worker sẽ lấy các Outbox pending.
        return response

    except Exception as exc:
        # Không raise lại.
        #
        # Role đã update và commit trước đó không bị ảnh hưởng.
        print(
            "Không thể tạo thông báo role.updated: "
            f"{type(exc).__name__}: {exc}",
            flush=True,
        )

        return {
            "status": "notification_failed",
            "mode": mode.value,
            "error_type": type(exc).__name__,
            "error_message": str(exc),
        }