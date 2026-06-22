from __future__ import annotations

from datetime import datetime
from typing import Optional

from sqlalchemy import Boolean, DateTime, Index, Integer, String, Unicode, UnicodeText, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from db.database import Base
from utils.utils import utc_now



"""
Notifications
= Nội dung là gì?

NotificationRecipients
= Ai nhận và người đó đã đọc chưa?

NotificationDeliveries
= Đã gửi qua WebSocket/email/push thành công chưa?

NotificationOutbox
= Event đã được publish vào Redis Stream chưa?




Mối quan hệ giữa các trường trong bảng
notifications
    notification_id
          │
          ├───────────────────────────────┐
          │                               │
          ▼                               ▼
notification_recipients         notification_deliveries
    notification_id                 notification_id
    recipient_user_id               recipient_user_id
                                    channel
          │
          │
          ▼
Người dùng đã đọc chưa?          Đã gửi qua kênh nào?
                                 Gửi thành công hay lỗi?

notifications
    event_id
    notification_id
          │
          ▼
notification_outbox
    event_id
    aggregate_id = notification_id
    stream_key
"""





# ============================================================
# 1. BẢNG NOTIFICATIONS
# ============================================================

class Notifications(Base):
    """
    Bảng lưu nội dung chung của một thông báo.

    Đây là bảng trung tâm của hệ thống notification.

    Một bản ghi tại đây mô tả:
    - Điều gì đã xảy ra.
    - Ai đã thực hiện hành động.
    - Đối tượng nghiệp vụ nào bị tác động.
    - Nội dung thông báo là gì.
    - Thông báo có hành động nào khi người dùng bấm vào.
    - Thông báo thuộc tenant nào.
    - Thông báo còn hiệu lực hay đã hết hạn.

    Bảng này KHÔNG lưu trạng thái unread/read của người dùng.

    Lý do:
    Một notification có thể gửi cho nhiều user.

    Ví dụ:
        notification_id = noti_001

        User A: đã đọc
        User B: chưa đọc
        User C: đã lưu trữ

    Trạng thái riêng của từng user phải nằm trong bảng
    notification_recipients.
    """

    __tablename__ = "notifications"

    # --------------------------------------------------------
    # INDEX TỔNG HỢP
    # --------------------------------------------------------
    #
    # Index tổng hợp giúp tối ưu các truy vấn thường dùng.
    #
    # Lưu ý:
    # Thứ tự cột trong index rất quan trọng.
    #
    # Ví dụ index:
    # (tenant_id, created_at)
    #
    # phù hợp với query:
    # WHERE tenant_id = ?
    # ORDER BY created_at DESC

    __table_args__ = (
        # Dùng khi lấy danh sách thông báo mới nhất của tenant.
        Index(
            "ix_notifications_tenant_created",
            "tenant_id",
            "created_at",
        ),

        # Dùng khi lọc thông báo theo loại trong một tenant.
        # Ví dụ lấy tất cả role.created của company_a.
        Index(
            "ix_notifications_tenant_type",
            "tenant_id",
            "notification_type",
        ),

        # Dùng khi lấy lịch sử thông báo của một resource.
        #
        # Ví dụ:
        # resource_type = "role"
        # resource_id   = "warehouse_manager"
        Index(
            "ix_notifications_resource",
            "tenant_id",
            "resource_type",
            "resource_id",
        ),

        # Dùng để kiểm tra thông báo trùng trong khoảng thời gian.
        Index(
            "ix_notifications_deduplication",
            "tenant_id",
            "deduplication_key",
            "created_at",
        ),
    )

    # --------------------------------------------------------
    # ID NỘI BỘ
    # --------------------------------------------------------

    # ID số tự tăng của SQL Server.
    #
    # Chỉ dùng nội bộ để:
    # - sắp xếp;
    # - tối ưu join;
    # - quản lý bản ghi trong DB.
    #
    # Không nên trả trường này cho client.
    id: Mapped[int] = mapped_column(
        Integer,
        primary_key=True,
        autoincrement=True,
    )

    # ID public của notification.
    #
    # Ví dụ:
    # noti_50a614d6b4714e58bc7582f45d43fc70
    #
    # ID này được dùng để:
    # - API lấy chi tiết notification;
    # - liên kết với notification_recipients;
    # - liên kết với notification_deliveries;
    # - liên kết với notification_outbox.
    notification_id: Mapped[str] = mapped_column( String(64), unique=True, index=True, nullable=False, )

    # ID của event nghiệp vụ.
    #
    # Ví dụ:
    # evt_66849c09d1df4567a6f1d18aa0e1c602
    #
    # notification_id:
    #     định danh nội dung thông báo.
    #
    # event_id:
    #     định danh sự kiện đã tạo ra thông báo.
    #
    # Event ID rất hữu ích cho:
    # - chống nhận trùng ở frontend;
    # - theo dõi event xuyên nhiều Redis Stream;
    # - audit;
    # - trace.
    event_id: Mapped[str] = mapped_column( String(64), unique=True, index=True, nullable=False, )
    # Phiên bản schema của payload event.
    # Khi cấu trúc event thay đổi, client có thể xử lý dựa theo version.
    schema_version: Mapped[str] = mapped_column( String(20), default="1.0", nullable=False, )

    # Tenant sở hữu notification.
    #
    # Bắt buộc trong hệ thống multi-tenant.
    #
    # Mọi truy vấn notification theo user nên đồng thời kiểm tra tenant_id
    # để tránh rò rỉ dữ liệu giữa các công ty.
    tenant_id: Mapped[str] = mapped_column( String(64), index=True, nullable=False, )

    # Loại sự kiện cụ thể.
    # - role.created
    # - role.updated
    # - user.password_changed
    # - file.uploaded
    # - file.processing_failed
    # - job.completed
    #
    # Không nên dùng tên quá chung như:
    # - UPDATE
    # - NOTIFICATION
    # - SYSTEM_EVENT
    notification_type: Mapped[str] = mapped_column( String(128), index=True, nullable=False, )

    # Nhóm chức năng lớn của notification.
    #
    # Ví dụ:
    # - security
    # - role
    # - user
    # - file
    # - job
    # - order
    # - system
    #
    # category thường được dùng để:
    # - lọc trên UI;
    # - hiển thị icon;
    # - thống kê số chưa đọc theo nhóm;
    # - áp dụng notification preference.
    category: Mapped[str] = mapped_column( String(64), index=True, nullable=False, )

    # Tên service tạo notification.
    #
    # Ví dụ:
    # - api-server
    # - order-service
    # - document-worker
    # - notification-worker
    source_service: Mapped[str] = mapped_column( String(100), nullable=False, )

    # Instance cụ thể tạo notification.
    #
    # Ví dụ:
    # - api-server-01
    # - api-server-pod-b7f8c
    # - role-controller
    #
    # Có thể để None nếu hệ thống chưa cần phân biệt instance.
    source_instance: Mapped[Optional[str]] = mapped_column( String(100), nullable=True, )

    # Môi trường tạo event.
    #
    # Giá trị thường dùng:
    # - development
    # - testing
    # - staging
    # - production
    environment: Mapped[str] = mapped_column( String(32), default="production", nullable=False, )

    # Loại actor.
    #
    # Ví dụ:
    # - user: người dùng thao tác
    # - system: hệ thống tự thực hiện
    # - service: một service thực hiện
    actor_type: Mapped[Optional[str]] = mapped_column( String(32), nullable=True, )

    # ID actor.
    #
    # Nếu actor_type = user:
    #     actor_id là user_id.
    #
    # Nếu actor_type = system:
    #     actor_id có thể là "system".
    #
    # Nếu actor_type = service:
    #     actor_id có thể là "job-worker".
    actor_id: Mapped[Optional[str]] = mapped_column( String(64), index=True, nullable=True, )

    # Tên hiển thị tại thời điểm tạo notification.
    actor_name: Mapped[Optional[str]] = mapped_column( Unicode(255), nullable=True, )

    # Avatar snapshot của actor.
    actor_avatar_url: Mapped[Optional[str]] = mapped_column( String(2048), nullable=True, )

    # ========================================================
    # RESOURCE CHỈ ĐỐI TƯỢNG NGHIỆP VỤ BỊ TÁC ĐỘNG
    # ========================================================

    # Loại resource.
    #
    # Ví dụ:
    # - role
    # - user
    # - job
    # - file
    # - order
    resource_type: Mapped[Optional[str]] = mapped_column( String(64), index=True, nullable=True, )

    # ID resource.
    #
    # Ví dụ:
    # resource_type = "role"
    # resource_id   = "warehouse_manager"
    #
    # Hoặc:
    # resource_type = "job"
    # resource_id   = "job_123"
    resource_id: Mapped[Optional[str]] = mapped_column( String(128), index=True, nullable=True, )

    # Phiên bản resource tại thời điểm xảy ra sự kiện.
    #
    # Hữu ích khi resource có optimistic locking hoặc versioning.
    resource_version: Mapped[Optional[int]] = mapped_column( Integer, nullable=True, )

    # Thông tin về người nhận thông báo
    # - user : là 1 người nhận
    # - users : Là nhiều người nhận
    # - role : 1 role nhận (là quy ước 1 nhóm theo role nào đó sẽ nhận)
    # - roles : Nhiều role nhận
    # - tenant : những người nằm trong tenant
    # - global : Toàn bộ
    audience_type: Mapped[str] = mapped_column( String(32), nullable=False, )

    # Snapshot cấu hình audience ban đầu dưới dạng JSON.
    #
    # Ví dụ:
    # {
    #     "type": "users",
    #     "user_ids": ["user_01", "user_02"],
    #     "role_ids": [],
    #     "exclude_user_ids": ["user_03"]
    # }
    # Danh sách người nhận thực tế được tách ra thành từng dòng
    # trong bảng notification_recipients.
    audience_json: Mapped[str] = mapped_column( UnicodeText, default="{}", nullable=False, )

    # Danh sách kênh vận chuyển notification dưới dạng JSON.
    #
    # Ví dụ:
    # [
    #     "in_app",
    #     "websocket",
    #     "email"
    # ]
    #
    # Phân biệt:
    #
    # channels_json:
    #     phương thức gửi, ví dụ websocket/email/push.
    #
    # stream_scope trong Outbox:
    #     Redis stream cụ thể, ví dụ user/tenant/job/global.
    channels_json: Mapped[str] = mapped_column( UnicodeText, default="[]", nullable=False, )

    # Mã template dùng để render nội dung.
    #
    # Ví dụ:
    # role.created
    # job.completed
    # file.processing_failed
    template_key: Mapped[Optional[str]] = mapped_column( String(128), nullable=True, )

    # Phiên bản template.
    template_version: Mapped[Optional[str]] = mapped_column( String(32), nullable=True, )

    # Ngôn ngữ notification.
    #
    # Ví dụ:
    # vi-VN
    # en-US
    # zh-CN
    locale: Mapped[str] = mapped_column( String(20), default="vi-VN", nullable=False, )

    # Tiêu đề thông báo.
    title: Mapped[str] = mapped_column( Unicode(255), nullable=False, )
    # Nội dung
    body: Mapped[str] = mapped_column( UnicodeText, nullable=False, )

    # Nội dung rút gọn dùng để hiển thị hoặc là preview
    short_body: Mapped[Optional[str]] = mapped_column( Unicode(500), nullable=True, )

    # Ảnh minh họa cho notification.
    image_url: Mapped[Optional[str]] = mapped_column( String(2048), nullable=True, )

    # Các biến đã dùng hoặc sẽ dùng để render template.
    #
    # Ví dụ:
    # {
    #     "role_id": "warehouse_manager",
    #     "role_name": "Warehouse Manager",
    #     "actor_name": "Super Admin"
    # }
    variables_json: Mapped[str] = mapped_column( UnicodeText, default="{}", nullable=False, )

    # Loại hành động khi người dùng bấm vào thông báo
    #
    # Ví dụ:
    # - none
    # - navigate
    # - open_url
    # - open_modal
    # - download_file
    # - call_api
    action_type: Mapped[str] = mapped_column( String(32), default="none", nullable=False, )

    # Đích đến của action.
    #
    # Ví dụ:
    # - /roles/warehouse_manager
    # - /jobs/job_123
    # - https://example.com/file.pdf
    action_target: Mapped[Optional[str]] = mapped_column( String(2048), nullable=True, )

    # Tên nút hiển thị.
    #
    # Ví dụ:
    # - Xem chi tiết
    # - Tải xuống
    # - Mở đơn hàng
    action_label: Mapped[Optional[str]] = mapped_column( Unicode(100), nullable=True, )

    # HTTP method nếu action có gọi API.
    #
    # Đối với navigate/open_url thường dùng GET.
    action_method: Mapped[str] = mapped_column( String(10), default="GET", nullable=False, )

    # Có mở action trong tab mới hay không.
    action_open_new_tab: Mapped[bool] = mapped_column( Boolean, default=False, nullable=False,)

    # Dữ liệu máy đọc dùng để frontend hoặc service xử lý.
    #
    # Ví dụ:
    # {
    #     "role_id": "warehouse_manager",
    #     "role_name": "Warehouse Manager",
    #     "permissions": ["job:read", "job:create"]
    # }
    #
    # Không đưa vào đây:
    # - password;
    # - access token;
    # - refresh token;
    # - thông tin nhạy cảm không cần thiết.
    data_json: Mapped[str] = mapped_column( UnicodeText, default="{}", nullable=False, )

    # Mức ưu tiên gửi.
    #
    # Giá trị:
    # - low
    # - normal
    # - high
    # - urgent
    priority: Mapped[str] = mapped_column( String(20), default="normal", nullable=False, )

    # Mức độ nghiêm trọng hoặc kiểu hiển thị.
    #
    # Giá trị:
    # - info
    # - success
    # - warning
    # - error
    # - critical
    severity: Mapped[str] = mapped_column( String(20), default="info", nullable=False, )

    # Khóa chống trùng do nghiệp vụ tự tạo.
    #
    # Ví dụ:
    # role:company_a:warehouse_manager:created
    #
    # Trước khi tạo notification, có thể query:
    # - cùng tenant_id;
    # - cùng deduplication_key;
    # - created_at nằm trong window.
    deduplication_key: Mapped[Optional[str]] = mapped_column( String(255), index=True, nullable=True, )

    # Khoảng thời gian chống trùng, tính bằng giây.
    #
    # Ví dụ 300 giây:
    # trong vòng 5 phút không tạo lại notification cùng key.
    deduplication_window_seconds: Mapped[Optional[int]] = mapped_column( Integer, nullable=True, )

    # TTL nghiệp vụ của notification, tính bằng giây.
    # 604800 giây = 7 ngày.
    delivery_ttl_seconds: Mapped[int] = mapped_column( Integer, default=604800, nullable=False, )

    # Số lần retry tối đa cho hoạt động gửi.
    max_attempts: Mapped[int] = mapped_column( Integer, default=5, nullable=False, )

    # Khoảng nghỉ cơ bản giữa các lần retry, tính bằng giây.
    retry_delay_seconds: Mapped[int] = mapped_column( Integer, default=10, nullable=False, )

    # Client có phải gửi ACK xác nhận đã nhận event hay không.
    #
    # False:
    #     server gửi thành công qua socket là đủ.
    #
    # True:
    #     cần cơ chế client ACK bằng notification_id/event_id.
    require_ack: Mapped[bool] = mapped_column( Boolean, default=False, nullable=False, )

    # Ví dụ một request tạo role có thể:
    # - tạo role;
    # - tạo audit log;
    # - tạo notification;
    # - publish Redis.
    #
    # Các thao tác này dùng cùng correlation_id.
    correlation_id: Mapped[Optional[str]] = mapped_column( String(128), index=True, nullable=True, )

    # Trace ID của request phân tán qua nhiều service.
    trace_id: Mapped[Optional[str]] = mapped_column( String(128), index=True, nullable=True, )

    # ID của hành động hoặc event trực tiếp gây ra notification.
    #
    # Ví dụ:
    # role_create:warehouse_manager
    causation_id: Mapped[Optional[str]] = mapped_column( String(128), nullable=True, )

    # Trạng thái chung của notification.
    #
    # Giá trị:
    # - active: còn hiệu lực
    # - cancelled: đã bị hủy
    # - expired: đã hết hạn
    #
    # Đây KHÔNG phải trạng thái unread/read.
    status: Mapped[str] = mapped_column( String(20), default="active", index=True, nullable=False, )

    # Thời điểm notification được phép bắt đầu gửi.
    # Cho phép hỗ trợ notification lên lịch trong tương lai.
    available_at: Mapped[datetime] = mapped_column( DateTime(timezone=True), default=utc_now, nullable=False, )

    # Thời điểm notification hết giá trị.
    #
    # Ví dụ notification chỉ có hiệu lực trong 7 ngày.
    expires_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True),index=True,nullable=True, )

    # Thời điểm tạo bản ghi.
    created_at: Mapped[datetime] = mapped_column( DateTime(timezone=True), default=utc_now, nullable=False, )

    # Thời điểm cập nhật gần nhất.
    updated_at: Mapped[datetime] = mapped_column( DateTime(timezone=True), default=utc_now, onupdate=utc_now, nullable=False, )


# ============================================================
# 2. BẢNG NOTIFICATION_RECIPIENTS
# ============================================================

class NotificationRecipients(Base):
    """
    Lưu trạng thái của một notification đối với từng người nhận.

    Mỗi notification có thể có nhiều recipient.

    Ví dụ:

        notification_id = noti_001

        recipient_user_id = user_a
        state = read

        recipient_user_id = user_b
        state = unread

    Bảng này được dùng trực tiếp cho:
    - danh sách thông báo của user;
    - số thông báo chưa đọc;
    - đánh dấu đã đọc;
    - ghim;
    - lưu trữ;
    - soft delete.
    """

    __tablename__ = "notification_recipients"

    __table_args__ = (
        # Một user chỉ có một trạng thái đối với một notification.
        #
        # Không được có:
        # noti_001 + user_a xuất hiện hai lần.
        UniqueConstraint(
            "notification_id",
            "recipient_user_id",
            name="uq_notification_recipient",
        ),

        # Tối ưu query:
        #
        # WHERE tenant_id = ?
        #   AND recipient_user_id = ?
        #   AND state = 'unread'
        Index(
            "ix_notification_recipients_user_state",
            "tenant_id",
            "recipient_user_id",
            "state",
        ),

        # Tối ưu danh sách notification mới nhất của user.
        Index(
            "ix_notification_recipients_user_created",
            "tenant_id",
            "recipient_user_id",
            "created_at",
        ),
    )

    # ID số nội bộ.
    id: Mapped[int] = mapped_column( Integer, primary_key=True, autoincrement=True, )

    # ID notification chung.
    #
    # Liên kết logic tới notifications.notification_id.
    #
    # Không có ForeignKey theo quy tắc thiết kế hiện tại của hệ thống.
    notification_id: Mapped[str] = mapped_column( String(64), index=True, nullable=False, )

    # Tenant của người nhận.
    tenant_id: Mapped[str] = mapped_column( String(64), index=True, nullable=False, )

    # User nhận notification.
    #
    # Liên kết logic tới users.user_id.
    recipient_user_id: Mapped[str] = mapped_column( String(64), index=True, nullable=False, )

    # Trạng thái notification của riêng user này.
    #
    # Luồng thường gặp:
    #
    # unread
    #   ↓
    # seen
    #   ↓
    # read
    #
    # Hoặc:
    # read → archived
    #
    # Hoặc:
    # bất kỳ trạng thái nào → deleted
    state: Mapped[str] = mapped_column( String(20), default="unread", index=True, nullable=False, )

    # User có ghim notification lên đầu danh sách hay không.
    is_pinned: Mapped[bool] = mapped_column( Boolean, default=False, nullable=False, )

    # Thời điểm notification đã được đưa vào vùng dữ liệu của user.
    #
    # Trường này thường được đặt khi:
    # - bản ghi recipient được tạo;
    # - hoặc hệ thống xác nhận notification đã sẵn sàng cho user.
    #
    # Không nhất thiết đồng nghĩa WebSocket client đã nhận.
    delivered_at: Mapped[Optional[datetime]] = mapped_column( DateTime(timezone=True), nullable=True, )

    # Thời điểm notification đã xuất hiện trên giao diện.
    #
    # Ví dụ user mở dropdown thông báo và notification được render.
    seen_at: Mapped[Optional[datetime]] = mapped_column( DateTime(timezone=True), nullable=True, )

    # Thời điểm user mở chi tiết hoặc chọn "đánh dấu đã đọc".
    read_at: Mapped[Optional[datetime]] = mapped_column( DateTime(timezone=True), nullable=True, )

    # Thời điểm user lưu trữ notification.
    archived_at: Mapped[Optional[datetime]] = mapped_column( DateTime(timezone=True), nullable=True, )

    # Thời điểm soft delete.
    #
    # Không xóa vật lý ngay để:
    # - audit;
    # - khôi phục;
    # - thống kê;
    # - tránh phá vỡ lịch sử.
    deleted_at: Mapped[Optional[datetime]] = mapped_column( DateTime(timezone=True), nullable=True, )

    created_at: Mapped[datetime] = mapped_column( DateTime(timezone=True), default=utc_now, nullable=False, )

    updated_at: Mapped[datetime] = mapped_column( DateTime(timezone=True), default=utc_now, onupdate=utc_now, nullable=False, )


# ============================================================
# 3. BẢNG NOTIFICATION_DELIVERIES
# ============================================================

class NotificationDeliveries(Base):
    """
    Theo dõi việc gửi notification theo từng user và từng kênh.

    Một notification gửi cho một user qua hai channel sẽ tạo hai dòng.

    Ví dụ:

        noti_001 + user_a + websocket
        noti_001 + user_a + in_app

    Nếu có:
        3 người nhận
        2 channel

    thì số dòng delivery là:
        3 × 2 = 6

    Bảng này trả lời các câu hỏi:
    - WebSocket đã gửi chưa?
    - Email đã gửi chưa?
    - Push bị lỗi gì?
    - Đã retry bao nhiêu lần?
    - Khi nào retry tiếp?
    """

    __tablename__ = "notification_deliveries"

    __table_args__ = (
        # Mỗi notification + recipient + channel chỉ có một dòng trạng thái.
        #
        # Retry sẽ cập nhật attempt_count trên cùng bản ghi.
        #
        # Nếu cần lưu lịch sử chi tiết từng attempt,
        # nên tạo thêm bảng notification_delivery_attempts.
        UniqueConstraint(
            "notification_id",
            "recipient_user_id",
            "channel",
            name="uq_notification_delivery_channel",
        ),

        # Worker dùng index này để lấy các delivery cần retry:
        #
        # WHERE status = 'failed'
        #   AND next_retry_at <= now
        Index(
            "ix_notification_deliveries_status_retry",
            "status",
            "next_retry_at",
        ),

        # Dùng khi xem lịch sử gửi tới một user.
        Index(
            "ix_notification_deliveries_recipient",
            "tenant_id",
            "recipient_user_id",
            "created_at",
        ),
    )

    id: Mapped[int] = mapped_column( Integer, primary_key=True, autoincrement=True, )

    # ID public của delivery.
    #
    # Ví dụ:
    # delivery_11cb1be0...
    delivery_id: Mapped[str] = mapped_column( String(64), unique=True, index=True, nullable=False, )

    # ID notification chung.
    notification_id: Mapped[str] = mapped_column( String(64), index=True, nullable=False, )

    # Tenant của delivery.
    tenant_id: Mapped[str] = mapped_column( String(64), index=True, nullable=False, )

    # User nhận notification.
    recipient_user_id: Mapped[str] = mapped_column( String(64), index=True, nullable=False, )

    # Kênh gửi.
    #
    # Giá trị:
    # - in_app
    # - websocket
    # - email
    # - push
    # - sms
    #
    # Đây là transport channel, không phải Redis stream scope.
    channel: Mapped[str] = mapped_column( String(32), index=True, nullable=False, )

    # Trạng thái của delivery.
    #
    # Luồng thường gặp:
    #
    # pending
    #   ↓
    # queued
    #   ↓
    # sending
    #   ↓
    # sent
    #   ↓
    # delivered
    #
    # Nếu lỗi:
    # sending → failed → queued → sending
    #
    # Trạng thái khác:
    # expired
    # cancelled
    status: Mapped[str] = mapped_column( String(20), default="pending", index=True, nullable=False, )

    # Nhà cung cấp hoặc cơ chế gửi.
    #
    # Ví dụ:
    # - redis-stream
    # - websocket-internal
    # - smtp
    # - firebase
    # - twilio
    provider: Mapped[Optional[str]] = mapped_column( String(100), nullable=True, )

    # ID message do provider trả về.
    #
    # Ví dụ:
    # - Redis stream ID: 1781685000100-0
    # - Firebase message ID
    # - SMTP provider message ID
    provider_message_id: Mapped[Optional[str]] = mapped_column( String(255), nullable=True, )

    # Số lần hệ thống đã thử gửi.
    attempt_count: Mapped[int] = mapped_column( Integer, default=0, nullable=False, )

    # Số lần gửi tối đa cho delivery này.
    #
    # Đây là snapshot từ delivery policy của notification.
    max_attempts: Mapped[int] = mapped_column( Integer, default=5, nullable=False, )

    # Thời điểm được phép retry tiếp.
    #
    # Worker thường query:
    # status = failed
    # next_retry_at <= current_time
    next_retry_at: Mapped[Optional[datetime]] = mapped_column( DateTime(timezone=True), index=True, nullable=True, )

    # Mã lỗi chuẩn hóa.
    #
    # Ví dụ:
    # - REDIS_TIMEOUT
    # - SMTP_AUTH_FAILED
    # - PUSH_TOKEN_INVALID
    error_code: Mapped[Optional[str]] = mapped_column( String(100), nullable=True, )

    # Mô tả lỗi chi tiết để debug.
    error_message: Mapped[Optional[str]] = mapped_column( UnicodeText, nullable=True, )

    # Thời điểm delivery được đưa vào queue.
    queued_at: Mapped[Optional[datetime]] = mapped_column( DateTime(timezone=True), nullable=True, )

    # Thời điểm provider nhận yêu cầu gửi thành công.
    #
    # Ví dụ Redis XADD thành công có thể đánh dấu sent_at.
    sent_at: Mapped[Optional[datetime]] = mapped_column( DateTime(timezone=True), nullable=True, )

    # Thời điểm xác nhận người nhận/client đã nhận.
    #
    # Với WebSocket, muốn chính xác cần:
    # - Notification Server xác nhận gửi;
    # hoặc
    # - client gửi ACK.
    delivered_at: Mapped[Optional[datetime]] = mapped_column( DateTime(timezone=True), nullable=True, )

    # Thời điểm gửi thất bại gần nhất.
    failed_at: Mapped[Optional[datetime]] = mapped_column( DateTime(timezone=True), nullable=True, )

    created_at: Mapped[datetime] = mapped_column( DateTime(timezone=True), default=utc_now, nullable=False, )

    updated_at: Mapped[datetime] = mapped_column( DateTime(timezone=True), default=utc_now, onupdate=utc_now, nullable=False, )


# ============================================================
# 4. BẢNG NOTIFICATION_OUTBOX
# ============================================================

class NotificationOutbox(Base):
    """
    Transactional Outbox dùng để bảo đảm event không bị mất
    khi Redis hoặc Notification Server tạm thời lỗi.

    Ví dụ luồng:

        1. Role được tạo thành công.
        2. Notification được lưu SQL Server.
        3. Outbox được lưu SQL Server.
        4. Redis bị down.
        5. API vẫn có dữ liệu Outbox.
        6. Worker retry khi Redis hồi phục.

    Một event có thể được gửi vào nhiều Redis Stream.

    Ví dụ event role.created:

        event_id = evt_001

        Outbox 1:
            stream_scope = user
            stream_key = stream:noti:tenant:t1:user:u1

        Outbox 2:
            stream_scope = user
            stream_key = stream:noti:tenant:t1:user:u2

        Outbox 3:
            stream_scope = tenant
            stream_key = stream:noti:tenant:t1:dashboard

        Outbox 4:
            stream_scope = global
            stream_key = stream:noti:global

    Vì vậy event_id KHÔNG được unique một mình.

    Cặp:
        event_id + stream_key

    mới phải duy nhất.
    """

    __tablename__ = "notification_outbox"

    __table_args__ = (
        # Một event chỉ được tạo một Outbox cho cùng một Redis Stream.
        #
        # Cho phép cùng event_id xuất hiện ở nhiều stream khác nhau.
        UniqueConstraint(
            "event_id",
            "stream_key",
            name="uq_notification_outbox_event_stream",
        ),

        # Worker tìm Outbox đang chờ:
        #
        # WHERE status = 'pending'
        #   AND available_at <= current_time
        Index(
            "ix_notification_outbox_pending",
            "status",
            "available_at",
        ),

        # Tìm toàn bộ outbox của một notification hoặc aggregate.
        Index(
            "ix_notification_outbox_aggregate",
            "aggregate_type",
            "aggregate_id",
        ),

        # Tìm outbox theo stream scope và trạng thái.
        #
        # Ví dụ retry riêng user stream hoặc global stream.
        Index(
            "ix_notification_outbox_scope_status",
            "stream_scope",
            "status",
        ),
    )

    id: Mapped[int] = mapped_column( Integer, primary_key=True, autoincrement=True, )

    # ID public riêng của mỗi bản ghi Outbox.
    #
    # Một event đi vào bốn stream sẽ có bốn outbox_id khác nhau.
    outbox_id: Mapped[str] = mapped_column( String(64), unique=True, index=True, nullable=False, )

    # ID event chung.
    #
    # QUAN TRỌNG:
    # Không đặt unique=True ở đây.
    #
    # Vì cùng một event có thể đi vào nhiều stream.
    event_id: Mapped[str] = mapped_column( String(64), index=True, nullable=False, )

    # Tenant sở hữu event.
    tenant_id: Mapped[str] = mapped_column( String(64), index=True, nullable=False, )

    # Phạm vi Redis Stream.
    #
    # Giá trị:
    # - user
    # - tenant
    # - job
    # - global
    #
    # Phân biệt với NotificationDeliveries.channel:
    #
    # channel:
    #     websocket, email, push...
    #
    # stream_scope:
    #     user, tenant, job, global.
    stream_scope: Mapped[str] = mapped_column( String(20), index=True, nullable=False, )

    # Tenant đích của stream.
    #
    # Bắt buộc với:
    # - user stream;
    # - tenant stream;
    # - job stream.
    #
    # Có thể None với global stream.
    target_tenant_id: Mapped[Optional[str]] = mapped_column( String(64), nullable=True, )

    # User đích.
    #
    # Chỉ có giá trị khi stream_scope = user.
    target_user_id: Mapped[Optional[str]] = mapped_column( String(64), nullable=True, )

    # Job đích.
    #
    # Chỉ có giá trị khi stream_scope = job.
    target_job_id: Mapped[Optional[str]] = mapped_column( String(64), nullable=True, )

    # Loại aggregate tạo event.
    #
    # Trong hệ thống notification thường dùng:
    # aggregate_type = "notification"
    #
    # Nhưng cũng có thể dùng:
    # - role
    # - job
    # - order
    aggregate_type: Mapped[str] = mapped_column( String(64), nullable=False, )

    # ID aggregate.
    #
    # Nếu aggregate_type = notification:
    # aggregate_id = notification_id.
    aggregate_id: Mapped[str] = mapped_column( String(64), index=True, nullable=False, )

    # Loại event.
    #
    # Ví dụ:
    # - role.created
    # - job.completed
    # - file.processing_failed
    event_type: Mapped[str] = mapped_column( String(128), index=True, nullable=False, )

    # Redis Stream key thật.
    #
    # Ví dụ:
    # - stream:noti:tenant:t1:user:u1
    # - stream:noti:tenant:t1:dashboard
    # - stream:noti:tenant:t1:job:j1
    # - stream:noti:global
    stream_key: Mapped[str] = mapped_column( String(255), nullable=False, )

    # Toàn bộ NotificationEvent được serialize thành JSON.
    #
    # Worker không cần build lại event từ nhiều bảng.
    #
    # Nó chỉ cần:
    # 1. đọc payload_json;
    # 2. publish XADD;
    # 3. cập nhật trạng thái Outbox.
    payload_json: Mapped[str] = mapped_column( UnicodeText, nullable=False, )

    # Trạng thái Outbox.
    #
    # Luồng:
    #
    # pending
    #   ↓ worker claim
    # processing
    #   ↓ publish thành công
    # published
    #
    # Nếu lỗi:
    # processing → pending để retry
    #
    # Nếu vượt max_attempts:
    # processing → failed
    status: Mapped[str] = mapped_column( String(20), default="pending", index=True, nullable=False, )

    # Số lần worker đã claim/thử publish.
    attempt_count: Mapped[int] = mapped_column( Integer, default=0, nullable=False, )

    # Số lần thử tối đa.
    max_attempts: Mapped[int] = mapped_column( Integer, default=10, nullable=False, )

    # Thời điểm Outbox được phép xử lý.
    #
    # Dùng cho:
    # - gửi trễ;
    # - exponential backoff;
    # - retry sau khi lỗi.
    available_at: Mapped[datetime] = mapped_column( DateTime(timezone=True), default=utc_now, index=True, nullable=False, )

    # Thời điểm worker claim bản ghi.
    #
    # Dùng để phát hiện bản ghi bị kẹt nếu worker crash.
    locked_at: Mapped[Optional[datetime]] = mapped_column( DateTime(timezone=True), nullable=True, )

    # ID worker đang xử lý.
    #
    # Ví dụ:
    # noti-worker:container-01:a82cd5
    locked_by: Mapped[Optional[str]] = mapped_column(String(100),nullable=True,)

    # Redis Stream ID trả về sau XADD.
    #
    # Ví dụ:
    # 1781685000100-0
    published_stream_id: Mapped[Optional[str]] = mapped_column( String(100), nullable=True, )

    # Thời điểm publish thành công.
    published_at: Mapped[Optional[datetime]] = mapped_column( DateTime(timezone=True), nullable=True, )

    # Lỗi gần nhất của Outbox.
    #
    # Ví dụ:
    # TimeoutError: Timeout writing to Redis
    last_error: Mapped[Optional[str]] = mapped_column( UnicodeText, nullable=True, )

    created_at: Mapped[datetime] = mapped_column( DateTime(timezone=True), default=utc_now, nullable=False, )

    updated_at: Mapped[datetime] = mapped_column( DateTime(timezone=True), default=utc_now, onupdate=utc_now, nullable=False, )