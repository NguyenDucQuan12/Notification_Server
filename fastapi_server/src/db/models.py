from __future__ import annotations

from datetime import datetime
from typing import Optional

from sqlalchemy import Boolean, DateTime, Integer, String, Text, Unicode, and_
from sqlalchemy.orm import Mapped, mapped_column

from db.database import Base
from utils.utils import utc_now


"""
Định nghĩa các bảng SQL Server bằng SQLAlchemy ORM.

Nguyên tắc thiết kế trong file này:
- Không sử dụng ForeignKey.
- Không để SQL Server tự ràng buộc quan hệ giữa các bảng.
- Các bảng liên kết với nhau bằng các mã chung như:
    + user_id
    + role_id
    + tenant_id
    + job_id
- Khi cần lấy dữ liệu liên quan, app sẽ tự join bằng SQLAlchemy.
- Một số relationship được khai báo bằng primaryjoin + viewonly=True.
  Điều này giúp truy vấn tiện hơn ở tầng ORM nhưng không tạo khóa ngoại trong DB.
"""


class Users(Base):
    """
    Bảng users lưu thông tin hồ sơ người dùng.

    Bảng này chỉ chứa thông tin định danh và hồ sơ cơ bản,
    """

    __tablename__ = "users"

    # ID số tự tăng nội bộ trong DB.
    # Cột này chủ yếu dùng cho DB, không nên đưa ra ngoài client.
    id: Mapped[int] = mapped_column( Integer, primary_key=True, autoincrement=True, )

    # ID public của người dùng.
    # Dùng để liên kết với các bảng khác như auth_user, job_record, notification...
    user_id: Mapped[str] = mapped_column( String(64), unique=True, index=True, nullable=False, )

    # Tenant dùng để tách dữ liệu giữa nhiều nhóm/khách hàng/công ty.
    # Ví dụ:
    # - tenant_id = "company_a"
    # - tenant_id = "company_b"

    tenant_id: Mapped[str] = mapped_column( String(64), index=True, nullable=True, )

    # Thông tin hồ sơ người dùng.
    full_name: Mapped[Optional[str]] = mapped_column( Unicode(255), nullable=True, )
    email: Mapped[Optional[str]] = mapped_column( String(255), nullable=True, index=True, )
    phone: Mapped[Optional[str]] = mapped_column( String(32), nullable=True, )

    # Trạng thái hoạt động chung của user.
    is_active: Mapped[bool] = mapped_column( Boolean, default=True, nullable=False, )

    # Luôn lưu thời gian theo UTC để tránh lệch múi giờ.
    created_at: Mapped[datetime] = mapped_column( DateTime(timezone=True), default=utc_now, nullable=False, )
    updated_at: Mapped[datetime] = mapped_column( DateTime(timezone=True), default=utc_now, onupdate=utc_now, nullable=False, )


class UserAuth(Base):
    """
    Bảng user_auth lưu thông tin đăng nhập và phân quyền.

    Lưu ý:
    - Chỉ lưu password_hash.
    """

    __tablename__ = "user_auth"


    id: Mapped[int] = mapped_column( Integer, primary_key=True, autoincrement=True, )

    # Mã tenant được lưu lại để khi login có thể kiểm tra theo tenant.
    # Việc lưu tenant_id ở user_auth là dạng denormalize nhẹ,
    # giúp truy vấn nhanh hơn và kiểm soát đăng nhập theo từng khách hàng/nhóm.
    tenant_id: Mapped[str] = mapped_column( String(64), index=True, nullable=False, )

    # user_id dùng để liên kết tới users.user_id.
    # Không khai báo ForeignKey, nhưng vẫn đặt unique=True để đảm bảo
    # mỗi user chỉ có một bản ghi đăng nhập.
    user_id: Mapped[str] = mapped_column( String(64), unique=True, index=True, nullable=False, )

    # role_id dùng để liên kết tới roles.role_id.
    role_id: Mapped[Optional[str]] = mapped_column( String(64), index=True, nullable=True, )

    # Username dùng để đăng nhập.
    username: Mapped[str] = mapped_column( String(128), index=True, nullable=False, )

    # Mật khẩu đã hash.
    # Tuyệt đối không lưu mật khẩu gốc.
    password_hash: Mapped[str] = mapped_column( String(512), nullable=False, )

    # Superuser có quyền cao nhất trong hệ thống.
    # Chỉ nên dùng cho admin kỹ thuật hoặc admin hệ thống.
    is_superuser: Mapped[bool] = mapped_column( Boolean, default=False, nullable=False, )

    # Cho phép khóa riêng phần đăng nhập.
    # Ví dụ user vẫn còn trong hệ thống nhưng không được login.
    is_login_enabled: Mapped[bool] = mapped_column( Boolean, default=True, nullable=False, )

    # Số lần đăng nhập sai.
    # Có thể dùng để khóa tài khoản tạm thời sau nhiều lần sai mật khẩu.
    failed_login_count: Mapped[int] = mapped_column( Integer, default=0, nullable=False, )

    # Lần đăng nhập thành công gần nhất.
    last_login_at: Mapped[Optional[datetime]] = mapped_column( DateTime(timezone=True), nullable=True, )

    # Thời điểm đổi mật khẩu gần nhất.
    password_changed_at: Mapped[Optional[datetime]] = mapped_column( DateTime(timezone=True), nullable=True, )

    created_at: Mapped[datetime] = mapped_column( DateTime(timezone=True), default=utc_now, nullable=False, )

    updated_at: Mapped[datetime] = mapped_column( DateTime(timezone=True), default=utc_now, onupdate=utc_now, nullable=False, )


class Roles(Base):
    """
    Bảng roles lưu thông tin quyền hạn.

    Mỗi role đại diện cho một nhóm quyền, ví dụ:
    - admin
    - manager
    - staff
    - viewer

    permissions_json lưu danh sách quyền ở dạng JSON string.
    Ví dụ:
    [
        "job:create",
        "job:read",
        "job:download",
        "user:manage"
    ]
    """

    __tablename__ = "roles"

    id: Mapped[int] = mapped_column( Integer, primary_key=True, autoincrement=True, )

    # ID public của role.
    # Dùng để gắn với auth_user.role_id.
    role_id: Mapped[str] = mapped_column( String(64), unique=True, index=True, nullable=False, )

    # Tên role hiển thị.
    role_name: Mapped[str] = mapped_column( String(64), unique=True, index=True, nullable=False, )

    # Mô tả ngắn về role.
    description: Mapped[Optional[str]] = mapped_column( Unicode(500), nullable=True, )

    # Chuỗi JSON mô tả danh sách quyền.
    # Dùng Text để lưu linh hoạt trong SQL Server.
    permissions_json: Mapped[str] = mapped_column( Text, default="[]", nullable=False, )

    is_active: Mapped[bool] = mapped_column( Boolean, default=True, nullable=False, )

    created_at: Mapped[datetime] = mapped_column( DateTime(timezone=True), default=utc_now, nullable=False, )

    updated_at: Mapped[datetime] = mapped_column( DateTime(timezone=True), default=utc_now, onupdate=utc_now, nullable=False, )


class JobRecord(Base):
    """
    Bảng job_record lưu thông tin công việc mà người dùng gửi lên.
    """

    __tablename__ = "job_record"

    # job_id do app sinh ra, ví dụ uuid.uuid4().hex.
    # Dùng làm primary key vì mỗi job là duy nhất.
    job_id: Mapped[str] = mapped_column( String(64), primary_key=True, )

    # tenant_id là mã khách hàng/nhóm/công ty sở hữu job này.
    #
    # Ý nghĩa rất quan trọng:
    # - Nếu hệ thống chỉ phục vụ một công ty thì tenant_id có thể là một giá trị cố định.
    # - Nếu hệ thống phục vụ nhiều công ty/khách hàng, tenant_id giúp tách dữ liệu.
    # - Khi truy vấn job, nên luôn lọc theo tenant_id để tránh user tenant A xem job của tenant B.
    # - Khi worker xử lý job, tenant_id cũng giúp xác định file, cấu hình, quyền,
    #   storage bucket hoặc chính sách xử lý thuộc khách hàng nào.
    #
    # Ví dụ:
    # tenant_id = "demo"
    # tenant_id = "company_a"
    # tenant_id = "customer_001"
    tenant_id: Mapped[str] = mapped_column(
        String(64),
        index=True,
        nullable=False,
    )

    # user_id là người tạo job.
    # Liên kết logic tới users.user_id, nhưng không dùng ForeignKey.
    user_id: Mapped[str] = mapped_column( String(64), index=True, nullable=False, )

    # Tên file gốc sau khi đã sanitize ở API.
    filename: Mapped[str] = mapped_column( Unicode(255), nullable=False, )

    # Trạng thái hiện tại của job.
    # Có thể dùng các giá trị:
    # - queued
    # - processing
    # - success
    # - failed
    # - cancelled
    status: Mapped[str] = mapped_column( String(32), index=True, nullable=False, )

    # Tiến độ xử lý từ 0 đến 100.
    progress: Mapped[int] = mapped_column( Integer, default=0, nullable=False, )

    # Message ngắn để hiển thị cho client.
    # Ví dụ: "Đang đọc file", "Đang xử lý", "Hoàn thành".
    message: Mapped[str] = mapped_column( Unicode(255), default="", nullable=False, )

    # Số lần worker đã thử xử lý job.
    # Có thể tăng lên khi job bị retry.
    attempts: Mapped[int] = mapped_column( Integer, default=0, nullable=False, )

    # Object key của file upload.
    # Nếu dùng local storage: đây có thể là path tương đối.
    # Nếu dùng S3/MinIO: đây là key trong bucket.
    upload_object_key: Mapped[str] = mapped_column( Text, nullable=False, )

    # Object key của file kết quả.
    # Chỉ có giá trị khi job xử lý thành công.
    result_object_key: Mapped[Optional[str]] = mapped_column( Text, nullable=True, )

    # Nội dung lỗi nếu job thất bại.
    error: Mapped[Optional[str]] = mapped_column( Unicode(500), nullable=True, )

    # trace_id dùng để liên kết log giữa API, worker và notification.
    # Ví dụ một request tạo job có trace_id, worker xử lý cũng ghi log cùng trace_id.
    trace_id: Mapped[str] = mapped_column( String(64), index=True, nullable=False, )

    created_at: Mapped[datetime] = mapped_column( DateTime(timezone=True), default=utc_now, nullable=False, )

    updated_at: Mapped[datetime] = mapped_column( DateTime(timezone=True), default=utc_now, onupdate=utc_now, nullable=False, )

    # Thời điểm job kết thúc.
    # Null nếu job vẫn đang queued hoặc processing.
    finished_at: Mapped[Optional[datetime]] = mapped_column( DateTime(timezone=True), nullable=True, )
