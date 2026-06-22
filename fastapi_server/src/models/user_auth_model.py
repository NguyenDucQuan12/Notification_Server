from __future__ import annotations

from datetime import datetime
from typing import Optional

from sqlalchemy import Boolean, DateTime, Integer, String
from sqlalchemy.orm import Mapped, mapped_column

from db.database import Base
from utils.utils import utc_now



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
