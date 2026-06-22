from __future__ import annotations

from datetime import datetime
from typing import Optional

from sqlalchemy import Boolean, DateTime, Integer, String, Unicode
from sqlalchemy.orm import Mapped, mapped_column

from db.database import Base
from utils.utils import utc_now


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
