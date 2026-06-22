from __future__ import annotations

from datetime import datetime
from typing import Optional

from sqlalchemy import Boolean, DateTime, Integer, String, Text, Unicode
from sqlalchemy.orm import Mapped, mapped_column

from db.database import Base
from utils.utils import utc_now



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