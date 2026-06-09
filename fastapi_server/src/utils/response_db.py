from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from db.models import Users, UserAuth, Roles, JobRecord


def utc_now() -> datetime:
    """
    Lấy thời gian hiện tại theo UTC.

    Nên dùng UTC cho toàn hệ thống vì:
    - API có thể chạy ở một máy chủ khác múi giờ.
    - Worker có thể chạy ở máy khác.
    - SQL Server có thể lưu thời gian khác timezone.
    - UTC giúp dữ liệu thời gian thống nhất hơn.
    """
    return datetime.now(timezone.utc)


def make_response(
    *,
    success: bool | None = False,
    data: Any = None,
    message: str = "",
) -> dict[str, Any]:
    """
    Hàm tạo response thống nhất cho toàn bộ query.

    success:
    - True: thao tác thành công
    - False: thao tác thất bại do lỗi
    - None: không lỗi kỹ thuật nhưng không tìm thấy dữ liệu

    data:
    - dữ liệu trả về

    message:
    - thông báo cho người dùng hoặc developer biết kết quả
    """
    return {
        "success": success,
        "data": data,
        "message": message,
    }


def user_to_dict(user: Users | None) -> dict[str, Any] | None:
    """
    Chuyển object Users thành dict.

    Không nên trả trực tiếp object ORM ra API vì:
    - ORM object không serialize JSON tốt.
    - Dễ trả thừa dữ liệu.
    - Khó kiểm soát field nào được phép hiển thị.
    """
    if user is None:
        return None

    return {
        "id": user.id,
        "user_id": user.user_id,
        "tenant_id": user.tenant_id,
        "full_name": user.full_name,
        "email": user.email,
        "phone": user.phone,
        "is_active": user.is_active,
        "created_at": user.created_at,
        "updated_at": user.updated_at,
    }


def auth_to_dict(auth: UserAuth | None) -> dict[str, Any] | None:
    """
    Chuyển object UserAuth thành dict.

    Lưu ý:
    - Không trả password_hash ra client.
    - password_hash là dữ liệu nhạy cảm.
    - Nếu cần password_hash để login thì viết hàm riêng dùng nội bộ.
    """
    if auth is None:
        return None

    return {
        "id": auth.id,
        "user_id": auth.user_id,
        "role_id": auth.role_id,
        "username": auth.username,
        "is_superuser": auth.is_superuser,
        "is_login_enabled": auth.is_login_enabled,
        "failed_login_count": auth.failed_login_count,
        "last_login_at": auth.last_login_at,
        "password_changed_at": auth.password_changed_at,
        "created_at": auth.created_at,
        "updated_at": auth.updated_at,
    }


def role_to_dict(role: Roles | None) -> dict[str, Any] | None:
    """
    Chuyển object Roles thành dict.
    """
    if role is None:
        return None

    return {
        "id": role.id,
        "role_id": role.role_id,
        "role_name": role.role_name,
        "description": role.description,
        "permissions_json": role.permissions_json,
        "is_active": role.is_active,
        "created_at": role.created_at,
        "updated_at": role.updated_at,
    }


def job_to_dict(job: JobRecord | None) -> dict[str, Any] | None:
    """
    Chuyển object JobRecord thành dict.
    """
    if job is None:
        return None

    return {
        "job_id": job.job_id,
        "tenant_id": job.tenant_id,
        "user_id": job.user_id,
        "filename": job.filename,
        "status": job.status,
        "progress": job.progress,
        "message": job.message,
        "attempts": job.attempts,
        "upload_object_key": job.upload_object_key,
        "result_object_key": job.result_object_key,
        "error": job.error,
        "trace_id": job.trace_id,
        "created_at": job.created_at,
        "updated_at": job.updated_at,
        "finished_at": job.finished_at,
    }