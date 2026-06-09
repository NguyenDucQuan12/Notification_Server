from __future__ import annotations

from typing import Any

from sqlalchemy import and_, exc, select
from sqlalchemy.ext.asyncio import AsyncSession

from db.models import Users, UserAuth, Roles
from utils.utils import utc_now
from utils.response_db import make_response, user_to_dict, role_to_dict, auth_to_dict


async def create_user_auth(
    db: AsyncSession,
    *,
    tenant_id: str,
    user_id: str,
    username: str,
    password_hash: str,
    role_id: str | None = None,
    is_superuser: bool = False,
) -> dict[str, Any]:
    """
    Tạo tài khoản đăng nhập cho user.

    Vì không dùng ForeignKey, hàm này phải tự kiểm tra:
    1. User có tồn tại trong tenant không.
    2. User có đang active không.
    3. User đã có auth chưa.
    4. Username đã tồn tại trong tenant chưa.
    5. role_id có tồn tại và đang active không.
    """
    response = make_response(message="Lỗi khi tạo tài khoản đăng nhập")

    try:
        user_result = await db.execute(
            select(Users).where(
                Users.tenant_id == tenant_id,
                Users.user_id == user_id,
                Users.is_active == True,
            )
        )
        user = user_result.scalar_one_or_none()

        if not user:
            return make_response(
                success=False,
                data=None,
                message=f"Không thể tạo auth vì user_id={user_id} không tồn tại hoặc đã bị khóa",
            )

        old_auth_result = await db.execute(
            select(UserAuth).where(
                UserAuth.user_id == user_id,
            )
        )
        old_auth = old_auth_result.scalar_one_or_none()

        if old_auth:
            return make_response(
                success=False,
                data=auth_to_dict(old_auth),
                message=f"User user_id={user_id} đã có tài khoản đăng nhập",
            )

        username_result = await db.execute(
            select(UserAuth).where(
                UserAuth.username == username,
            )
        )
        username_exists = username_result.scalar_one_or_none()

        if username_exists:
            return make_response(
                success=False,
                data=None,
                message=f"Username={username} đã tồn tại",
            )

        if role_id is not None:
            role_result = await db.execute(
                select(Roles).where(
                    Roles.role_id == role_id,
                    Roles.is_active == True,
                )
            )
            role = role_result.scalar_one_or_none()

            if not role:
                return make_response(
                    success=False,
                    data=None,
                    message=f"Không thể gán role_id={role_id} vì role không tồn tại hoặc đã bị khóa",
                )

        now = utc_now()

        auth = UserAuth(
            user_id=user_id,
            tenant_id = tenant_id,
            role_id=role_id,
            username=username,
            password_hash=password_hash,
            is_superuser=is_superuser,
            is_login_enabled=True,
            failed_login_count=0,
            last_login_at=None,
            password_changed_at=now,
            created_at=now,
            updated_at=now,
        )

        db.add(auth)
        await db.commit()
        await db.refresh(auth)

        return make_response(
            success=True,
            data=auth_to_dict(auth),
            message=f"Đã tạo tài khoản đăng nhập cho user_id={user_id}",
        )

    except exc.IntegrityError as e:
        await db.rollback()
        response["message"] = f"Lỗi ràng buộc dữ liệu khi tạo user_auth: {str(e)}"

    except exc.DataError as e:
        await db.rollback()
        response["message"] = f"Lỗi dữ liệu khi tạo user_auth: {str(e)}"

    except exc.SQLAlchemyError as e:
        await db.rollback()
        response["message"] = f"Lỗi SQLAlchemy khi tạo user_auth: {str(e)}"

    except Exception as e:
        await db.rollback()
        response["message"] = f"Lỗi không xác định khi tạo user_auth: {str(e)}"

    return response


async def get_auth_by_user_id(
    db: AsyncSession,
    *,
    tenant_id: str,
    user_id: str,
    include_user: bool = False,
    include_role: bool = False,
) -> dict[str, Any]:
    """
    Lấy thông tin auth theo user_id.

    Vì bảng UserAuth không có tenant_id trong model ban đầu của bạn,
    khi cần kiểm tra tenant thì phải join sang bảng Users.

    Nếu không include_user và không include_role:
    - Vẫn join Users để đảm bảo user thuộc đúng tenant_id.

    Điều kiện join:
    UserAuth.user_id == Users.user_id
    """
    response = make_response(message="Không thể truy vấn user_auth")

    try:
        stmt = (
            select(UserAuth, Users, Roles)
            .join(
                Users,
                UserAuth.user_id == Users.user_id,
            )
            .outerjoin(
                Roles,
                UserAuth.role_id == Roles.role_id,
            )
            .where(
                Users.tenant_id == tenant_id,
                Users.user_id == user_id,
            )
        )

        result = await db.execute(stmt)
        row = result.first()

        if not row:
            return make_response(
                success=None,
                data=None,
                message=f"Không tìm thấy user_auth của user_id={user_id}",
            )

        auth, user, role = row

        data = {
            "auth": auth_to_dict(auth),
        }

        if include_user:
            data["user"] = user_to_dict(user)

        if include_role:
            data["role"] = role_to_dict(role)

        return make_response(
            success=True,
            data=data,
            message=f"Tìm thấy user_auth của user_id={user_id}",
        )

    except exc.SQLAlchemyError as e:
        response["message"] = f"Lỗi SQLAlchemy khi truy vấn user_auth: {str(e)}"

    except Exception as e:
        response["message"] = f"Lỗi không xác định khi truy vấn user_auth: {str(e)}"

    return response


async def get_auth_by_username(
    db: AsyncSession,
    *,
    tenant_id: str,
    username: str,
    include_user: bool = True,
    include_role: bool = True,
) -> dict[str, Any]:
    """
    Lấy tài khoản đăng nhập theo username.

    Hàm này thường dùng khi login.

    Vì UserAuth không có tenant_id trong model ban đầu,
    phải join sang Users để kiểm tra tenant_id.

    Chỉ những auth có user thuộc đúng tenant_id mới được trả về.
    """
    response = make_response(message="Không thể truy vấn user_auth theo username")

    try:
        stmt = (
            select(UserAuth, Users, Roles)
            .join(
                Users,
                UserAuth.user_id == Users.user_id,
            )
            .outerjoin(
                Roles,
                UserAuth.role_id == Roles.role_id,
            )
            .where(
                Users.tenant_id == tenant_id,
                UserAuth.username == username,
            )
        )

        result = await db.execute(stmt)
        row = result.first()

        if not row:
            return make_response(
                success=None,
                data=None,
                message=f"Không tìm thấy username={username} trong tenant_id={tenant_id}",
            )

        auth, user, role = row

        data = {
            "auth": auth_to_dict(auth),
        }

        if include_user:
            data["user"] = user_to_dict(user)

        if include_role:
            data["role"] = role_to_dict(role)

        return make_response(
            success=True,
            data=data,
            message=f"Tìm thấy user_auth username={username}",
        )

    except exc.SQLAlchemyError as e:
        response["message"] = f"Lỗi SQLAlchemy khi truy vấn username: {str(e)}"

    except Exception as e:
        response["message"] = f"Lỗi không xác định khi truy vấn username: {str(e)}"

    return response


async def get_auth_for_login(
    db: AsyncSession,
    *,
    tenant_id: str,
    username: str,
) -> dict[str, Any]:
    """
    Lấy dữ liệu phục vụ login.

    Hàm này trả thêm password_hash để service login kiểm tra mật khẩu.

    Lưu ý:
    - Chỉ dùng nội bộ.
    - Không trả response này trực tiếp ra client.
    - Client không bao giờ được thấy password_hash.
    """
    response = make_response(message="Không thể lấy dữ liệu đăng nhập")

    try:
        stmt = (
            select(UserAuth, Users, Roles)
            .join(
                Users,
                UserAuth.user_id == Users.user_id,
            )
            .outerjoin(
                Roles,
                UserAuth.role_id == Roles.role_id,
            )
            .where(
                Users.tenant_id == tenant_id,
                UserAuth.username == username,
            )
        )

        result = await db.execute(stmt)
        row = result.first()

        if not row:
            return make_response(
                success=None,
                data=None,
                message="Sai tài khoản hoặc mật khẩu",
            )

        auth, user, role = row

        data = {
            "auth": {
                **auth_to_dict(auth),
                "password_hash": auth.password_hash,
            },
            "user": user_to_dict(user),
            "role": role_to_dict(role),
        }

        return make_response(
            success=True,
            data=data,
            message="Tìm thấy dữ liệu đăng nhập",
        )

    except exc.SQLAlchemyError as e:
        response["message"] = f"Lỗi SQLAlchemy khi lấy dữ liệu login: {str(e)}"

    except Exception as e:
        response["message"] = f"Lỗi không xác định khi lấy dữ liệu login: {str(e)}"

    return response


async def update_user_auth(
    db: AsyncSession,
    *,
    tenant_id: str,
    user_id: str,
    username: str | None = None,
    role_id: str | None = None,
    is_superuser: bool | None = None,
    is_login_enabled: bool | None = None,
) -> dict[str, Any]:
    """
    Cập nhật thông tin auth.

    Không đổi password ở hàm này.
    Đổi mật khẩu nên dùng hàm change_password_hash riêng.

    Vì UserAuth không có tenant_id trong model ban đầu,
    muốn cập nhật đúng user trong đúng tenant thì phải:
    - join UserAuth với Users
    - lọc Users.tenant_id
    - lọc Users.user_id
    """
    response = make_response(message="Lỗi khi cập nhật user_auth")

    try:
        result = await db.execute(
            select(UserAuth)
            .join(
                Users,
                UserAuth.user_id == Users.user_id,
            )
            .where(
                Users.tenant_id == tenant_id,
                Users.user_id == user_id,
            )
        )
        auth = result.scalar_one_or_none()

        if not auth:
            return make_response(
                success=None,
                data=None,
                message=f"Không tìm thấy user_auth của user_id={user_id}",
            )

        if username is not None and username != auth.username:
            username_result = await db.execute(
                select(UserAuth).where(
                    UserAuth.username == username,
                    UserAuth.user_id != user_id,
                )
            )
            username_exists = username_result.scalar_one_or_none()

            if username_exists:
                return make_response(
                    success=False,
                    data=None,
                    message=f"Username={username} đã tồn tại",
                )

            auth.username = username

        if role_id is not None:
            role_result = await db.execute(
                select(Roles).where(
                    Roles.role_id == role_id,
                    Roles.is_active == True,
                )
            )
            role = role_result.scalar_one_or_none()

            if not role:
                return make_response(
                    success=False,
                    data=None,
                    message=f"Không thể gán role_id={role_id} vì role không tồn tại hoặc đã bị khóa",
                )

            auth.role_id = role_id

        if is_superuser is not None:
            auth.is_superuser = is_superuser

        if is_login_enabled is not None:
            auth.is_login_enabled = is_login_enabled

        auth.updated_at = utc_now()

        await db.commit()
        await db.refresh(auth)

        return make_response(
            success=True,
            data=auth_to_dict(auth),
            message=f"Đã cập nhật user_auth của user_id={user_id}",
        )

    except exc.IntegrityError as e:
        await db.rollback()
        response["message"] = f"Lỗi ràng buộc dữ liệu khi cập nhật user_auth: {str(e)}"

    except exc.SQLAlchemyError as e:
        await db.rollback()
        response["message"] = f"Lỗi SQLAlchemy khi cập nhật user_auth: {str(e)}"

    except Exception as e:
        await db.rollback()
        response["message"] = f"Lỗi không xác định khi cập nhật user_auth: {str(e)}"

    return response


async def change_password_hash(
    db: AsyncSession,
    *,
    tenant_id: str,
    user_id: str,
    new_password_hash: str,
) -> dict[str, Any]:
    """
    Đổi mật khẩu.

    Hàm này nhận password đã hash.
    Không truyền mật khẩu gốc vào đây.

    Vì không dùng tenant_id trong UserAuth,
    phải join sang Users để đảm bảo đúng tenant.
    """
    response = make_response(message="Lỗi khi đổi mật khẩu")

    try:
        result = await db.execute(
            select(UserAuth)
            .join(
                Users,
                UserAuth.user_id == Users.user_id,
            )
            .where(
                Users.tenant_id == tenant_id,
                Users.user_id == user_id,
            )
        )
        auth = result.scalar_one_or_none()

        if not auth:
            return make_response(
                success=None,
                data=None,
                message=f"Không tìm thấy user_auth của user_id={user_id}",
            )

        now = utc_now()

        auth.password_hash = new_password_hash
        auth.password_changed_at = now
        auth.updated_at = now

        await db.commit()
        await db.refresh(auth)

        return make_response(
            success=True,
            data=auth_to_dict(auth),
            message=f"Đã đổi mật khẩu cho user_id={user_id}",
        )

    except exc.SQLAlchemyError as e:
        await db.rollback()
        response["message"] = f"Lỗi SQLAlchemy khi đổi mật khẩu: {str(e)}"

    except Exception as e:
        await db.rollback()
        response["message"] = f"Lỗi không xác định khi đổi mật khẩu: {str(e)}"

    return response


async def record_login_success(
    db: AsyncSession,
    *,
    tenant_id: str,
    username: str,
) -> dict[str, Any]:
    """
    Ghi nhận đăng nhập thành công.

    Khi login thành công:
    - reset failed_login_count = 0
    - cập nhật last_login_at
    """
    response = make_response(message="Lỗi khi ghi nhận login thành công")

    try:
        result = await db.execute(
            select(UserAuth)
            .join(
                Users,
                UserAuth.user_id == Users.user_id,
            )
            .where(
                Users.tenant_id == tenant_id,
                UserAuth.username == username,
            )
        )
        auth = result.scalar_one_or_none()

        if not auth:
            return make_response(
                success=None,
                data=None,
                message=f"Không tìm thấy username={username}",
            )

        now = utc_now()

        auth.failed_login_count = 0
        auth.last_login_at = now
        auth.updated_at = now

        await db.commit()
        await db.refresh(auth)

        return make_response(
            success=True,
            data=auth_to_dict(auth),
            message=f"Đã ghi nhận login thành công cho username={username}",
        )

    except exc.SQLAlchemyError as e:
        await db.rollback()
        response["message"] = f"Lỗi SQLAlchemy khi ghi nhận login thành công: {str(e)}"

    except Exception as e:
        await db.rollback()
        response["message"] = f"Lỗi không xác định khi ghi nhận login thành công: {str(e)}"

    return response


async def record_login_failed(
    db: AsyncSession,
    *,
    tenant_id: str,
    username: str,
) -> dict[str, Any]:
    """
    Ghi nhận đăng nhập thất bại.

    Nếu username tồn tại trong tenant:
    - tăng failed_login_count thêm 1.

    Nếu username không tồn tại:
    - không nên báo quá chi tiết ra client trong API login thực tế,
      vì sẽ làm lộ username nào có tồn tại.
    """
    response = make_response(message="Lỗi khi ghi nhận login thất bại")

    try:
        result = await db.execute(
            select(UserAuth)
            .join(
                Users,
                UserAuth.user_id == Users.user_id,
            )
            .where(
                Users.tenant_id == tenant_id,
                UserAuth.username == username,
            )
        )
        auth = result.scalar_one_or_none()

        if not auth:
            return make_response(
                success=None,
                data=None,
                message=f"Không tìm thấy username={username} để ghi nhận login thất bại",
            )

        auth.failed_login_count += 1
        auth.updated_at = utc_now()

        await db.commit()
        await db.refresh(auth)

        return make_response(
            success=True,
            data=auth_to_dict(auth),
            message=f"Đã tăng failed_login_count cho username={username}",
        )

    except exc.SQLAlchemyError as e:
        await db.rollback()
        response["message"] = f"Lỗi SQLAlchemy khi ghi nhận login thất bại: {str(e)}"

    except Exception as e:
        await db.rollback()
        response["message"] = f"Lỗi không xác định khi ghi nhận login thất bại: {str(e)}"

    return response


async def delete_user_auth(
    db: AsyncSession,
    *,
    tenant_id: str,
    user_id: str,
) -> dict[str, Any]:
    """
    Xóa tài khoản đăng nhập của user.

    Vì không dùng ForeignKey:
    - Xóa UserAuth không xóa Users.
    - Hồ sơ user vẫn còn.
    - Chỉ mất thông tin đăng nhập.
    """
    response = make_response(message="Lỗi khi xóa user_auth")

    try:
        result = await db.execute(
            select(UserAuth)
            .join(
                Users,
                UserAuth.user_id == Users.user_id,
            )
            .where(
                Users.tenant_id == tenant_id,
                Users.user_id == user_id,
            )
        )
        auth = result.scalar_one_or_none()

        if not auth:
            return make_response(
                success=None,
                data=None,
                message=f"Không tìm thấy user_auth của user_id={user_id}",
            )

        await db.delete(auth)
        await db.commit()

        return make_response(
            success=True,
            data=None,
            message=f"Đã xóa user_auth của user_id={user_id}",
        )

    except exc.SQLAlchemyError as e:
        await db.rollback()
        response["message"] = f"Lỗi SQLAlchemy khi xóa user_auth: {str(e)}"

    except Exception as e:
        await db.rollback()
        response["message"] = f"Lỗi không xác định khi xóa user_auth: {str(e)}"

    return response