from __future__ import annotations

import uuid
from typing import Any

from sqlalchemy import exc, select
from sqlalchemy.ext.asyncio import AsyncSession

from db.models import Users, UserAuth, Roles, JobRecord
from utils.utils import utc_now
from utils.response_db import make_response, user_to_dict, auth_to_dict, role_to_dict, job_to_dict


# ============================================================
# 1. TẠO HỒ SƠ USER
# ============================================================

async def create_user_profile(
    db: AsyncSession,
    *,
    tenant_id: str,
    full_name: str | None = None,
    email: str | None = None,
    phone: str | None = None,
) -> dict[str, Any]:
    """
    Tạo hồ sơ user mới trong bảng users.

    Hàm này chỉ tạo hồ sơ người dùng.
    Không tạo username/password.
    Không tạo dữ liệu trong bảng auth_user.

    tenant_id:
    - Dùng để xác định user này thuộc nhóm/công ty/khách hàng nào.
    - Khi hệ thống có nhiều tenant, mọi truy vấn user nên lọc theo tenant_id.
    """
    response = make_response(message="Lỗi khi tạo hồ sơ người dùng")

    now = utc_now()

    user = Users(
        user_id=uuid.uuid4().hex,
        tenant_id=tenant_id,
        full_name=full_name,
        email=email,
        phone=phone,
        is_active=True,
        created_at=now,
        updated_at=now,
    )

    try:
        db.add(user)
        await db.commit()
        await db.refresh(user)

        return make_response(
            success=True,
            data=user_to_dict(user),
            message=f"Hồ sơ người dùng đã được tạo với user_id={user.user_id}",
        )

    except exc.IntegrityError as e:
        await db.rollback()
        response["message"] = f"Lỗi tạo user: user_id/email hoặc dữ liệu unique đã tồn tại ({str(e)})"

    except exc.DataError as e:
        await db.rollback()
        response["message"] = f"Lỗi dữ liệu khi tạo user: {str(e)}"

    except exc.SQLAlchemyError as e:
        await db.rollback()
        response["message"] = f"Lỗi SQLAlchemy khi tạo user: {str(e)}"

    except Exception as e:
        await db.rollback()
        response["message"] = f"Lỗi không xác định khi tạo user: {str(e)}"

    return response


# ============================================================
# 2. LẤY USER THEO user_id
# ============================================================

async def get_user_by_user_id(
    db: AsyncSession,
    *,
    tenant_id: str,
    user_id: str,
) -> dict[str, Any]:
    """
    Lấy hồ sơ user theo tenant_id và user_id.

    Vì bạn dùng mô hình multi-tenant, nên không nên chỉ lọc theo user_id.
    Nên lọc cả:
    - tenant_id
    - user_id

    Điều này tránh trường hợp lấy nhầm user ở tenant khác.
    """
    response = make_response(message="Không thể truy vấn hồ sơ người dùng")

    try:
        stmt = (
            select(Users)
            .where(
                Users.tenant_id == tenant_id,
                Users.user_id == user_id,
            )
        )

        result = await db.execute(stmt)
        user = result.scalar_one_or_none()

        if not user:
            return make_response(
                success=None,
                data=None,
                message=f"Không tìm thấy user_id={user_id} trong tenant_id={tenant_id}",
            )

        return make_response(
            success=True,
            data=user_to_dict(user),
            message=f"Tìm thấy hồ sơ user_id={user_id}",
        )

    except exc.SQLAlchemyError as e:
        response["message"] = f"Lỗi SQLAlchemy khi truy vấn user: {str(e)}"

    except Exception as e:
        response["message"] = f"Lỗi không xác định khi truy vấn user: {str(e)}"

    return response


# ============================================================
# 3. LẤY USER KÈM AUTH VÀ ROLE BẰNG JOIN THỦ CÔNG
# ============================================================

async def get_user_detail(
    db: AsyncSession,
    *,
    tenant_id: str,
    user_id: str,
) -> dict[str, Any]:
    """
    Lấy chi tiết user kèm thông tin đăng nhập và role.

    Vì model không có relationship, ta không dùng:
    - joinedload(Users.auth)
    - user.auth
    - auth.role

    Thay vào đó dùng join thủ công:

    users
        LEFT JOIN auth_user
            ON users.user_id = auth_user.user_id
           AND users.tenant_id = auth_user.tenant_id
        LEFT JOIN role
            ON auth_user.role_id = role.role_id

    Tại sao dùng LEFT JOIN?
    - Vì một user có thể đã có hồ sơ nhưng chưa có tài khoản đăng nhập.
    - Nếu dùng INNER JOIN, user chưa có auth_user sẽ không được trả về.
    """
    response = make_response(message="Không thể truy vấn chi tiết người dùng")

    try:
        stmt = (
            select(Users, UserAuth, Roles)
            .outerjoin(
                UserAuth,
                (Users.user_id == UserAuth.user_id)
                & (Users.tenant_id == UserAuth.tenant_id),
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
                message=f"Không tìm thấy user_id={user_id} trong tenant_id={tenant_id}",
            )

        user, auth, role = row

        data = {
            "user": user_to_dict(user),
            "auth": auth_to_dict(auth),
            "role": role_to_dict(role),
        }

        return make_response(
            success=True,
            data=data,
            message=f"Tìm thấy chi tiết user_id={user_id}",
        )

    except exc.SQLAlchemyError as e:
        response["message"] = f"Lỗi SQLAlchemy khi truy vấn chi tiết user: {str(e)}"

    except Exception as e:
        response["message"] = f"Lỗi không xác định khi truy vấn chi tiết user: {str(e)}"

    return response


# ============================================================
# 4. LẤY USER THEO USERNAME BẰNG JOIN THỦ CÔNG
# ============================================================

async def get_user_by_username(
    db: AsyncSession,
    *,
    tenant_id: str,
    username: str,
) -> dict[str, Any]:
    """
    Lấy user theo username.

    username nằm trong bảng auth_user,
    còn hồ sơ người dùng nằm trong bảng users.

    Do đó phải join:
    auth_user.user_id = users.user_id
    auth_user.tenant_id = users.tenant_id

    Trường hợp này dùng INNER JOIN vì:
    - Muốn tìm theo username thì bắt buộc phải có bản ghi auth_user.
    - User không có auth_user chắc chắn không tìm được bằng username.
    """
    response = make_response(message="Không thể truy vấn user theo username")

    try:
        stmt = (
            select(Users, UserAuth, Roles)
            .join(
                UserAuth,
                (Users.user_id == UserAuth.user_id)
                & (Users.tenant_id == UserAuth.tenant_id),
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

        user, auth, role = row

        data = {
            "user": user_to_dict(user),
            "auth": auth_to_dict(auth),
            "role": role_to_dict(role),
        }

        return make_response(
            success=True,
            data=data,
            message=f"Tìm thấy user có username={username}",
        )

    except exc.SQLAlchemyError as e:
        response["message"] = f"Lỗi SQLAlchemy khi truy vấn user theo username: {str(e)}"

    except Exception as e:
        response["message"] = f"Lỗi không xác định khi truy vấn user theo username: {str(e)}"

    return response


# ============================================================
# 5. LẤY DANH SÁCH USERS
# ============================================================

async def get_all_users(
    db: AsyncSession,
    *,
    tenant_id: str | None = None,
    include_inactive: bool = True,
    limit: int = 1000,
) -> dict[str, Any]:
    """
    Lấy danh sách user.

    tenant_id:
    - Nếu truyền tenant_id, chỉ lấy user trong tenant đó.
    - Nếu None, lấy user của toàn hệ thống.

    include_inactive:
    - True: lấy cả user đang bị khóa.
    - False: chỉ lấy user đang hoạt động.

    limit:
    - Giới hạn số lượng bản ghi để tránh truy vấn quá lớn.
    """
    response = make_response(message="Không thể truy vấn danh sách người dùng")

    try:
        stmt = select(Users)

        if tenant_id:
            stmt = stmt.where(Users.tenant_id == tenant_id)

        if not include_inactive:
            stmt = stmt.where(Users.is_active.is_(True))

        stmt = (
            stmt
            .order_by(Users.created_at.desc())
            .limit(limit)
        )

        result = await db.execute(stmt)
        users = list(result.scalars().all())

        if not users:
            return make_response(
                success=None,
                data=[],
                message="Chưa có dữ liệu người dùng",
            )

        return make_response(
            success=True,
            data=[user_to_dict(user) for user in users],
            message="Tìm thấy danh sách người dùng",
        )

    except exc.SQLAlchemyError as e:
        response["message"] = f"Lỗi SQLAlchemy khi truy vấn danh sách người dùng: {str(e)}"

    except Exception as e:
        response["message"] = f"Lỗi không xác định khi truy vấn danh sách người dùng: {str(e)}"

    return response


# ============================================================
# 6. LẤY DANH SÁCH USERS KÈM AUTH VÀ ROLE
# ============================================================

async def get_all_users_with_auth(
    db: AsyncSession,
    *,
    tenant_id: str | None = None,
    include_inactive: bool = True,
    limit: int = 1000,
) -> dict[str, Any]:
    """
    Lấy danh sách user kèm auth_user và role.

    Vì không dùng relationship nên không thể dùng:
    .options(joinedload(Users.auth))

    Thay vào đó dùng select(Users, AuthUser, Role).

    Mỗi dòng kết quả trả về là tuple:
    (user, auth, role)

    Dùng LEFT JOIN để:
    - user chưa có tài khoản đăng nhập vẫn xuất hiện
    - auth chưa được gán role vẫn xuất hiện
    """
    response = make_response(message="Không thể truy vấn danh sách user kèm auth")

    try:
        stmt = (
            select(Users, UserAuth, Roles)
            .outerjoin(
                UserAuth,
                (Users.user_id == UserAuth.user_id)
                & (Users.tenant_id == UserAuth.tenant_id),
            )
            .outerjoin(
                Roles,
                UserAuth.role_id == Roles.role_id,
            )
        )

        if tenant_id:
            stmt = stmt.where(Users.tenant_id == tenant_id)

        if not include_inactive:
            stmt = stmt.where(Users.is_active.is_(True))

        stmt = (
            stmt
            .order_by(Users.created_at.desc())
            .limit(limit)
        )

        result = await db.execute(stmt)
        rows = result.all()

        if not rows:
            return make_response(
                success=None,
                data=[],
                message="Chưa có dữ liệu người dùng",
            )

        data = []

        for user, auth, role in rows:
            data.append(
                {
                    "user": user_to_dict(user),
                    "auth": auth_to_dict(auth),
                    "role": role_to_dict(role),
                }
            )

        return make_response(
            success=True,
            data=data,
            message="Tìm thấy danh sách user kèm auth và role",
        )

    except exc.SQLAlchemyError as e:
        response["message"] = f"Lỗi SQLAlchemy khi join danh sách user: {str(e)}"

    except Exception as e:
        response["message"] = f"Lỗi không xác định khi join danh sách user: {str(e)}"

    return response


# ============================================================
# 7. LẤY DANH SÁCH JOB CỦA USER
# ============================================================

async def get_user_jobs(
    db: AsyncSession,
    *,
    tenant_id: str,
    user_id: str,
    status: str | None = None,
    limit: int = 100,
) -> dict[str, Any]:
    """
    Lấy danh sách job do user tạo.

    Có 2 cách làm:

    Cách đơn giản:
    - Query trực tiếp bảng job_record theo tenant_id và user_id.

    Cách chắc chắn hơn:
    - Join với users để đảm bảo user tồn tại.

    Ở đây dùng join với users để:
    - Xác nhận job thuộc đúng user trong đúng tenant.
    - Tránh lấy dữ liệu mồ côi nếu trước đó user bị xóa cứng.
    """
    response = make_response(message="Không thể truy vấn danh sách job của user")

    try:
        stmt = (
            select(JobRecord)
            .join(
                Users,
                (Users.user_id == JobRecord.user_id)
                & (Users.tenant_id == JobRecord.tenant_id),
            )
            .where(
                Users.tenant_id == tenant_id,
                Users.user_id == user_id,
            )
        )

        if status:
            stmt = stmt.where(JobRecord.status == status)

        stmt = (
            stmt
            .order_by(JobRecord.created_at.desc())
            .limit(limit)
        )

        result = await db.execute(stmt)
        jobs = list(result.scalars().all())

        if not jobs:
            return make_response(
                success=None,
                data=[],
                message=f"User user_id={user_id} chưa có job nào",
            )

        return make_response(
            success=True,
            data=[job_to_dict(job) for job in jobs],
            message=f"Tìm thấy danh sách job của user_id={user_id}",
        )

    except exc.SQLAlchemyError as e:
        response["message"] = f"Lỗi SQLAlchemy khi truy vấn job của user: {str(e)}"

    except Exception as e:
        response["message"] = f"Lỗi không xác định khi truy vấn job của user: {str(e)}"

    return response


# ============================================================
# 8. LẤY CHI TIẾT USER KÈM SỐ LƯỢNG JOB
# ============================================================

async def get_user_summary(
    db: AsyncSession,
    *,
    tenant_id: str,
    user_id: str,
) -> dict[str, Any]:
    """
    Lấy thông tin tóm tắt của user.

    Hàm này minh họa cách lấy user, auth, role và danh sách job
    khi không dùng relationship.

    Cách làm:
    1. Query user + auth + role bằng join thủ công.
    2. Query job riêng bằng bảng job_record.

    Không nên ép tất cả vào một query lớn nếu không cần,
    vì user có thể có nhiều job, join một-nhiều sẽ làm lặp dữ liệu user.
    """
    response = make_response(message="Không thể truy vấn tóm tắt user")

    try:
        user_stmt = (
            select(Users, UserAuth, Roles)
            .outerjoin(
                UserAuth,
                (Users.user_id == UserAuth.user_id)
                & (Users.tenant_id == UserAuth.tenant_id),
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

        user_result = await db.execute(user_stmt)
        row = user_result.first()

        if not row:
            return make_response(
                success=None,
                data=None,
                message=f"Không tìm thấy user_id={user_id} trong tenant_id={tenant_id}",
            )

        user, auth, role = row

        job_stmt = (
            select(JobRecord)
            .where(
                JobRecord.tenant_id == tenant_id,
                JobRecord.user_id == user_id,
            )
            .order_by(JobRecord.created_at.desc())
        )

        job_result = await db.execute(job_stmt)
        jobs = list(job_result.scalars().all())

        data = {
            "user": user_to_dict(user),
            "auth": auth_to_dict(auth),
            "role": role_to_dict(role),
            "job_count": len(jobs),
            "jobs": [job_to_dict(job) for job in jobs],
        }

        return make_response(
            success=True,
            data=data,
            message=f"Tìm thấy thông tin tóm tắt user_id={user_id}",
        )

    except exc.SQLAlchemyError as e:
        response["message"] = f"Lỗi SQLAlchemy khi truy vấn tóm tắt user: {str(e)}"

    except Exception as e:
        response["message"] = f"Lỗi không xác định khi truy vấn tóm tắt user: {str(e)}"

    return response


# ============================================================
# 9. CẬP NHẬT HỒ SƠ USER
# ============================================================

async def update_user_profile(
    db: AsyncSession,
    *,
    tenant_id: str,
    user_id: str,
    full_name: str | None = None,
    email: str | None = None,
    phone: str | None = None,
) -> dict[str, Any]:
    """
    Cập nhật hồ sơ user.

    Chỉ cập nhật field được truyền vào khác None.

    Lưu ý:
    - Luôn lọc theo tenant_id và user_id.
    - Không cập nhật auth_user ở đây.
    - Không cập nhật role ở đây.
    """
    response = make_response(message="Lỗi khi cập nhật hồ sơ người dùng")

    try:
        result = await db.execute(
            select(Users).where(
                Users.tenant_id == tenant_id,
                Users.user_id == user_id,
            )
        )

        user = result.scalar_one_or_none()

        if not user:
            return make_response(
                success=None,
                data=None,
                message=f"Không tìm thấy user_id={user_id} trong tenant_id={tenant_id}",
            )

        if full_name is not None:
            user.full_name = full_name

        if email is not None:
            user.email = email

        if phone is not None:
            user.phone = phone

        user.updated_at = utc_now()

        await db.commit()
        await db.refresh(user)

        return make_response(
            success=True,
            data=user_to_dict(user),
            message=f"Hồ sơ user_id={user_id} đã được cập nhật",
        )

    except exc.IntegrityError as e:
        await db.rollback()
        response["message"] = f"Lỗi ràng buộc dữ liệu khi cập nhật user: {str(e)}"

    except exc.SQLAlchemyError as e:
        await db.rollback()
        response["message"] = f"Lỗi SQLAlchemy khi cập nhật user: {str(e)}"

    except Exception as e:
        await db.rollback()
        response["message"] = f"Lỗi không xác định khi cập nhật user: {str(e)}"

    return response


# ============================================================
# 10. KHÓA / MỞ KHÓA USER
# ============================================================

async def activate_user_profile(
    db: AsyncSession,
    *,
    tenant_id: str,
    user_id: str,
    activate: bool,
) -> dict[str, Any]:
    """
    Kích hoạt hoặc khóa hồ sơ user.

    is_active=False:
    - User bị khóa ở cấp hồ sơ.
    - Có thể chặn tạo job mới.
    - Có thể chặn sử dụng chức năng hệ thống.

    Khác với auth_user.is_login_enabled:
    - is_login_enabled=False chỉ khóa đăng nhập.
    - users.is_active=False nên hiểu là khóa user ở cấp nghiệp vụ rộng hơn.
    """
    response = make_response(message="Lỗi khi cập nhật trạng thái user")

    try:
        result = await db.execute(
            select(Users).where(
                Users.tenant_id == tenant_id,
                Users.user_id == user_id,
            )
        )

        user = result.scalar_one_or_none()

        if not user:
            return make_response(
                success=None,
                data=None,
                message=f"Không tìm thấy user_id={user_id} trong tenant_id={tenant_id}",
            )

        user.is_active = activate
        user.updated_at = utc_now()

        await db.commit()
        await db.refresh(user)

        return make_response(
            success=True,
            data=user_to_dict(user),
            message=f"User user_id={user_id} đã được cập nhật is_active={activate}",
        )

    except exc.SQLAlchemyError as e:
        await db.rollback()
        response["message"] = f"Lỗi SQLAlchemy khi cập nhật trạng thái user: {str(e)}"

    except Exception as e:
        await db.rollback()
        response["message"] = f"Lỗi không xác định khi cập nhật trạng thái user: {str(e)}"

    return response


# ============================================================
# 11. XÓA USER
# ============================================================

async def delete_user_profile(
    db: AsyncSession,
    *,
    tenant_id: str,
    user_id: str,
    hard_delete: bool = False,
) -> dict[str, Any]:
    """
    Xóa hồ sơ người dùng.

    Vì model không dùng ForeignKey và không dùng relationship,
    nên khi xóa Users, SQL Server sẽ KHÔNG tự xóa:
    - auth_user
    - job_record

    Do đó nên ưu tiên soft delete:
    - hard_delete=False
    - chỉ set is_active=False

    hard_delete=True:
    - xóa thật dòng trong bảng users
    - có nguy cơ để lại dữ liệu mồ côi ở auth_user, job_record
    - chỉ nên dùng khi bạn đã tự xử lý xóa dữ liệu liên quan
    """
    response = make_response(message="Lỗi khi xóa hồ sơ người dùng")

    try:
        result = await db.execute(
            select(Users).where(
                Users.tenant_id == tenant_id,
                Users.user_id == user_id,
            )
        )

        user = result.scalar_one_or_none()

        if not user:
            return make_response(
                success=None,
                data=None,
                message=f"Không tìm thấy user_id={user_id} trong tenant_id={tenant_id}",
            )

        if not hard_delete:
            user.is_active = False
            user.updated_at = utc_now()

            await db.commit()
            await db.refresh(user)

            return make_response(
                success=True,
                data=user_to_dict(user),
                message=f"Đã khóa mềm user_id={user_id}",
            )

        await db.delete(user)
        await db.commit()

        return make_response(
            success=True,
            data=None,
            message=f"Đã xóa cứng user_id={user_id}",
        )

    except exc.SQLAlchemyError as e:
        await db.rollback()
        response["message"] = f"Lỗi SQLAlchemy khi xóa user: {str(e)}"

    except Exception as e:
        await db.rollback()
        response["message"] = f"Lỗi không xác định khi xóa user: {str(e)}"

    return response


# ============================================================
# 12. KIỂM TRA USER CÓ TỒN TẠI KHÔNG
# ============================================================

async def check_user_exists(
    db: AsyncSession,
    *,
    tenant_id: str,
    user_id: str,
    only_active: bool = True,
) -> dict[str, Any]:
    """
    Kiểm tra user có tồn tại hay không.

    Hàm này rất hữu ích trước khi:
    - tạo auth_user
    - tạo job_record
    - cho phép user upload file
    - cho phép user gọi API nghiệp vụ

    Vì không có ForeignKey, app phải tự kiểm tra user tồn tại.
    """
    response = make_response(message="Không thể kiểm tra user")

    try:
        stmt = select(Users).where(
            Users.tenant_id == tenant_id,
            Users.user_id == user_id,
        )

        if only_active:
            stmt = stmt.where(Users.is_active.is_(True))

        result = await db.execute(stmt)
        user = result.scalar_one_or_none()

        if not user:
            return make_response(
                success=None,
                data={"exists": False},
                message=f"User user_id={user_id} không tồn tại hoặc không hoạt động",
            )

        return make_response(
            success=True,
            data={
                "exists": True,
                "user": user_to_dict(user),
            },
            message=f"User user_id={user_id} tồn tại",
        )

    except exc.SQLAlchemyError as e:
        response["message"] = f"Lỗi SQLAlchemy khi kiểm tra user: {str(e)}"

    except Exception as e:
        response["message"] = f"Lỗi không xác định khi kiểm tra user: {str(e)}"

    return response