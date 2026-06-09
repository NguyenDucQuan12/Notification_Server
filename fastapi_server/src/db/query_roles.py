from __future__ import annotations

import uuid
from typing import Any

from sqlalchemy import exc, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from db.models import Roles, UserAuth
from utils.response_db import make_response, role_to_dict, utc_now


async def create_role(
    db: AsyncSession,
    *,
    role_name: str,
    role_id: str | None = None,
    description: str | None = None,
    permissions_json: str = "[]",
) -> dict[str, Any]:
    """
    Tạo role mới.

    role_id:
    - Là mã định danh public của role.
    - Dùng để gắn vào UserAuth.role_id.
    - Vì không dùng ForeignKey, SQL Server không tự kiểm tra role_id.
    - Do đó, khi gán role cho user, code phải tự kiểm tra role có tồn tại không.

    permissions_json:
    - Lưu danh sách quyền dạng chuỗi JSON.
    - Ví dụ:
      '["job:create", "job:read", "user:manage"]'
    """
    response = make_response(message="Lỗi khi tạo role")

    now = utc_now()

    role = Roles(
        role_id=role_id or uuid.uuid4().hex,
        role_name=role_name,
        description=description,
        permissions_json=permissions_json,
        is_active=True,
        created_at=now,
        updated_at=now,
    )

    try:
        db.add(role)
        await db.commit()
        await db.refresh(role)

        return make_response(
            success=True,
            data=role_to_dict(role),
            message=f"Đã tạo role role_id={role.role_id}",
        )

    except exc.IntegrityError as e:
        await db.rollback()
        response["message"] = f"Lỗi tạo role: role_id hoặc role_name đã tồn tại ({str(e)})"

    except exc.DataError as e:
        await db.rollback()
        response["message"] = f"Lỗi dữ liệu khi tạo role: {str(e)}"

    except exc.SQLAlchemyError as e:
        await db.rollback()
        response["message"] = f"Lỗi SQLAlchemy khi tạo role: {str(e)}"

    except Exception as e:
        await db.rollback()
        response["message"] = f"Lỗi không xác định khi tạo role: {str(e)}"

    return response


async def get_role_by_role_id(
    db: AsyncSession,
    *,
    role_id: str,
    include_user_count: bool = False,
) -> dict[str, Any]:
    """
    Lấy role theo role_id.

    include_user_count=True:
    - Đếm số lượng tài khoản đang dùng role này.
    - Vì không dùng relationship nên phải đếm thủ công:
      UserAuth.role_id == Roles.role_id
    """
    response = make_response(message="Không thể truy vấn role")

    try:
        result = await db.execute(
            select(Roles).where(Roles.role_id == role_id)
        )
        role = result.scalar_one_or_none()

        if not role:
            return make_response(
                success=None,
                data=None,
                message=f"Không tìm thấy role_id={role_id}",
            )

        data = role_to_dict(role)

        if include_user_count:
            count_result = await db.execute(
                select(func.count(UserAuth.id)).where(UserAuth.role_id == role_id)
            )
            data["user_count"] = count_result.scalar_one()

        return make_response(
            success=True,
            data=data,
            message=f"Tìm thấy role_id={role_id}",
        )

    except exc.SQLAlchemyError as e:
        response["message"] = f"Lỗi SQLAlchemy khi truy vấn role: {str(e)}"

    except Exception as e:
        response["message"] = f"Lỗi không xác định khi truy vấn role: {str(e)}"

    return response


async def get_all_roles(
    db: AsyncSession,
    *,
    include_inactive: bool = True,
    limit: int = 1000,
) -> dict[str, Any]:
    """
    Lấy danh sách role.

    include_inactive:
    - True: lấy cả role đang hoạt động và role đã khóa.
    - False: chỉ lấy role đang hoạt động.
    """
    response = make_response(message="Không thể truy vấn danh sách role")

    try:
        stmt = select(Roles)

        if not include_inactive:
            stmt = stmt.where(Roles.is_active.is_(True))

        stmt = stmt.order_by(Roles.created_at.desc()).limit(limit)

        result = await db.execute(stmt)
        roles = list(result.scalars().all())

        if not roles:
            return make_response(
                success=None,
                data=[],
                message="Chưa có dữ liệu role",
            )

        return make_response(
            success=True,
            data=[role_to_dict(role) for role in roles],
            message="Tìm thấy danh sách role",
        )

    except exc.SQLAlchemyError as e:
        response["message"] = f"Lỗi SQLAlchemy khi truy vấn danh sách role: {str(e)}"

    except Exception as e:
        response["message"] = f"Lỗi không xác định khi truy vấn danh sách role: {str(e)}"

    return response


async def update_role(
    db: AsyncSession,
    *,
    role_id: str,
    role_name: str | None = None,
    description: str | None = None,
    permissions_json: str | None = None,
) -> dict[str, Any]:
    """
    Cập nhật role.

    Chỉ field nào truyền khác None thì mới cập nhật.
    Ví dụ:
    - role_name=None nghĩa là giữ nguyên role_name cũ.
    - description="..." nghĩa là cập nhật description mới.
    """
    response = make_response(message="Lỗi khi cập nhật role")

    try:
        result = await db.execute(
            select(Roles).where(Roles.role_id == role_id)
        )
        role = result.scalar_one_or_none()

        if not role:
            return make_response(
                success=None,
                data=None,
                message=f"Không tìm thấy role_id={role_id}",
            )

        if role_name is not None:
            role.role_name = role_name

        if description is not None:
            role.description = description

        if permissions_json is not None:
            role.permissions_json = permissions_json

        role.updated_at = utc_now()

        await db.commit()
        await db.refresh(role)

        return make_response(
            success=True,
            data=role_to_dict(role),
            message=f"Đã cập nhật role_id={role_id}",
        )

    except exc.IntegrityError as e:
        await db.rollback()
        response["message"] = f"Lỗi ràng buộc dữ liệu khi cập nhật role: {str(e)}"

    except exc.SQLAlchemyError as e:
        await db.rollback()
        response["message"] = f"Lỗi SQLAlchemy khi cập nhật role: {str(e)}"

    except Exception as e:
        await db.rollback()
        response["message"] = f"Lỗi không xác định khi cập nhật role: {str(e)}"

    return response


async def activate_role(
    db: AsyncSession,
    *,
    role_id: str,
    activate: bool,
) -> dict[str, Any]:
    """
    Khóa hoặc mở khóa role.

    is_active=False:
    - Không nên gán role này cho user mới.
    - User cũ đang dùng role này vẫn còn role_id.
    - Khi kiểm tra quyền, service nên kiểm tra cả role.is_active.
    """
    response = make_response(message="Lỗi khi cập nhật trạng thái role")

    try:
        result = await db.execute(
            select(Roles).where(Roles.role_id == role_id)
        )
        role = result.scalar_one_or_none()

        if not role:
            return make_response(
                success=None,
                data=None,
                message=f"Không tìm thấy role_id={role_id}",
            )

        role.is_active = activate
        role.updated_at = utc_now()

        await db.commit()
        await db.refresh(role)

        return make_response(
            success=True,
            data=role_to_dict(role),
            message=f"Đã cập nhật role_id={role_id}, is_active={activate}",
        )

    except exc.SQLAlchemyError as e:
        await db.rollback()
        response["message"] = f"Lỗi SQLAlchemy khi cập nhật trạng thái role: {str(e)}"

    except Exception as e:
        await db.rollback()
        response["message"] = f"Lỗi không xác định khi cập nhật trạng thái role: {str(e)}"

    return response


async def delete_role(
    db: AsyncSession,
    *,
    role_id: str,
    hard_delete: bool = False,
    force: bool = False,
) -> dict[str, Any]:
    """
    Xóa role.

    Vì không dùng ForeignKey:
    - SQL Server không tự biết role này đang được UserAuth sử dụng.
    - Do đó phải tự kiểm tra trước khi xóa cứng.

    hard_delete=False:
    - Khóa mềm role bằng is_active=False.
    - Đây là cách an toàn hơn.

    hard_delete=True:
    - Xóa thật role khỏi DB.

    force=True:
    - Cho phép xóa cứng dù vẫn có tài khoản đang dùng role này.
    - Không khuyến nghị vì sẽ tạo dữ liệu mồ côi.
    """
    response = make_response(message="Lỗi khi xóa role")

    try:
        result = await db.execute(
            select(Roles).where(Roles.role_id == role_id)
        )
        role = result.scalar_one_or_none()

        if not role:
            return make_response(
                success=None,
                data=None,
                message=f"Không tìm thấy role_id={role_id}",
            )

        if not hard_delete:
            role.is_active = False
            role.updated_at = utc_now()

            await db.commit()
            await db.refresh(role)

            return make_response(
                success=True,
                data=role_to_dict(role),
                message=f"Đã khóa mềm role_id={role_id}",
            )

        count_result = await db.execute(
            select(func.count(UserAuth.id)).where(UserAuth.role_id == role_id)
        )
        using_count = count_result.scalar_one()

        if using_count > 0 and not force:
            return make_response(
                success=False,
                data={"using_count": using_count},
                message=(
                    f"Không thể xóa cứng role_id={role_id} vì đang có "
                    f"{using_count} tài khoản sử dụng role này"
                ),
            )

        await db.delete(role)
        await db.commit()

        return make_response(
            success=True,
            data=None,
            message=f"Đã xóa cứng role_id={role_id}",
        )

    except exc.SQLAlchemyError as e:
        await db.rollback()
        response["message"] = f"Lỗi SQLAlchemy khi xóa role: {str(e)}"

    except Exception as e:
        await db.rollback()
        response["message"] = f"Lỗi không xác định khi xóa role: {str(e)}"

    return response