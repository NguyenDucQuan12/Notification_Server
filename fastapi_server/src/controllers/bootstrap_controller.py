from __future__ import annotations

import json
from typing import Any

from fastapi import HTTPException, status
import os
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession
from dotenv import load_dotenv

from db.models import Users, UserAuth, Roles
from db.query_roles import create_role
from db.query_users import create_user_profile
from db.query_user_auth import create_user_auth
from schemas.schemas import BootstrapAdminRequest
from services.notification_publisher import publish_job_event
from utils.events import build_role_event
from auth.hash import Hash

load_dotenv()


class BootstrapController:
    """
    Controller tạo dữ liệu gốc khi DB trống.

    Controller này không dùng current_user vì lúc đầu chưa có admin.
    Thay vào đó dùng bootstrap_secret để bảo vệ.
    """

    @staticmethod
    async def init_admin(
        *,
        request: BootstrapAdminRequest,
        db: AsyncSession,
    ) -> dict[str, Any]:
        """
        Tạo role admin và tài khoản admin đầu tiên.

        Chỉ cho phép chạy khi hệ thống chưa có superuser.
        """

        if request.bootstrap_secret != os.getenv("BOOTSTRAP_SECRET", '1234567890'):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail={"message": "bootstrap_secret không hợp lệ"},
            )

        # Kiểm tra hệ thống đã có superuser chưa.
        # Nếu đã có thì không cho bootstrap nữa.
        count_result = await db.execute(
            select(func.count(UserAuth.id)).where(
                UserAuth.is_superuser == True
            )
        )
        superuser_count = count_result.scalar_one()

        if superuser_count > 0:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail={"message": "Hệ thống đã có superuser, không được bootstrap lại"},
            )

        admin_role_id = "admin"

        # 1. Tạo role admin nếu chưa có.
        role_result = await db.execute(
            select(Roles).where(Roles.role_id == admin_role_id)
        )
        admin_role = role_result.scalar_one_or_none()

        if not admin_role:
            permissions = [
                "notification:subscribe",
                "notification:tenant_dashboard",
                "notification:job_watch",
                "notification:global_subscribe",
                "role:create",
                "role:read",
                "role:update",
                "role:delete",
                "user:create",
                "user:read",
                "user:update",
                "job:create",
                "job:read",
                "job:read_all",
                "job:download",
            ]

            created_role = await create_role(
                db,
                role_id=admin_role_id,
                role_name="Admin",
                description="Quản trị hệ thống",
                permissions_json=json.dumps(permissions, ensure_ascii=False),
            )

            if not created_role["success"]:
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail={"message": created_role["message"]},
                )

            role_data = created_role["data"]

            # Ghi event role.created vào global stream.
            # Lúc này có thể chưa có admin nào đang nghe,
            # nhưng event vẫn được lưu trong Redis Stream.
            try:
                await publish_job_event(
                    event=build_role_event(
                        event_type="role.created",
                        role_id=role_data["role_id"],
                        role_name=role_data["role_name"],
                        description=role_data.get("description"),
                        permissions_json=role_data.get("permissions_json"),
                        actor_user_id=None,
                        actor_username="bootstrap",
                        message="Role Admin được tạo trong quá trình bootstrap",
                    )
                )
            except Exception:
                pass

        # 2. Tạo hồ sơ user admin.
        created_user = await create_user_profile(
            db,
            tenant_id=request.tenant_id,
            full_name=request.full_name,
            email=str(request.email) if request.email else None,
            phone=request.phone,
        )

        if not created_user["success"]:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail={"message": created_user["message"]},
            )

        user_data = created_user["data"]

        # 3. Tạo auth cho admin.
        password_hash = Hash.bcrypt(request.password)

        created_auth = await create_user_auth(
            db,
            tenant_id=request.tenant_id,
            user_id=user_data["user_id"],
            username=request.username,
            password_hash=password_hash,
            role_id=admin_role_id,
            is_superuser=True,
        )

        if not created_auth["success"]:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail={"message": created_auth["message"]},
            )

        return {
            "success": True,
            "data": {
                "user": user_data,
                "auth": created_auth["data"],
                "role_id": admin_role_id,
            },
            "message": "Bootstrap admin thành công",
        }