from __future__ import annotations

import re
from typing import Any

from fastapi import HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession

from db.query_users import create_user_profile, delete_user_profile
from db.query_user_auth import create_user_auth, get_auth_for_login, record_login_failed, record_login_success
from utils.utils import parse_permissions_from_role
from schemas.schemas import LoginRequest, RegisterUserRequest
from auth.hash import Hash
from auth.oauth2 import create_access_token
from utils.constants import EMAIL_REGEX


class AuthController:
    """
    Controller xử lý nghiệp vụ người dùng và đăng nhập.

    Router chỉ nhận request và gọi controller.
    Controller chịu trách nhiệm:
    - validate dữ liệu nghiệp vụ
    - gọi query DB
    - hash password
    - tạo token
    - xử lý lỗi
    """

    @staticmethod
    async def register_user(
        *,
        request: RegisterUserRequest,
        db: AsyncSession,
    ) -> dict[str, Any]:
        """
        Tạo người dùng mới.

        Luồng xử lý:
        1. Validate dữ liệu đầu vào.
        2. Tạo hồ sơ trong bảng users.
        3. Hash password.
        4. Tạo tài khoản đăng nhập trong bảng user_auth.
        5. Trả về user + auth.
        """

        # ====================================================
        # 1. VALIDATE DỮ LIỆU CƠ BẢN
        # ====================================================

        tenant_id = request.tenant_id.strip()
        username = request.username.strip()
        password = request.password.strip()

        if not tenant_id:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail={"message": "tenant_id là bắt buộc"},
            )

        if not username:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail={"message": "username là bắt buộc"},
            )

        if len(username) < 3:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail={"message": "username phải có ít nhất 3 ký tự"},
            )

        if not password:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail={"message": "password là bắt buộc"},
            )

        if len(password) < 6:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail={"message": "password phải có ít nhất 6 ký tự"},
            )

        email = str(request.email).strip() if request.email else None

        if email and not re.match(EMAIL_REGEX, email):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail={"message": f"Email {email} không hợp lệ"},
            )

        full_name = request.full_name.strip() if request.full_name else None
        phone = request.phone.strip() if request.phone else None

        # ====================================================
        # 2. TẠO HỒ SƠ USER
        # ====================================================

        created_user = await create_user_profile(
            db,
            tenant_id=tenant_id,
            full_name=full_name,
            email=email,
            phone=phone,
        )

        if not created_user["success"]:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail={"message": created_user["message"]},
            )

        user_data = created_user["data"]
        user_id = user_data["user_id"]

        # ====================================================
        # 3. HASH PASSWORD
        # ====================================================

        password_hash = Hash.bcrypt(password)

        # ====================================================
        # 4. TẠO AUTH_USER
        # ====================================================

        created_auth = await create_user_auth(
            db,
            tenant_id=tenant_id,
            user_id=user_id,
            username=username,
            password_hash=password_hash,
            role_id=request.role_id,
            is_superuser=request.is_superuser,
        )

        if not created_auth["success"]:
            # Vì create_user_profile đã commit trước đó,
            # nếu tạo auth lỗi thì nên rollback nghiệp vụ bằng cách xóa/khóa user vừa tạo.
            await delete_user_profile(
                db,
                tenant_id=tenant_id,
                user_id=user_id,
                hard_delete=True,
            )

            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail={"message": created_auth["message"]},
            )

        return {
            "success": True,
            "data": {
                "user": user_data,
                "auth": created_auth["data"],
                "role": None,
            },
            "message": "Tạo người dùng thành công",
        }

    @staticmethod
    async def login(
        *,
        request: LoginRequest,
        db: AsyncSession,
    ) -> dict[str, Any]:
        """
        Đăng nhập.

        Luồng xử lý:
        1. Validate tenant_id, username, password.
        2. Lấy auth_user + users + roles từ DB.
        3. Kiểm tra user có active không.
        4. Kiểm tra auth có được phép login không.
        5. Verify password.
        6. Tạo access token.
        7. Ghi nhận login thành công hoặc thất bại.
        """

        # Lấy các thông tin từ request và strip để loại bỏ khoảng trắng thừa ở đầu và cuối chuỗi. Điều này giúp tránh lỗi do người dùng nhập nhầm khoảng trắng khi đăng nhập hoặc đăng ký.
        tenant_id = request.tenant_id.strip()         # Tenant ID để phân biệt người dùng này thuộc về tổ chức/ nhóm nào, giúp quản lý người dùng theo tenant.
        username = request.username.strip()
        password = request.password

        if not tenant_id:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail={"message": "tenant_id là bắt buộc để xác định tổ chức của người dùng"},
            )

        if not username:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail={"message": "username là bắt buộc"},
            )

        if not password:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail={"message": "password là bắt buộc"},
            )

        # Kiểm tra đăng nhập và lấy thông tin user + auth (kèm password_hash) + role từ DB
        login_data = await get_auth_for_login(
            db,
            tenant_id=tenant_id,
            username=username,
        )

        if not login_data["success"]:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail={"message": "Sai tài khoản hoặc mật khẩu"},
            )

        data = login_data["data"]
        auth = data["auth"]
        user = data["user"]
        role = data["role"]

        if not user["is_active"]:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail={"message": "Tài khoản người dùng đã bị khóa"},
            )

        if not auth["is_login_enabled"]:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail={"message": "Tài khoản đăng nhập đã bị khóa"},
            )

        # Lấy password_hash để verify với password người dùng vừa cung cấp
        password_hash = auth["password_hash"]
        # Nếu verify thất bại thì ghi nhận login failed và trả về lỗi 401 Unauthorized.
        if not Hash.verify(password, password_hash):
            await record_login_failed(
                db,
                tenant_id=tenant_id,
                username=username,
            )

            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail={"message": "Sai tài khoản hoặc mật khẩu"},
            )

        # Nếu verify thành công thì ghi nhận login success và tạo token trả về cho client.
        await record_login_success(
            db,
            tenant_id=tenant_id,
            username=username,
        )

        permissions = parse_permissions_from_role(role)

        #Tạo token với thông tin đi kèm. Không truyền biến chứa các giá trị datetime vào, nên sử dụng các kiểu đơn giản như str, int, bool,...
        access_token = create_access_token(
            data={
                "tenant_id": user["tenant_id"],
                "user_id": user["user_id"],
                "user_name": auth["username"],
                "email": user["email"],
                "role_id": auth["role_id"],
                "permissions": permissions,
                "is_superuser": auth["is_superuser"],
            },
        )

        # Không trả password_hash ra client. Loại bỏ trường password_hash khỏi dict auth trước khi trả về response. Nếu có nhiều trường nhạy cảm khác cũng nên loại bỏ tương tự.
        auth.pop("password_hash", None)

        return {
            "success": True,
            "data": {
                "token": {
                    "access_token": access_token,
                    "token_type": "bearer",
                },
                "user": user,
                "auth": auth,
                "role": role,
            },
            "message": "Đăng nhập thành công",
        }