from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession

from controllers.user_auth_controller import AuthController
from db.database import get_db
from schemas.schemas import APIResponse, LoginDisplay, LoginRequest, RegisterUserRequest, RegisterUserDisplay


router = APIRouter(
    prefix="/auth",
    tags=["Auth"],
)


@router.post(
    "/register",
    response_model=APIResponse[RegisterUserDisplay],
)
async def register_user(
    request: RegisterUserRequest,
    db: AsyncSession = Depends(get_db),
) -> Any:
    """
    API tạo người dùng mới.

    Router chỉ làm nhiệm vụ:
    - nhận request
    - lấy db session
    - gọi controller

    Toàn bộ validate nghiệp vụ nằm trong AuthController.register_user().
    """

    return await AuthController.register_user(
        request=request,
        db=db,
    )


@router.post(
    "/login",
    response_model=APIResponse[LoginDisplay],
)
async def login(
    request: LoginRequest,
    db: AsyncSession = Depends(get_db),
) -> Any:
    """
    API đăng nhập.

    Router không tự xử lý logic đăng nhập.
    Controller sẽ:
    - kiểm tra username/password
    - kiểm tra user active
    - kiểm tra login enabled
    - tạo access token
    """

    return await AuthController.login(
        request=request,
        db=db,
    )