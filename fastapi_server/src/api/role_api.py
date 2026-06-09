from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession

from auth.oauth2 import required_token_user
from controllers.role_controller import RoleController
from db.database import get_db
from schemas.schemas import APIResponse, RoleActivateRequest, RoleCreate, RoleDisplay, RoleListResponse, RoleResponse, RoleUpdate


router = APIRouter(
    prefix="/roles",
    tags=["Roles"],
)


@router.post(
    "/create",
    response_model=RoleResponse,
)
async def api_create_role(
    request: RoleCreate,
    db: AsyncSession = Depends(get_db),
    current_user: dict[str, Any] = Depends(required_token_user),
) -> Any:
    """
    API tạo role mới.

    Router chỉ nhận request và gọi controller.

    Controller sẽ xử lý:
    - kiểm tra current_user có phải superuser không
    - validate role_id
    - validate role_name
    - validate permissions_json
    - gọi query để insert database
    """

    return await RoleController.create_new_role(
        request=request,
        db=db,
        current_user=current_user,
    )


@router.get(
    "/list",
    response_model=RoleListResponse,
)
async def api_get_list_roles(
    include_inactive: bool = Query(
        default=True,
        description="True để lấy cả role bị khóa, False để chỉ lấy role active.",
    ),
    limit: int = Query(
        default=1000,
        ge=1,
        le=5000,
        description="Số role tối đa trả về.",
    ),
    db: AsyncSession = Depends(get_db),
    current_user: dict[str, Any] = Depends(required_token_user),
) -> Any:
    """
    API lấy danh sách role.

    API này yêu cầu đã đăng nhập.
    Nếu muốn chỉ superuser được xem danh sách role,
    hãy gọi _require_superuser trong controller.
    """

    return await RoleController.get_list_roles(
        db=db,
        current_user=current_user,
        include_inactive=include_inactive,
        limit=limit,
    )


@router.get(
    "/{role_id}",
    response_model=APIResponse[dict[str, Any]],
)
async def api_get_role_detail(
    role_id: str,
    include_user_count: bool = Query(
        default=False,
        description="True để trả thêm số lượng tài khoản đang dùng role này.",
    ),
    db: AsyncSession = Depends(get_db),
    current_user: dict[str, Any] = Depends(required_token_user),
) -> Any:
    """
    API lấy chi tiết role theo role_id.

    include_user_count=True:
    - trả thêm user_count
    - phù hợp cho màn hình quản trị role
    """

    return await RoleController.get_role_detail(
        role_id=role_id,
        db=db,
        current_user=current_user,
        include_user_count=include_user_count,
    )


@router.put(
    "/{role_id}",
    response_model=RoleResponse,
)
async def api_update_role(
    role_id: str,
    request: RoleUpdate,
    db: AsyncSession = Depends(get_db),
    current_user: dict[str, Any] = Depends(required_token_user),
) -> Any:
    """
    API cập nhật role.

    Chỉ superuser được cập nhật.

    Có thể cập nhật:
    - role_name
    - description
    - permissions_json
    """

    return await RoleController.update_role_info(
        role_id=role_id,
        request=request,
        db=db,
        current_user=current_user,
    )


@router.patch(
    "/{role_id}/activate",
    response_model=RoleResponse,
)
async def api_activate_role(
    role_id: str,
    request: RoleActivateRequest,
    db: AsyncSession = Depends(get_db),
    current_user: dict[str, Any] = Depends(required_token_user),
) -> Any:
    """
    API khóa hoặc mở khóa role.

    Body mẫu:
    {
        "activate": true
    }

    hoặc:

    {
        "activate": false
    }
    """

    return await RoleController.activate_role_info(
        role_id=role_id,
        request=request,
        db=db,
        current_user=current_user,
    )


@router.delete(
    "/{role_id}",
    response_model=APIResponse[Any],
)
async def api_delete_role(
    role_id: str,
    hard_delete: bool = Query(
        default=False,
        description=(
            "False là khóa mềm role, True là xóa cứng role khỏi database."
        ),
    ),
    force: bool = Query(
        default=False,
        description=(
            "True cho phép xóa cứng dù role đang được user_auth sử dụng. "
            "Không khuyến nghị dùng."
        ),
    ),
    db: AsyncSession = Depends(get_db),
    current_user: dict[str, Any] = Depends(required_token_user),
) -> Any:
    """
    API xóa role.

    Khuyến nghị:
    - hard_delete=False để khóa mềm role.
    - Chỉ dùng hard_delete=True khi chắc chắn role không còn được sử dụng.
    """

    return await RoleController.delete_role_info(
        role_id=role_id,
        db=db,
        current_user=current_user,
        hard_delete=hard_delete,
        force=force,
    )