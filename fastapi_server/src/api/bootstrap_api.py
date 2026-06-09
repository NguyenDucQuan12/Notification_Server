from typing import Any

from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession

from controllers.bootstrap_controller import BootstrapController
from db.database import get_db
from schemas.schemas import BootstrapAdminRequest


router = APIRouter(
    prefix="/bootstrap",
    tags=["Bootstrap"],
)


@router.post("/init-admin")
async def api_init_admin(
    request: BootstrapAdminRequest,
    db: AsyncSession = Depends(get_db),
) -> Any:
    """
    API tạo admin đầu tiên khi DB trống.

    Lưu ý:
    - Chỉ dùng khi hệ thống chưa có superuser.
    - Phải có bootstrap_secret đúng.
    - Production nên giới hạn endpoint này bằng network/internal hoặc tắt sau khi bootstrap.
    """

    return await BootstrapController.init_admin(
        request=request,
        db=db,
    )