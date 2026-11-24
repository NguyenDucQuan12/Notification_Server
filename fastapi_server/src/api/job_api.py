"""
API luồng xử lý file/job.

- POST /jobs/upload            : upload file -> tạo job, nhận thông báo job.queued
- GET  /jobs                   : danh sách job của tôi (all_users=true cần job:read_all)
- GET  /jobs/{job_id}          : trạng thái / tiến độ của job
- GET  /jobs/{job_id}/result   : tải file kết quả khi job thành công

Theo dõi realtime: POST /notifications/ws-ticket?channel=job&job_id=<job_id> rồi mở WebSocket.
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, File, Query, UploadFile
from sqlalchemy.ext.asyncio import AsyncSession

from auth.oauth2 import required_token_user
from controllers.job_controller import JobController
from db.database import get_db


router = APIRouter(
    prefix="/jobs",
    tags=["Jobs"],
)


@router.post("/upload")
async def api_upload_file(
    file: UploadFile = File(..., description="File cần xử lý"),
    db: AsyncSession = Depends(get_db),
    current_user: dict[str, Any] = Depends(required_token_user),
) -> Any:
    """
    Upload file và tạo job xử lý nền. Cần quyền `job:create`.

    Trả về ngay sau khi lưu file + tạo job (không chờ xử lý xong).
    Tiến trình được gửi qua thông báo: job.queued -> job.started -> job.progress -> job.succeeded / job.failed.
    """
    return await JobController.upload_and_create_job(file=file, db=db, current_user=current_user)


@router.get("")
async def api_list_jobs(
    status: str | None = Query(default=None, description="queued | processing | success | failed | cancelled"),
    all_users: bool = Query(default=False, description="True: job của cả tenant (cần job:read_all)"),
    limit: int = Query(default=100, ge=1, le=1000),
    db: AsyncSession = Depends(get_db),
    current_user: dict[str, Any] = Depends(required_token_user),
) -> Any:
    """Danh sách job, mới nhất trước."""
    return await JobController.list_jobs(
        db=db, current_user=current_user, status_filter=status, all_users=all_users, limit=limit,
    )


@router.get("/{job_id}")
async def api_get_job(
    job_id: str,
    db: AsyncSession = Depends(get_db),
    current_user: dict[str, Any] = Depends(required_token_user),
) -> Any:
    """Trạng thái, tiến độ, lỗi (nếu có) của 1 job. Chủ job hoặc người có job:read_all."""
    return await JobController.get_job_detail(job_id=job_id, db=db, current_user=current_user)


@router.get("/{job_id}/result")
async def api_download_job_result(
    job_id: str,
    db: AsyncSession = Depends(get_db),
    current_user: dict[str, Any] = Depends(required_token_user),
) -> Any:
    """Tải file kết quả (chỉ khi status=success)."""
    return await JobController.download_result(job_id=job_id, db=db, current_user=current_user)
