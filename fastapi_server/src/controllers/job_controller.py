"""
Nghiệp vụ luồng file/job phía API Server.

Upload:  lưu file -> tạo job_record (queued) -> LPUSH hàng đợi -> thông báo job.queued
Xử lý:   do Job Worker đảm nhận (workers/job_worker.py)
Kết quả: client nhận job.succeeded / job.failed qua WebSocket, rồi gọi GET /jobs/{job_id}/result.
"""
from __future__ import annotations

import uuid
from typing import Any

from fastapi import HTTPException, UploadFile, status
from fastapi.responses import FileResponse
from sqlalchemy.ext.asyncio import AsyncSession

from controllers.notification_controller import NotificationController
from db.query_job_record import create_job_record, get_all_jobs, get_job_by_id, get_user_jobs
from services.job_notifier import notify_job
from services.job_queue import enqueue_job
from services.job_storage import (
    UploadTooLargeError,
    delete_job_files,
    resolve_object_path,
    sanitize_filename,
    save_upload,
    upload_object_key,
)

JOB_STATUSES = {"queued", "processing", "success", "failed", "cancelled"}


class JobController:
    """Controller cho API /jobs."""

    @staticmethod
    def _identity(current_user: dict[str, Any]) -> tuple[str, str]:
        if not current_user:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail={"message": "Chưa xác thực người dùng"},
            )
        return str(current_user["tenant_id"]), str(current_user["user_id"])

    @staticmethod
    def can_view_job(current_user: dict[str, Any], job: dict[str, Any]) -> bool:
        """Chủ job, hoặc người có quyền job:read_all (superuser luôn được)."""
        return str(job["user_id"]) == str(current_user.get("user_id")) or (
            NotificationController.has_permission(current_user, "job:read_all")
        )

    @staticmethod
    async def _get_visible_job(db: AsyncSession, current_user: dict[str, Any], job_id: str) -> dict[str, Any]:
        tenant_id, _ = JobController._identity(current_user)
        result = await get_job_by_id(db, tenant_id=tenant_id, job_id=job_id)

        if result["success"] is False:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail={"message": result["message"]},
            )
        # Không tiết lộ job của người khác có tồn tại hay không -> cùng trả 404.
        if result["success"] is None or not JobController.can_view_job(current_user, result["data"]):
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail={"message": f"Không tìm thấy job_id={job_id}"},
            )
        return result["data"]

    # ============================================================
    # UPLOAD -> TẠO JOB
    # ============================================================
    @staticmethod
    async def upload_and_create_job(*, file: UploadFile, db: AsyncSession, current_user: dict[str, Any]) -> dict[str, Any]:
        """
        Upload file và tạo job xử lý nền. Cần quyền `job:create`.
        """
        tenant_id, user_id = JobController._identity(current_user)

        if not NotificationController.has_permission(current_user, "job:create"):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail={"message": "Bạn không có quyền tạo job (job:create)"},
            )

        job_id = uuid.uuid4().hex
        filename = sanitize_filename(file.filename)
        object_key = upload_object_key(tenant_id, job_id, filename)
        trace_id = str(current_user.get("trace_id") or uuid.uuid4().hex)

        # 1. Lưu file vào storage.
        try:
            size = await save_upload(file.file, object_key)
        except UploadTooLargeError as exc:
            await delete_job_files(tenant_id, job_id)
            raise HTTPException(
                status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
                detail={"message": str(exc)},
            ) from exc
        except OSError as exc:
            await delete_job_files(tenant_id, job_id)
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail={"message": f"Không lưu được file: {type(exc).__name__}"},
            ) from exc
        finally:
            await file.close()

        if size == 0:
            await delete_job_files(tenant_id, job_id)
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail={"message": "File rỗng"},
            )

        # 2. Tạo job_record (status=queued). Lỗi -> xóa file vừa lưu.
        created = await create_job_record(
            db,
            tenant_id=tenant_id,
            user_id=user_id,
            filename=filename,
            upload_object_key=object_key,
            trace_id=trace_id,
            job_id=job_id,
        )
        if not created["success"]:
            await delete_job_files(tenant_id, job_id)
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail={"message": created["message"]},
            )
        job = created["data"]

        # 3. Đẩy vào hàng đợi. Redis lỗi thì KHÔNG báo lỗi cho client: job vẫn 'queued' trong DB,
        #    Job Worker sẽ tự tìm và xử lý sau JOB_STALE_AFTER_SECONDS.
        queued = True
        try:
            await enqueue_job(tenant_id=tenant_id, job_id=job_id)
        except Exception as exc:  # pylint: disable=broad-except
            queued = False
            print(f"Không đẩy được job {job_id} vào hàng đợi: {type(exc).__name__}: {exc}", flush=True)

        # 4. Thông báo job.queued (cho người upload + stream của job).
        notification = await notify_job(job, "queued", current_user=current_user)

        return {
            "success": True,
            "data": {
                "job": job,
                "size_bytes": size,
                "enqueued": queued,
                "notification": notification,
                # Client mở WebSocket theo dõi tiến trình:
                # POST /notifications/ws-ticket?channel=job&job_id=<job_id>
                "watch": {"channel": "job", "job_id": job_id},
            },
            "message": "Đã nhận file, job đang chờ xử lý",
        }

    # ============================================================
    # TRA CỨU
    # ============================================================
    @staticmethod
    async def get_job_detail(*, job_id: str, db: AsyncSession, current_user: dict[str, Any]) -> dict[str, Any]:
        job = await JobController._get_visible_job(db, current_user, job_id)
        return {"success": True, "data": job, "message": f"Tìm thấy job_id={job_id}"}

    @staticmethod
    async def list_jobs(
        *,
        db: AsyncSession,
        current_user: dict[str, Any],
        status_filter: str | None,
        all_users: bool,
        limit: int,
    ) -> dict[str, Any]:
        tenant_id, user_id = JobController._identity(current_user)

        if status_filter is not None and status_filter not in JOB_STATUSES:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail={"message": f"status phải là một trong {sorted(JOB_STATUSES)}"},
            )

        if all_users:
            if not NotificationController.has_permission(current_user, "job:read_all"):
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail={"message": "Bạn không có quyền xem job của người khác (job:read_all)"},
                )
            result = await get_all_jobs(db, tenant_id=tenant_id, status=status_filter, limit=limit)
        else:
            result = await get_user_jobs(
                db, tenant_id=tenant_id, user_id=user_id, status=status_filter, limit=limit,
            )

        if result["success"] is False:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail={"message": result["message"]},
            )
        # success=None nghĩa là chưa có job nào -> vẫn trả danh sách rỗng.
        return {"success": True, "data": result["data"] or [], "message": result["message"]}

    @staticmethod
    async def download_result(*, job_id: str, db: AsyncSession, current_user: dict[str, Any]) -> FileResponse:
        job = await JobController._get_visible_job(db, current_user, job_id)

        if job["status"] != "success" or not job.get("result_object_key"):
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail={"message": f"Job chưa có kết quả (status={job['status']})"},
            )

        path = resolve_object_path(job["result_object_key"])
        if not path.is_file():
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail={"message": "File kết quả không còn tồn tại"},
            )

        stem = job["filename"].rsplit(".", 1)[0]
        return FileResponse(path=path, filename=f"{stem}_result{path.suffix}", media_type="application/json")
