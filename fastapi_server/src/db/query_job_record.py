from __future__ import annotations

import uuid
from datetime import timedelta
from typing import Any

from sqlalchemy import exc, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from models.job_record_model import JobRecord
from models.users_model import Users
from utils.response_db import make_response, user_to_dict, job_to_dict
from utils.utils import utc_now


async def create_job_record(
    db: AsyncSession,
    *,
    tenant_id: str,
    user_id: str,
    filename: str,
    upload_object_key: str,
    trace_id: str,
    job_id: str | None = None,
) -> dict[str, Any]:
    """
    Tạo bản ghi job mới.

    Luồng xử lý thông thường:
    1. User upload file.
    2. API lưu file vào storage.
    3. API tạo job_record trong SQL Server.
    4. API đẩy job_id vào queue.
    5. Worker lấy job_id từ queue.
    6. Worker cập nhật lại trạng thái job.

    Vì không có ForeignKey, trước khi tạo job phải tự kiểm tra:
    - user_id có tồn tại không
    - user thuộc đúng tenant_id không
    - user có đang active không
    """
    response = make_response(message="Lỗi khi tạo job_record")

    try:
        user_result = await db.execute(
            select(Users).where(
                Users.tenant_id == tenant_id,
                Users.user_id == user_id,
                Users.is_active == True,  # noqa: E712 - SQL Server không hỗ trợ "IS 1"
            )
        )
        user = user_result.scalar_one_or_none()

        if not user:
            return make_response(
                success=False,
                data=None,
                message=f"Không thể tạo job vì user_id={user_id} không tồn tại hoặc đã bị khóa",
            )

        now = utc_now()

        job = JobRecord(
            job_id=job_id or uuid.uuid4().hex,
            tenant_id=tenant_id,
            user_id=user_id,
            filename=filename,
            status="queued",
            progress=0,
            message="Job đã được đưa vào hàng chờ",
            attempts=0,
            upload_object_key=upload_object_key,
            result_object_key=None,
            error=None,
            trace_id=trace_id,
            created_at=now,
            updated_at=now,
            finished_at=None,
        )

        db.add(job)
        await db.commit()
        await db.refresh(job)

        return make_response(
            success=True,
            data=job_to_dict(job),
            message=f"Đã tạo job_record job_id={job.job_id}",
        )

    except exc.IntegrityError as e:
        await db.rollback()
        response["message"] = f"Lỗi ràng buộc dữ liệu khi tạo job_record: {str(e)}"

    except exc.DataError as e:
        await db.rollback()
        response["message"] = f"Lỗi dữ liệu khi tạo job_record: {str(e)}"

    except exc.SQLAlchemyError as e:
        await db.rollback()
        response["message"] = f"Lỗi SQLAlchemy khi tạo job_record: {str(e)}"

    except Exception as e:
        await db.rollback()
        response["message"] = f"Lỗi không xác định khi tạo job_record: {str(e)}"

    return response


async def get_job_by_id(
    db: AsyncSession,
    *,
    tenant_id: str,
    job_id: str,
    include_user: bool = False,
) -> dict[str, Any]:
    """
    Lấy job theo job_id.

    Luôn lọc theo tenant_id để tránh lấy nhầm job của tenant khác.

    include_user=True:
    - Join sang bảng Users để lấy thông tin người tạo job.
    """
    response = make_response(message="Không thể truy vấn job_record")

    try:
        if not include_user:
            result = await db.execute(
                select(JobRecord).where(
                    JobRecord.tenant_id == tenant_id,
                    JobRecord.job_id == job_id,
                )
            )
            job = result.scalar_one_or_none()

            if not job:
                return make_response(
                    success=None,
                    data=None,
                    message=f"Không tìm thấy job_id={job_id}",
                )

            return make_response(
                success=True,
                data=job_to_dict(job),
                message=f"Tìm thấy job_id={job_id}",
            )

        stmt = (
            select(JobRecord, Users)
            .outerjoin(
                Users,
                (JobRecord.user_id == Users.user_id)
                & (JobRecord.tenant_id == Users.tenant_id),
            )
            .where(
                JobRecord.tenant_id == tenant_id,
                JobRecord.job_id == job_id,
            )
        )

        result = await db.execute(stmt)
        row = result.first()

        if not row:
            return make_response(
                success=None,
                data=None,
                message=f"Không tìm thấy job_id={job_id}",
            )

        job, user = row

        data = {
            "job": job_to_dict(job),
            "user": user_to_dict(user),
        }

        return make_response(
            success=True,
            data=data,
            message=f"Tìm thấy job_id={job_id}",
        )

    except exc.SQLAlchemyError as e:
        response["message"] = f"Lỗi SQLAlchemy khi truy vấn job_record: {str(e)}"

    except Exception as e:
        response["message"] = f"Lỗi không xác định khi truy vấn job_record: {str(e)}"

    return response


async def get_user_jobs(
    db: AsyncSession,
    *,
    tenant_id: str,
    user_id: str,
    status: str | None = None,
    limit: int = 100,
) -> dict[str, Any]:
    """
    Lấy danh sách job của một user.

    status:
    - Nếu truyền vào thì chỉ lấy job theo trạng thái đó.
    - Ví dụ: queued, processing, success, failed.

    Vì JobRecord đã có tenant_id và user_id,
    có thể query trực tiếp bảng JobRecord.

    Tuy nhiên, nếu muốn chắc chắn user tồn tại,
    bạn có thể join thêm bảng Users.
    Ở đây dùng query trực tiếp để đơn giản và nhanh hơn.
    """
    response = make_response(message="Không thể truy vấn danh sách job của user")

    try:
        stmt = select(JobRecord).where(
            JobRecord.tenant_id == tenant_id,
            JobRecord.user_id == user_id,
        )

        if status is not None:
            stmt = stmt.where(JobRecord.status == status)

        stmt = stmt.order_by(JobRecord.created_at.desc()).limit(limit)

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


async def get_all_jobs(
    db: AsyncSession,
    *,
    tenant_id: str | None = None,
    user_id: str | None = None,
    status: str | None = None,
    limit: int = 1000,
) -> dict[str, Any]:
    """
    Lấy danh sách job.

    Có thể lọc theo:
    - tenant_id
    - user_id
    - status

    Với API thực tế, nếu người dùng không phải super admin,
    nên luôn truyền tenant_id để tránh trả job toàn hệ thống.
    """
    response = make_response(message="Không thể truy vấn danh sách job")

    try:
        stmt = select(JobRecord)

        if tenant_id is not None:
            stmt = stmt.where(JobRecord.tenant_id == tenant_id)

        if user_id is not None:
            stmt = stmt.where(JobRecord.user_id == user_id)

        if status is not None:
            stmt = stmt.where(JobRecord.status == status)

        stmt = stmt.order_by(JobRecord.created_at.desc()).limit(limit)

        result = await db.execute(stmt)
        jobs = list(result.scalars().all())

        if not jobs:
            return make_response(
                success=None,
                data=[],
                message="Chưa có dữ liệu job",
            )

        return make_response(
            success=True,
            data=[job_to_dict(job) for job in jobs],
            message="Tìm thấy danh sách job",
        )

    except exc.SQLAlchemyError as e:
        response["message"] = f"Lỗi SQLAlchemy khi truy vấn danh sách job: {str(e)}"

    except Exception as e:
        response["message"] = f"Lỗi không xác định khi truy vấn danh sách job: {str(e)}"

    return response


async def update_job_progress(
    db: AsyncSession,
    *,
    tenant_id: str,
    job_id: str,
    status: str | None = None,
    progress: int | None = None,
    message: str | None = None,
) -> dict[str, Any]:
    """
    Cập nhật tiến độ job.

    Hàm này thường do worker gọi.

    progress:
    - Nên nằm trong khoảng 0 đến 100.
    - Nếu progress < 0 hoặc > 100 thì trả lỗi trước khi update DB.
    """
    response = make_response(message="Lỗi khi cập nhật tiến độ job")

    try:
        if progress is not None and not 0 <= progress <= 100:
            return make_response(
                success=False,
                data=None,
                message="progress phải nằm trong khoảng 0 đến 100",
            )

        result = await db.execute(
            select(JobRecord).where(
                JobRecord.tenant_id == tenant_id,
                JobRecord.job_id == job_id,
            )
        )
        job = result.scalar_one_or_none()

        if not job:
            return make_response(
                success=None,
                data=None,
                message=f"Không tìm thấy job_id={job_id}",
            )

        if status is not None:
            job.status = status

        if progress is not None:
            job.progress = progress

        if message is not None:
            job.message = message

        job.updated_at = utc_now()

        await db.commit()
        await db.refresh(job)

        return make_response(
            success=True,
            data=job_to_dict(job),
            message=f"Đã cập nhật tiến độ job_id={job_id}",
        )

    except exc.SQLAlchemyError as e:
        await db.rollback()
        response["message"] = f"Lỗi SQLAlchemy khi cập nhật job: {str(e)}"

    except Exception as e:
        await db.rollback()
        response["message"] = f"Lỗi không xác định khi cập nhật job: {str(e)}"

    return response


async def increment_job_attempts(
    db: AsyncSession,
    *,
    tenant_id: str,
    job_id: str,
) -> dict[str, Any]:
    """
    Tăng số lần worker thử xử lý job.

    Mỗi lần worker bắt đầu xử lý hoặc retry,
    có thể gọi hàm này để tăng attempts.
    """
    response = make_response(message="Lỗi khi tăng attempts của job")

    try:
        result = await db.execute(
            select(JobRecord).where(
                JobRecord.tenant_id == tenant_id,
                JobRecord.job_id == job_id,
            )
        )
        job = result.scalar_one_or_none()

        if not job:
            return make_response(
                success=None,
                data=None,
                message=f"Không tìm thấy job_id={job_id}",
            )

        job.attempts += 1
        job.updated_at = utc_now()

        await db.commit()
        await db.refresh(job)

        return make_response(
            success=True,
            data=job_to_dict(job),
            message=f"Đã tăng attempts cho job_id={job_id}",
        )

    except exc.SQLAlchemyError as e:
        await db.rollback()
        response["message"] = f"Lỗi SQLAlchemy khi tăng attempts: {str(e)}"

    except Exception as e:
        await db.rollback()
        response["message"] = f"Lỗi không xác định khi tăng attempts: {str(e)}"

    return response


async def mark_job_success(
    db: AsyncSession,
    *,
    tenant_id: str,
    job_id: str,
    result_object_key: str,
    message: str = "Xử lý hoàn tất",
) -> dict[str, Any]:
    """
    Đánh dấu job xử lý thành công.

    Khi thành công:
    - status = success
    - progress = 100
    - result_object_key có giá trị
    - error = None
    - finished_at = thời điểm hiện tại
    """
    response = make_response(message="Lỗi khi đánh dấu job thành công")

    try:
        result = await db.execute(
            select(JobRecord).where(
                JobRecord.tenant_id == tenant_id,
                JobRecord.job_id == job_id,
            )
        )
        job = result.scalar_one_or_none()

        if not job:
            return make_response(
                success=None,
                data=None,
                message=f"Không tìm thấy job_id={job_id}",
            )

        now = utc_now()

        job.status = "success"
        job.progress = 100
        job.message = message
        job.result_object_key = result_object_key
        job.error = None
        job.updated_at = now
        job.finished_at = now

        await db.commit()
        await db.refresh(job)

        return make_response(
            success=True,
            data=job_to_dict(job),
            message=f"Job job_id={job_id} đã hoàn thành",
        )

    except exc.SQLAlchemyError as e:
        await db.rollback()
        response["message"] = f"Lỗi SQLAlchemy khi đánh dấu success: {str(e)}"

    except Exception as e:
        await db.rollback()
        response["message"] = f"Lỗi không xác định khi đánh dấu success: {str(e)}"

    return response


async def mark_job_failed(
    db: AsyncSession,
    *,
    tenant_id: str,
    job_id: str,
    error: str,
    message: str = "Xử lý thất bại",
) -> dict[str, Any]:
    """
    Đánh dấu job xử lý thất bại.

    Khi thất bại:
    - status = failed
    - error lưu nội dung lỗi
    - finished_at = thời điểm hiện tại
    """
    response = make_response(message="Lỗi khi đánh dấu job thất bại")

    try:
        result = await db.execute(
            select(JobRecord).where(
                JobRecord.tenant_id == tenant_id,
                JobRecord.job_id == job_id,
            )
        )
        job = result.scalar_one_or_none()

        if not job:
            return make_response(
                success=None,
                data=None,
                message=f"Không tìm thấy job_id={job_id}",
            )

        now = utc_now()

        job.status = "failed"
        job.message = message
        job.error = error
        job.updated_at = now
        job.finished_at = now

        await db.commit()
        await db.refresh(job)

        return make_response(
            success=True,
            data=job_to_dict(job),
            message=f"Job job_id={job_id} đã được đánh dấu failed",
        )

    except exc.SQLAlchemyError as e:
        await db.rollback()
        response["message"] = f"Lỗi SQLAlchemy khi đánh dấu failed: {str(e)}"

    except Exception as e:
        await db.rollback()
        response["message"] = f"Lỗi không xác định khi đánh dấu failed: {str(e)}"

    return response


async def delete_job_record(
    db: AsyncSession,
    *,
    tenant_id: str,
    job_id: str,
) -> dict[str, Any]:
    """
    Xóa job_record.

    Lưu ý:
    - Hàm này chỉ xóa metadata trong SQL Server.
    - Không tự xóa file upload hoặc file kết quả trong storage.
    - Nếu muốn xóa sạch, service bên ngoài phải xóa object storage riêng.
    """
    response = make_response(message="Lỗi khi xóa job_record")

    try:
        result = await db.execute(
            select(JobRecord).where(
                JobRecord.tenant_id == tenant_id,
                JobRecord.job_id == job_id,
            )
        )
        job = result.scalar_one_or_none()

        if not job:
            return make_response(
                success=None,
                data=None,
                message=f"Không tìm thấy job_id={job_id}",
            )

        await db.delete(job)
        await db.commit()

        return make_response(
            success=True,
            data=None,
            message=f"Đã xóa job_id={job_id}",
        )

    except exc.SQLAlchemyError as e:
        await db.rollback()
        response["message"] = f"Lỗi SQLAlchemy khi xóa job_record: {str(e)}"

    except Exception as e:
        await db.rollback()
        response["message"] = f"Lỗi không xác định khi xóa job_record: {str(e)}"

    return response


# ============================================================
# CÁC HÀM DÀNH CHO JOB WORKER
# ============================================================

async def claim_job_for_processing(
    db: AsyncSession,
    *,
    tenant_id: str,
    job_id: str,
    message: str = "Đang xử lý",
) -> dict[str, Any] | None:
    """
    Nhận (claim) job để xử lý một cách nguyên tử: queued -> processing, attempts + 1.

    Câu UPDATE có điều kiện status='queued' nên khi nhiều worker cùng lấy được 1 job_id
    (ví dụ job bị đẩy vào hàng đợi 2 lần) thì chỉ đúng 1 worker claim thành công.

    Trả về dict job sau khi claim, hoặc None nếu job không tồn tại / không còn ở trạng thái queued.
    """
    now = utc_now()
    try:
        result = await db.execute(
            update(JobRecord)
            .where(
                JobRecord.tenant_id == tenant_id,
                JobRecord.job_id == job_id,
                JobRecord.status == "queued",
            )
            .values(
                status="processing",
                attempts=JobRecord.attempts + 1,
                message=message,
                error=None,
                updated_at=now,
            )
            .execution_options(synchronize_session=False)
        )
        if result.rowcount != 1:
            await db.rollback()
            return None

        await db.commit()

        job = (
            await db.execute(
                select(JobRecord).where(
                    JobRecord.tenant_id == tenant_id,
                    JobRecord.job_id == job_id,
                )
            )
        ).scalar_one()
        return job_to_dict(job)

    except Exception:
        await db.rollback()
        raise


async def requeue_job_for_retry(
    db: AsyncSession,
    *,
    tenant_id: str,
    job_id: str,
    error: str,
    message: str,
) -> dict[str, Any] | None:
    """
    Đưa job đang processing về lại queued để thử lại (giữ nguyên attempts đã tăng).
    """
    try:
        job = (
            await db.execute(
                select(JobRecord).where(
                    JobRecord.tenant_id == tenant_id,
                    JobRecord.job_id == job_id,
                )
            )
        ).scalar_one_or_none()
        if not job:
            return None

        job.status = "queued"
        job.message = message
        job.error = error[:500]
        job.updated_at = utc_now()

        await db.commit()
        await db.refresh(job)
        return job_to_dict(job)

    except Exception:
        await db.rollback()
        raise


async def recover_stale_jobs(
    db: AsyncSession,
    *,
    stale_after_seconds: int,
    limit: int = 500,
) -> list[dict[str, Any]]:
    """
    Tìm các job bị "bỏ quên" để Job Worker đẩy lại vào hàng đợi:

    - status='processing' nhưng lâu không cập nhật: worker xử lý đã chết giữa chừng
      -> chuyển về 'queued'.
    - status='queued' nhưng lâu không ai xử lý: phần tử trong Redis queue bị mất
      (Redis restart, API lỗi khi LPUSH...).

    Việc đẩy trùng là an toàn vì claim_job_for_processing() chỉ cho 1 worker nhận job.
    Trả về danh sách {"tenant_id", "job_id"} cần đẩy lại vào hàng đợi.
    """
    threshold = utc_now() - timedelta(seconds=stale_after_seconds)
    try:
        jobs = list(
            (
                await db.execute(
                    select(JobRecord)
                    .where(
                        JobRecord.status.in_(("queued", "processing")),
                        JobRecord.updated_at < threshold,
                    )
                    .order_by(JobRecord.updated_at)
                    .limit(limit)
                )
            ).scalars().all()
        )

        now = utc_now()
        for job in jobs:
            if job.status == "processing":
                job.status = "queued"
                job.message = "Job bị gián đoạn, đang đưa lại vào hàng chờ"
            # Cập nhật updated_at để không bị quét lại ngay ở vòng sau.
            job.updated_at = now

        await db.commit()
        return [{"tenant_id": job.tenant_id, "job_id": job.job_id} for job in jobs]

    except Exception:
        await db.rollback()
        raise
