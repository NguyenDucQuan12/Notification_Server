from __future__ import annotations

from datetime import datetime
from typing import Optional

from sqlalchemy import Boolean, DateTime, Integer, String, Text, Unicode, and_
from sqlalchemy.orm import Mapped, mapped_column

from db.database import Base
from utils.utils import utc_now




class JobRecord(Base):
    """
    Bảng job_record lưu thông tin công việc mà người dùng gửi lên.
    """

    __tablename__ = "job_record"

    # job_id do app sinh ra, ví dụ uuid.uuid4().hex.
    # Dùng làm primary key vì mỗi job là duy nhất.
    job_id: Mapped[str] = mapped_column( String(64), primary_key=True, )

    # tenant_id là mã khách hàng/nhóm/công ty sở hữu job này.
    #
    # Ý nghĩa rất quan trọng:
    # - Nếu hệ thống chỉ phục vụ một công ty thì tenant_id có thể là một giá trị cố định.
    # - Nếu hệ thống phục vụ nhiều công ty/khách hàng, tenant_id giúp tách dữ liệu.
    # - Khi truy vấn job, nên luôn lọc theo tenant_id để tránh user tenant A xem job của tenant B.
    # - Khi worker xử lý job, tenant_id cũng giúp xác định file, cấu hình, quyền,
    #   storage bucket hoặc chính sách xử lý thuộc khách hàng nào.
    #
    # Ví dụ:
    # tenant_id = "demo"
    # tenant_id = "company_a"
    # tenant_id = "customer_001"
    tenant_id: Mapped[str] = mapped_column(
        String(64),
        index=True,
        nullable=False,
    )

    # user_id là người tạo job.
    # Liên kết logic tới users.user_id, nhưng không dùng ForeignKey.
    user_id: Mapped[str] = mapped_column( String(64), index=True, nullable=False, )

    # Tên file gốc sau khi đã sanitize ở API.
    filename: Mapped[str] = mapped_column( Unicode(255), nullable=False, )

    # Trạng thái hiện tại của job.
    # Có thể dùng các giá trị:
    # - queued
    # - processing
    # - success
    # - failed
    # - cancelled
    status: Mapped[str] = mapped_column( String(32), index=True, nullable=False, )

    # Tiến độ xử lý từ 0 đến 100.
    progress: Mapped[int] = mapped_column( Integer, default=0, nullable=False, )

    # Message ngắn để hiển thị cho client.
    # Ví dụ: "Đang đọc file", "Đang xử lý", "Hoàn thành".
    message: Mapped[str] = mapped_column( Unicode(255), default="", nullable=False, )

    # Số lần worker đã thử xử lý job.
    # Có thể tăng lên khi job bị retry.
    attempts: Mapped[int] = mapped_column( Integer, default=0, nullable=False, )

    # Object key của file upload.
    # Nếu dùng local storage: đây có thể là path tương đối.
    # Nếu dùng S3/MinIO: đây là key trong bucket.
    upload_object_key: Mapped[str] = mapped_column( Text, nullable=False, )

    # Object key của file kết quả.
    # Chỉ có giá trị khi job xử lý thành công.
    result_object_key: Mapped[Optional[str]] = mapped_column( Text, nullable=True, )

    # Nội dung lỗi nếu job thất bại.
    error: Mapped[Optional[str]] = mapped_column( Unicode(500), nullable=True, )

    # trace_id dùng để liên kết log giữa API, worker và notification.
    # Ví dụ một request tạo job có trace_id, worker xử lý cũng ghi log cùng trace_id.
    trace_id: Mapped[str] = mapped_column( String(64), index=True, nullable=False, )

    created_at: Mapped[datetime] = mapped_column( DateTime(timezone=True), default=utc_now, nullable=False, )

    updated_at: Mapped[datetime] = mapped_column( DateTime(timezone=True), default=utc_now, onupdate=utc_now, nullable=False, )

    # Thời điểm job kết thúc.
    # Null nếu job vẫn đang queued hoặc processing.
    finished_at: Mapped[Optional[datetime]] = mapped_column( DateTime(timezone=True), nullable=True, )
