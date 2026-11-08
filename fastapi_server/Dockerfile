# ========= 1) Chọn image Python (slim cho nhẹ) =========
FROM python:3.12-slim AS base

# ========= 1.1) ENV cơ bản =========
# - Không tạo .pyc
# - Log đi thẳng ra stdout (không buffer)
# - Không lưu cache pip (image nhẹ)
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

# ========= 2) Cài gói hệ thống cần thiết =========
# - build-essential: biên dịch một số lib Python (vd: pyodbc)
# - curl, ca-certificates, gnupg: để thêm repo Microsoft và xác thực
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    curl \
    ca-certificates \
    gnupg \
    tzdata \
    && rm -rf /var/lib/apt/lists/*

# ========= 3) Cài ODBC Driver 18 + unixODBC cho SQL Server =========
# Mặc định linux khi sử dụng thư viện msslq thì ko cần phải cài thêm driver hay gì thêm
# Khi sử dụng pyodbc thì cần driver ODBC, mà trong linux không có driver ODBC nên ta sẽ cài ODBC phiên bản 18 (Sửa code khi kết nối sử dụng ODBC driver phiên bản 18)

RUN curl -fsSL https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor -o /usr/share/keyrings/ms-prod.gpg && \
    echo "deb [arch=amd64,arm64 signed-by=/usr/share/keyrings/ms-prod.gpg] https://packages.microsoft.com/debian/12/prod bookworm main" > /etc/apt/sources.list.d/msprod.list && \
    apt-get update && \
    ACCEPT_EULA=Y apt-get install -y --no-install-recommends msodbcsql18 unixodbc-dev && \
    rm -rf /var/lib/apt/lists/*

# ========= 4) Tạo user non-root (an toàn hơn) =========
# Không nên chạy với quyền root (Cao nhất) tránh các thao tác không đáng có, luôn hạn chế quyền
# - Cố định UID/GID = 1000 giúp bind-mount từ host Linux đỡ vướng quyền
RUN groupadd -g 1000 app && \
    useradd -m -u 1000 -g app -s /usr/sbin/nologin app

# ========= 5) Làm việc tại /app =========
WORKDIR /app

# ========= 6) Copy tệp requirements và cài đặt thư viện theo layer cache =========
COPY requirements.txt /app/requirements.txt
RUN pip install --upgrade pip && pip install -r /app/requirements.txt

# ========= 7) Copy source =========
# --chown đảm bảo code thuộc về user "app", own là owner
COPY --chown=app:app . /app

# Chuyển đổi thời gian trong container về múi giờ Vietnam (UTC+7)
RUN ln -sf /usr/share/zoneinfo/Asia/Ho_Chi_Minh /etc/localtime \
    && echo "Asia/Ho_Chi_Minh" > /etc/timezone

# - Thư mục upload mặc định (code đang dùng UPLOAD_DIRECTORY)
# ENV UPLOAD_DIRECTORY=/app/uploads

# Tạo sẵn thư mục upload và gán quyền
RUN install -d -o app -g app /app/uploads /app/update_application /app/log/api_log /app/log/system_log

# Chạy dưới user thường
USER app

# ========= 8) Mở cổng ứng dụng =========
EXPOSE 8000

# ========= 8) Lệnh khởi động =========
CMD ["fastapi", "run", "src/main.py", "--host", "0.0.0.0", "--port", "8000"]
# CMD ["python", "-m", "uvicorn", "src.main:app", "--host", "0.0.0.0", "--port", "8000"]