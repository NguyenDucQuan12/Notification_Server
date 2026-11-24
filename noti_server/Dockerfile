# ==============================================================
# Image cho Notification Server
# Build: docker build -t noti-server:2.0.0 ./noti_server
# ==============================================================
FROM python:3.12-slim

# PYTHONDONTWRITEBYTECODE: không sinh file .pyc trong container.
# PYTHONUNBUFFERED      : log in ra ngay, Docker thu được log realtime.
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    TZ=Asia/Ho_Chi_Minh \
    PORT=8000

# tzdata: múi giờ; curl: dùng cho healthcheck.
RUN apt-get update && apt-get install -y --no-install-recommends tzdata curl \
    && rm -rf /var/lib/apt/lists/*

# Chạy bằng user thường, KHÔNG chạy bằng root (nguyên tắc bảo mật cơ bản).
RUN groupadd -g 1000 app && useradd -m -u 1000 -g app -s /usr/sbin/nologin app

WORKDIR /app

# Copy requirements trước để tận dụng cache: sửa code không phải cài lại thư viện.
COPY requirements.txt /app/requirements.txt
RUN pip install -r /app/requirements.txt

COPY --chown=app:app src /app/src

USER app
WORKDIR /app/src
EXPOSE 8000

# Docker tự đánh dấu container "unhealthy" nếu /health/live lỗi.
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD curl -fsS "http://localhost:${PORT}/health/live" || exit 1

# Chạy qua main.py (thay vì "uvicorn main:app") để áp dụng các tham số
# ws_max_size, ping, graceful shutdown... đọc từ config.
CMD ["python", "main.py"]
