"""
Máy chủ FastAPI chính, chạy các api và các endpoint.
"""
import threading
import os
from contextlib import asynccontextmanager
import uvicorn
from starlette.middleware.base import BaseHTTPMiddleware
from fastapi import FastAPI  # pip install "fastapi[standard]"
from fastapi.responses import FileResponse
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv               # pip install python-dotenv


# Mở comment 3 dòng bên dưới mỗi khi test (Chạy trực tiếp hàm if __main__)
# import os,sys
# PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
# sys.path.append(PROJECT_DIR)

from db.database import close_db, configure_db_executor
from log.system_log import _rotation_thread
from api import auth_api, health_check, role_api, notification_api, bootstrap_api, job_api
from middleware import logger
from middleware.security_guard import security_guard
from services.email_services import InternalEmailSender


load_dotenv()  # Tự động tìm và nạp file .env ở dự án (nằm vị trí gốc của dự án, cùng cấp với src, logs, data,...)

# Khởi tạo email
email_service = InternalEmailSender()


# Các thông tin cấu hình từ tệp env
PORT_HOST = os.getenv("PORT_HOST", "8000")
EMAIL_ADMIN = os.getenv("EMAIL_ADMIN", "nguyenducquan2001@gmail.com")
IP_ADDRESS_HOST = os.getenv("IP_ADDRESS_HOST", "nguyenducquan2001@gmail.com")

# Ép kiểu để port là số nguyên
PORT = int(PORT_HOST)

# Khởi động thread nền tạo file log cho ngày mới của log hệ thống
# (ko gọi trong system_log vì mỗi khi import nó lại mở 1 thread, chỉ nên gọi 1 lần ở main)
log_thread = threading.Thread(target=_rotation_thread, name="SystemLogThread", daemon=True)
log_thread.start()


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Lifespan context manager cho FastAPI. Thực hiện các tác vụ khi khởi động và kết thúc server.
    """
    # Các câu lệnh được thực hiện khi khởi động (Dùng cho việc khởi tạo các mô hình AI)
    print("Khởi tạo Fast API server")
    # Thread pool đủ lớn cho aioodbc (xem giải thích trong db/database.py).
    configure_db_executor()
    # Gửi mail thông báo tới người dùng là đã khởi động FastAPI
    email_service.send_mail_on_startup(to_email= EMAIL_ADMIN, website_name= IP_ADDRESS_HOST)
    # Khởi tạo database (nếu có, chỉ nên khởi tạo 1 lần vì nó làm chậm quá trình khởi động api)
    # await init_db()
    yield
    # Các câu lệnh sau yield được thực hiện khi kết thúc chương trình
    await close_db()
    print("Kết thúc Fast API server")
    # email_service.send_mail_on_shutdown(to_email= EMAIL_ADMIN, website_name= IP_ADDRESS_HOST)

# Khởi tại FastAPi
app = FastAPI(
    docs_url="/docs",        # Đặt đường dẫn Swagger UI thành "/myapi"
    redoc_url=None,           # Tắt Redoc UI
    lifespan= lifespan
)

# Đăng ký middleware bảo vệ (đặt càng sớm càng tốt)
app.middleware("http")(security_guard)

# Đưa middleware ghi log vào app FastAPI
app.add_middleware(BaseHTTPMiddleware, dispatch=logger.log_requests)

# Thêm các endpoint ở đây
app.include_router(health_check.router)
app.include_router(auth_api.router)
app.include_router(role_api.router)
app.include_router(notification_api.router)
app.include_router(bootstrap_api.router)
app.include_router(job_api.router)
# app.include_router(security_admin.router)

# Tạo icon cho trang web api, nó sẽ hiển thị hình ảnh favicon ở thư mục `static/favicon.ico`
@app.get('/favicon.ico')
async def favicon():
    """
    Biểu tượng nhỏ hiển thị trên tab trình duyệt. Đặt tệp favicon.ico trong thư mục static.
    """
    file_name = "favicon.ico"
    static_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "static")
    file_path = os.path.join(static_dir, file_name)
    return FileResponse(path=file_path, media_type="image/x-icon")

# pylint: disable=pointless-string-statement
"""
Cho phép các trang web, app, api trên cùng 1 máy tính có thể truy cập đến api này  
Mặc định các api trên cùng 1 máy không thể chia sẻ tài nguyên cho nhau  
Điều này phục vụ cho mục đích test, vì không thể lúc nào cũng có sẵn 2 máy tính khác nhau để test
"""
origins = [
    "http://localhost:3000",
    "http://172.31.99.130"
]

app.add_middleware(
    CORSMiddleware,
    allow_origins = origins,
    allow_credentials = True,
    allow_methods = ["*"],
    allow_headers = ["*"]
)

if __name__ == "__main__":

    # API_WORKERS > 1: chạy nhiều tiến trình để dùng nhiều nhân CPU (khuyến nghị 2-4 khi có ~2.000 user).
    # Mỗi tiến trình có DB pool riêng (DB_POOL + DB_MAX_OVERFLOW) => tổng kết nối SQL = số worker x pool.
    # Rate-limit / ticket / stream đều nằm trong Redis nên các tiến trình dùng chung được.
    api_workers = int(os.getenv("API_WORKERS", "1"))
    if api_workers > 1:
        # Nhiều worker yêu cầu import string trỏ tới module thật ("main:app"), không dùng được "__main__".
        uvicorn.run("main:app", host="0.0.0.0", port=PORT, workers=api_workers)
    else:
        uvicorn.run("__main__:app", host="0.0.0.0", port=PORT)  # log_config= "logs\\logging_config.json"

    # Hoặc gõ trực tiếp lệnh `fastapi dev fastapi_server/src/main.py` để vào chế độ developer
    # Hoặc gõ trực tiếp lệnh `fastapi run src/main.py` để vào chế độ lấy máy chạy làm server
