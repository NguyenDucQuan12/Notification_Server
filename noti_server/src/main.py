from __future__ import annotations
import uvicorn
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.concurrency import asynccontextmanager


from api import noti_api




@asynccontextmanager
async def lifespan(app: FastAPI):
    # Các câu lệnh được thực hiện khi khởi động (Dùng cho việc khởi tạo các mô hình AI)
    print("Khởi tạo Fast API server")
    # Khởi tạo database (nếu có)
    yield
    # Các câu lệnh sau yield được thực hiện khi kết thúc chương trình
    print("Kết thúc Fast API server")
    # Nên thêm hàm đóng kết nối tới Redis


app = FastAPI(
    title="Notification Service",
    version="1.0.0",
    lifespan=lifespan,
    docs_url="/docs",
    redoc_url=None,
    path_prefix="/quan",
)


# Thêm các middleware ở đây nếu cần, ví dụ middleware xác thực, middleware logging, v.v.



# Thêm các endpoint ở đây
app.include_router(noti_api.router)

for route in app.routes:
    print(
        "ROUTE:",
        getattr(route, "path", None),
        type(route).__name__,
    )


@app.get("/")
async def root() -> dict[str, str]:
    """
    Health check đơn giản.
    """

    return {
        "message": "Notification service is running",
    }


# Tạo icon cho trang web api, nó sẽ hiển thị hình ảnh favicon ở thư mục `static/favicon.ico`
# @app.get('/favicon.ico')
# async def favicon():
#     file_name = "favicon.ico"
#     file_path = os.path.join("assets", "images", "static", file_name)
#     return FileResponse(path=file_path, headers={"Content-Disposition": "attachment; filename=" + file_name})


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
    #Thêm tham số log_config= "logs\\logging_config.json" để chuyển các log của uvicorn vào tệp
    uvicorn.run("__main__:app", host="0.0.0.0", port=1234)  # log_config= "logs\\logging_config.json"

    # Hoặc gõ trực tiếp lệnh `fastapi dev src/main.py` để vào chế độ developer
    # Hoặc gõ trực tiếp lệnh `fastapi run src/main.py` để vào chế độ lấy máy chạy làm server

"""

Mẫu kết nối từ phía client:

async function connectUserNotification() {
  const res = await fetch("/notifications/ws-ticket?channel=user", {
    method: "POST",
    headers: {
      Authorization: `Bearer ${localStorage.getItem("access_token")}`,
    },
  });

  const body = await res.json();

  const ticket = body.data.ticket;
  const lastEventId = localStorage.getItem("user_last_event_id") || "0-0";

  const ws = new WebSocket(
    `wss://example.com/ws/events?ticket=${encodeURIComponent(ticket)}&last_event_id=${lastEventId}`
  );

  ws.onmessage = (message) => {
    const event = JSON.parse(message.data);

    console.log("NOTI EVENT:", event);

    if (event.event_id) {
      localStorage.setItem("user_last_event_id", event.event_id);
    }
  };

  return ws;
}

"""