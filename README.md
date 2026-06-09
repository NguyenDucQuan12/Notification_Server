# máy chủ thông báo bằng WebSocket
Dự án này hướng dẫn cách người dùng nhận thông báo `realtime` trên ứng dụng python bằng FastAPI và WevSocket.  

Ta có sơ đồ tổng quan quá trình  
Đầu tiên client kết nối đến máy chủ notification để lắng nghe các sự kiện sắp xảy ra
```bash
Client
  |
  | 1. Login username/password
  v
API Server
  |
  | Trả JWT
  v
Client
  |
  | 2. Mở WebSocket với token
  v
Notification Server
  |
  | Xác thực token
  | Đọc Redis Stream theo user
  v
Chờ event realtime
```

Sau đó client đẩy file/công việc lên server thông qua API Server  
```bash
Client
  |
  | 3. Upload file + JWT
  v
API Server
  |
  | Lưu file vào storage
  | Tạo JobRecord trong SQL Server
  | LPUSH job vào Redis Queue
  | XADD event job_queued vào Redis Stream
  v
Redis
  |
  | 4. Notification Server đọc event
  v
Client nhận: "Server đã nhận file"
```

Máy chủ sau khi nhận file thì đẩy lên Queue, sau đó worker tự động lấy các thông tin trên Queue để tiến hành xử lý công việc và cập nhật các thông tin.  
```bash
Worker
  |
  | 5. BRPOP lấy job từ Redis Queue
  | Lấy file từ storage
  | Xử lý file
  | Update SQL Server
  | XADD event job_processing/job_progress/job_success
  v
Redis Stream
  |
  | 6. Notification Server đọc event
  v
Client nhận tiến trình realtime
```

Ta khởi động Redis trước bằng Docker và `Docker-compose`.  
Mở `Docker Desktop` và chạy lệnh sau trên `Terminal`:  
```bash
docker compose up -d
```



Các mục chính trong dự án như sau:  

# I. API Server
API Server sẽ sử dụng FastAPI làm nền tảng, server này chịu trách nhiệm:  
- Cung cấp JWT cho người dùng khi họ login  
- Nhận các tệp tin người dùng tải lên  
- Tạo công việc để các worker xử lý  
- Lưu thông tin vào CSDL SQL Server  
- Đẩy


## 1 Tạo môi trường ảo
Ta chạy lệnh sau để tạo môi trường ảo  
```bash
py -3.12 -m venv .noti_server_venv --prompt="Notification Server"
```

## 2. Cài đặt các thư viện cần thiết


# II. Worker
Worker sẽ là các luồng chờ đợi công việc được đẩy vào Redis Queue từ API Server, và xử lý các công việc đó  
- Chờ job trong Redis Queue  
- Lấy file từ storage  
- Xử lý file  
- Cập nhật trạng thái job trong SQL Server  
- Ghi event tiến trình vào Redis Stream  


# III. Notification Server
Đây là máy chủ thông báo, chịu trách nhiệm gửi các thông báo tới người dùng thông qua WebSocket  
- Giữ WebSocket  
- Xác thực JWT  
- Đọc Redis Stream  
- Gửi event realtime cho đúng user  

# IV. Client
Client sẽ nhận các thông báo từ Server  
- Login lấy token  
- Mở WebSocket tới notification server  
- Upload file qua API server  
- Nhận thông báo realtime  
- Khi job xong thì gọi API lấy result  
