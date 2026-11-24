# Notification Server — Tài liệu chi tiết

Mục lục
- [I. Notification Server làm gì?](#i-notification-server-làm-gì)
- [II. Vì sao dùng Redis Stream và WebSocket ticket?](#ii-vì-sao-dùng-redis-stream-và-websocket-ticket)
- [III. Bức tranh tổng thể và luồng của 1 kết nối](#iii-bức-tranh-tổng-thể-và-luồng-của-1-kết-nối)
- [IV. Cấu trúc mã nguồn và cách chạy](#iv-cấu-trúc-mã-nguồn-và-cách-chạy)
- [V. Giải thích từng file, từng hàm](#v-giải-thích-từng-file-từng-hàm)
  - [1. main.py](#1-srcmainpy--điểm-khởi-động)
  - [2. config/configs.py](#2-srcconfigconfigspy--cấu-hình)
  - [3. core/logging_config.py](#3-srccorelogging_configpy--log)
  - [4. api/noti_api.py](#4-srcapinoti_apipy--4-endpoint-websocket)
  - [5. api/health_api.py](#5-srcapihealth_apipy--kiểm-tra-sức-khỏe)
  - [6. controllers/noti_controller.py](#6-srccontrollersnoti_controllerpy--vòng-đời-1-kết-nối)
  - [7. services/ — giải thích TỪNG DÒNG](#7-srcservices--giải-thích-từng-dòng)
  - [8. utils/](#8-srcutils--các-hàm-tiện-ích)
- [VI. Hợp đồng message, close code](#vi-hợp-đồng-message-gửi-cho-client-và-close-code)
- [VII. Lịch sử sửa lỗi và hướng mở rộng](#vii-lịch-sử-sửa-lỗi-và-hướng-mở-rộng)

---

# I. Notification Server làm gì?

Notification Server (NS) là **trạm trung chuyển** giữa Redis và client:

```
API Server / Worker  ──XADD──►  Redis Stream  ──XREAD──►  Notification Server  ──WebSocket──►  Client
   (ghi thông báo)                (cuốn sổ)                 (đọc + phát lại)                  (app, web)
```

NS chỉ làm 3 việc:
1. **Xác thực** người kết nối bằng *WebSocket ticket* (do API Server cấp, lưu trong Redis).
2. **Đọc** event mới trong Redis Stream của người đó.
3. **Đẩy** event xuống client qua WebSocket, kèm gửi lại (replay) các event client bỏ lỡ khi mất mạng.

NS **không** truy cập SQL Server, **không** tạo thông báo, **không** tạo ticket. Đó là việc của API Server
(`fastapi_server`). Vì vậy muốn app nhận được thông báo thì phải có ai đó gọi `POST /notifications/send`
của API Server (hoặc worker ghi event vào stream).

---

# II. Vì sao dùng Redis Stream và WebSocket ticket?

## 1. Redis Stream thay vì Redis Pub/Sub
Redis có 2 cách đẩy thông báo: `Redis Stream` và `Redis Pub/Sub`.

- **Redis Stream** giống một **cuốn sổ**: mỗi event được ghi lại kèm một ID tăng dần (vd `1713000000000-0`).
  Client mất kết nối rồi quay lại chỉ cần nói "tôi đã đọc tới ID X", server đọc tiếp từ X → **không mất event**.
  Stream hỗ trợ `XREAD`, `XRANGE`, `XREVRANGE` và cắt bớt (`MAXLEN`) để không phình vô hạn.
- **Redis Pub/Sub** giống một **loa phát thanh**: phát xong là mất. Client đang mất mạng đúng lúc phát thì
  không bao giờ nhận lại được.

→ Chọn **Redis Stream**.

## 2. WebSocket ticket thay vì gửi JWT trên URL
Trình duyệt không cho gắn header `Authorization: Bearer ...` khi mở WebSocket, nên token phải nằm trên URL
(`ws://.../ws/events?token=...`). Việc này dễ lộ token vì URL bị ghi vào access log của Nginx/load balancer,
lịch sử trình duyệt, công cụ giám sát...

Giải pháp: **ticket** = tấm vé vào cửa, sống rất ngắn (60 giây) và **chỉ dùng được 1 lần**.

```
Bước 1  Client đăng nhập API Server → có access token (JWT)
Bước 2  Client gọi  POST /notifications/ws-ticket?channel=user   (kèm header Authorization)
Bước 3  API kiểm tra JWT, tạo ticket ngẫu nhiên, lưu Redis:  ws:ticket:<ticket> = {tenant_id, user_id, permissions...}  TTL 60s
Bước 4  Client mở WebSocket:  ws://127.0.0.1:1234/noti/ws/events?ticket=<ticket>&last_event_id=<ID cuối đã nhận>
Bước 5  NS lấy-và-xóa ticket trong Redis (GETDEL) → biết người này là ai, có quyền gì
Bước 6  Hợp lệ → giữ kết nối lâu dài.  Sai/hết hạn → đóng với mã 4401
```

Ticket hết hạn **không** làm WebSocket đang mở bị ngắt. Ticket chỉ là vé vào cửa; đã vào phòng thì không
cần đưa vé nữa. Chỉ khi mất kết nối và muốn vào lại mới phải xin vé mới.

---

# III. Bức tranh tổng thể và luồng của 1 kết nối

## 1. Kiến trúc bên trong 1 process

```
                     ┌─────────────────────────── Notification Server (1 process) ───────────────────────────┐
                     │                                                                                       │
 Redis Streams       │   StreamHub (services/stream_hub.py)              Mỗi WebSocket (noti_controller.py)  │
┌───────────────┐    │  ┌──────────────────────────────┐                ┌───────────────────────────────┐   │
│ ...:user:u1   │◄───┼──│ Shard 0  XREAD BLOCK nhiều   │──offer()──►    │ queue ─► sender ─► client A   │   │
│ ...:user:u2   │    │  │          stream trong 1 lệnh │                │          receiver ◄─ ping     │   │
│ ...:dashboard │◄───┼──│ Shard 1, 2, 3 ...            │──offer()──►    │ queue ─► sender ─► client B   │   │
│ stream:global │    │  └──────────────────────────────┘                └───────────────────────────────┘   │
└───────────────┘    │                                                                                       │
                     │   ConnectionManager: đếm/giới hạn kết nối, thống kê, đóng tất cả khi tắt              │
                     └───────────────────────────────────────────────────────────────────────────────────────┘
```

- Cả process chỉ có `HUB_SHARDS` (mặc định 4) kết nối Redis để đọc event, **dù có 10.000 WebSocket**.
- Mỗi WebSocket có một **hàng đợi trong RAM** (`asyncio.Queue`). Shard đọc được event nào thì thả vào hàng
  đợi của những WebSocket đang nghe stream đó.

## 2. Hành trình của 1 kết nối, từ đầu tới cuối

```
Client mở ws://127.0.0.1:1234/noti/ws/events?ticket=T&last_event_id=L
  │
  ▼ api/noti_api.py :: websocket_user_events → _serve_channel
  1. websocket.accept()
  2. utils/auth.py :: authenticate_websocket
       check_origin → lấy ticket → services/ws_ticket_service.consume_ws_ticket (GETDEL)
       → Identity → kiểm tra channel, job_id, permission
  3. Tính stream key từ Identity:  stream:noti:tenant:{tenant}:user:{user}
  │
  ▼ controllers/noti_controller.py :: run_stream_websocket
  4. connection_manager.register  (vượt giới hạn → đóng 1013 / 4429)
  5. Chạy song song 2 task:
       sender  : stream_hub.subscribe → gửi "connected" → replay_stream(L) → stream_live_events (vòng lặp)
       receiver: đọc tin client gửi lên ("ping" → "pong"; client đóng → kết thúc)
  6. Task nào xong trước → hủy task kia → chọn close code → safe_close
  7. stream_hub.unsubscribe + connection_manager.unregister + ghi log tổng kết
```

## 3. Vì sao không mất, không trùng event giữa "replay" và "live"?
Thứ tự trong `_sender` là: **subscribe trước, replay sau, rồi mới đọc hàng đợi**.

```
Thời gian ──────────────────────────────────────────────────────────►
            subscribe()                 replay xong (cursor = R)
                │                              │
 Event 1..R     │ ← replay gửi (đọc từ Redis)  │
 Event mới      │ ─────── vào hàng đợi ───────────────────► live gửi
                                                Bỏ qua event có ID <= R (đã gửi lúc replay)
```

- Event có trước lúc replay đọc → replay gửi.
- Event đến sau lúc subscribe → nằm trong hàng đợi.
- Phần giao nhau (event đến trong lúc replay) → có ở cả hai nơi → `stream_live_events` bỏ qua event có
  `stream_id <= cursor`. Kết quả: không sót, không trùng.

---

# IV. Cấu trúc mã nguồn và cách chạy

```
noti_server/
├── Dockerfile, requirements.txt, .env.example
├── DEMO.md                            # Hướng dẫn demo từng bước
├── demo/noti_demo.py, demo/demo.html  # Công cụ demo: ticket / listen / publish / load
└── src/
    ├── main.py                        # Tạo app FastAPI, lifespan (khởi động / tắt an toàn)
    ├── config/configs.py              # Đọc cấu hình từ biến môi trường + kiểm tra chéo
    ├── core/logging_config.py         # Log dạng chữ/JSON, tự che ticket/token
    ├── api/noti_api.py                # 4 endpoint WebSocket
    ├── api/health_api.py              # /health/live, /health/ready, /health/stats
    ├── controllers/noti_controller.py # Vòng đời 1 kết nối: replay + live + nhận ping
    ├── services/redis_client.py       # 2 pool Redis: lệnh nhanh và XREAD BLOCK
    ├── services/ws_ticket_service.py  # GETDEL ticket (nguyên tử, dùng 1 lần)
    ├── services/connection_manager.py # Giới hạn kết nối, thống kê, đóng khi tắt
    ├── services/stream_hub.py         # Đọc Redis Stream dùng chung cho mọi WebSocket
    └── utils/                         # auth, close_codes, events, redis_keys, utils
```

Cách chạy:
```bash
# Local (Redis >= 6.2, khuyến nghị 7.x). Đứng tại noti_server/src:
python main.py
#   → lắng nghe ws://127.0.0.1:1234/noti/...  (cấu hình đọc từ env/.env.common + env/.env.noti)

# Docker (từ thư mục gốc dự án)
docker compose up -d --build noti_server
```

Kiểm tra: `GET http://127.0.0.1:1234/health/ready` phải trả `{"status": "ready", ...}`.

---

# V. Giải thích từng file, từng hàm

## 1. `src/main.py` — điểm khởi động

| Thành phần | Giải thích |
|---|---|
| `setup_logging(...)` (dòng 25) | Cấu hình log **trước** khi import các module khác, để log phát sinh lúc import cũng đúng định dạng. Vì vậy các import phía sau có `# noqa: E402`. |
| `lifespan(_app)` | Hàm vòng đời của FastAPI. Phần **trước `yield`** chạy 1 lần lúc khởi động: ghi log phiên bản, `ping_redis()` để cảnh báo sớm nếu Redis chưa sống (chỉ cảnh báo, không dừng server vì Redis có thể khởi động chậm hơn trong docker-compose), rồi `stream_hub.start()` khởi chạy các shard. Phần **sau `yield`** chạy khi tắt (Ctrl+C, `docker stop`): (1) `connection_manager.shutdown()` đóng mọi WebSocket với mã 1012 để client reconnect ngay; (2) `stream_hub.stop()` dừng shard; (3) `close_redis_clients()` đóng kết nối Redis. Thứ tự này quan trọng: đóng client trước, rồi mới cắt nguồn dữ liệu. |
| `app = FastAPI(...)` | Tạo ứng dụng. `root_path` dùng khi chạy sau reverse proxy có tiền tố. `/docs` tự tắt ở production (`settings.docs_enabled`). |
| `CORSMiddleware` | Chỉ có tác dụng với HTTP (`/health`), **không** áp dụng cho WebSocket. WebSocket được bảo vệ bằng ticket + kiểm tra Origin. |
| `include_router(...)` | Gắn router health và router WebSocket. |
| `root()` | `GET /` trả `{"service": ..., "status": "running"}` để kiểm tra nhanh. |
| `if __name__ == "__main__": uvicorn.run(...)` | Chạy server. `ws_max_size` chặn message lớn hơn 64 KB; `ws_ping_interval/ws_ping_timeout`: uvicorn tự ping ở tầng giao thức để phát hiện kết nối chết; `timeout_graceful_shutdown` không cho việc tắt server treo vô hạn; `proxy_headers` lấy IP thật khi chạy sau proxy. |

## 2. `src/config/configs.py` — cấu hình

Lớp `Settings(BaseSettings)` của **pydantic-settings**: mỗi thuộc tính là một biến môi trường cùng tên.
Thứ tự ưu tiên (cao → thấp): biến môi trường thật → `.env` ở thư mục chạy lệnh → `env/.env.noti` →
`env/.env.common` → giá trị mặc định trong code.

| Nhóm | Biến quan trọng | Ý nghĩa |
|---|---|---|
| Ứng dụng | `ENVIRONMENT`, `HOST`, `PORT=1234`, `ROOT_PATH`, `ENABLE_DOCS` | Môi trường chạy, cổng, tiền tố proxy, bật/tắt Swagger |
| Log | `LOG_LEVEL`, `LOG_JSON` | Mức log; log JSON (mặc định bật ở production) |
| Redis | `REDIS_URL` | **Phải trùng** với Redis mà API Server ghi vào |
| | `REDIS_CONNECT_TIMEOUT`, `REDIS_SOCKET_TIMEOUT` | Timeout mở kết nối / lệnh nhanh |
| | `REDIS_MAX_CONNECTIONS`, `REDIS_POOL_TIMEOUT` | Số kết nối tối đa của pool "fast" và thời gian chờ khi pool bận |
| | `REDIS_STREAM_SOCKET_TIMEOUT` | Timeout socket cho XREAD BLOCK, phải > `HUB_BLOCK_MS/1000 + 1` |
| Stream Hub | `HUB_SHARDS` | Số shard đọc song song (= số kết nối Redis để đọc event) |
| | `HUB_BLOCK_MS` | Mỗi lệnh XREAD chờ tối đa bao lâu |
| | `STREAM_READ_COUNT` | Số event tối đa đọc mỗi stream trong 1 lần XREAD |
| | `HUB_SUBSCRIBER_QUEUE_SIZE` | Kích thước hàng đợi mỗi WebSocket; đầy → ngắt 4408 |
| | `HEARTBEAT_INTERVAL_SECONDS` | Bao lâu không có event thì gửi heartbeat |
| | `REPLAY_MAX_EVENTS` | Số event cũ tối đa gửi lại khi reconnect (0 = tắt replay) |
| WebSocket | `WS_MAX_CONNECTIONS`, `WS_MAX_CONNECTIONS_PER_USER` | Giới hạn tổng / mỗi user (theo từng process) |
| | `WS_ALLOWED_ORIGINS` | Danh sách Origin được phép (chống CSWSH); rỗng = không kiểm tra |
| | `WS_SEND_TIMEOUT_SECONDS` | Gửi 1 message quá lâu → coi là client chậm, ngắt 4408 |
| | `WS_MAX_MESSAGE_SIZE`, `WS_PING_INTERVAL`, `WS_PING_TIMEOUT`, `GRACEFUL_SHUTDOWN_SECONDS` | Truyền cho uvicorn |
| CORS | `CORS_ALLOWED_ORIGINS` | Chỉ cho HTTP |

Các hàm/thuộc tính:

| Hàm | Giải thích |
|---|---|
| `is_production` | `True` nếu `ENVIRONMENT == "production"`. |
| `docs_enabled` | Nếu `ENABLE_DOCS` không đặt → bật docs khi **không** phải production; nếu đặt thì theo giá trị đó. |
| `log_json` | Tương tự: không đặt → JSON khi production. |
| `ws_allowed_origins` / `cors_allowed_origins` | Tách chuỗi CSV thành set/list bằng `_split_csv`. |
| `redis_stream_max_connections` | `HUB_SHARDS + 2`: mỗi shard 1 kết nối + 2 dự phòng. |
| `_validate_consistency()` | Chạy sau khi đọc xong cấu hình. Báo lỗi ngay khi khởi động nếu `REDIS_STREAM_SOCKET_TIMEOUT <= HUB_BLOCK_MS/1000 + 1` (nếu không, mỗi lần XREAD chờ hết giờ mà không có event thì redis-py sẽ báo timeout và shard phải kết nối lại liên tục) hoặc `WS_MAX_CONNECTIONS_PER_USER > WS_MAX_CONNECTIONS`. |
| `_split_csv(value)` | `"a, b ,c/"` → `{"a", "b", "c"}`: bỏ khoảng trắng, phần tử rỗng và dấu `/` cuối. |
| `settings = Settings()` | Tạo 1 lần khi import. Cấu hình sai → lỗi ngay tại đây, server không khởi động. |

## 3. `src/core/logging_config.py` — log

| Hàm / lớp | Giải thích |
|---|---|
| `_STANDARD_ATTRS` | Tập tên thuộc tính có sẵn của `LogRecord`. Thuộc tính nào **không** nằm trong tập này là dữ liệu mình truyền qua `extra={...}`. |
| `_SECRET_PATTERN`, `_redact(text)` | Regex tìm `ticket=...`, `token=...`, `access_token=...` và thay giá trị bằng `***`. Lý do: uvicorn tự log URL WebSocket **kèm query string**, tức là ghi cả ticket. |
| `_extra_fields(record)` | Lấy các field `extra=` có kiểu cơ bản (str, int, list...). Bỏ object nội bộ của uvicorn để log không bị nhiễu. |
| `RedactSecretsFilter.filter()` | Chạy trước khi ghi mỗi dòng log: nếu message chứa bí mật thì thay bằng bản đã che và xóa `args` để formatter không format lại. Luôn trả `True` (không chặn log, chỉ sửa nội dung). Gắn vào **handler** nên log của uvicorn cũng đi qua. |
| `JsonFormatter.format()` | Mỗi log là 1 dòng JSON `{"ts", "level", "logger", "msg", ...extra, "exc_info"}`, dễ đẩy vào ELK/Loki. |
| `TextFormatter.format()` | Dạng `2026-11-24 10:00:00 \| INFO \| noti.ws \| WebSocket connected \| conn_id=ab12 user_id=7`. |
| `setup_logging(level, json_logs)` | Cấu hình bằng `dictConfig`: 1 handler ghi ra stdout (Docker thu log từ stdout), gắn filter che bí mật. Logger của uvicorn dùng chung handler, `propagate=False` để không in 2 lần. Logger `redis` để WARNING vì ở DEBUG nó in rất nhiều. |
| `UVICORN_LOG_CONFIG = None` | Truyền cho `uvicorn.run(log_config=None)` để uvicorn **không** ghi đè cấu hình log của mình. |

## 4. `src/api/noti_api.py` — 4 endpoint WebSocket

| Đường dẫn | channel trong ticket | Quyền cần có | Redis Stream nghe |
|---|---|---|---|
| `/noti/ws/events` | `user` | `notification:subscribe` | `stream:noti:tenant:{t}:user:{u}` |
| `/noti/ws/tenant-dashboard` | `tenant_dashboard` | `notification:tenant_dashboard` | `stream:noti:tenant:{t}:dashboard` |
| `/noti/ws/jobs/{job_id}` | `job` | `notification:job_watch` | `stream:noti:tenant:{t}:job:{job_id}` |
| `/noti/ws/global-events` | `global` | `notification:global_subscribe` | `stream:noti:global` |

Superuser có mọi quyền.

| Hàm | Giải thích |
|---|---|
| `_serve_channel(websocket, channel, permission, stream_key_of, job_id)` | Luồng chung của cả 4 endpoint. (1) `accept()` **trước** khi xác thực để có thể đóng với close code có nghĩa (4401, 4403...); nếu từ chối trước khi accept thì client chỉ thấy lỗi HTTP 403 chung chung. (2) `authenticate_websocket(...)`; lỗi → `record_rejection` để thống kê, ghi log, `safe_close` với code/reason của lỗi. (3) Tính stream key bằng `stream_key_of(identity)`: stream key được tính **từ Identity trong ticket**, không lấy từ input của client, nên không thể nghe trộm stream của người khác. (4) Giao cho `NotiController.run_stream_websocket`. |
| `_client_addr(websocket)` | Trả `"ip:port"` của client để ghi log. |
| `websocket_user_events` | Endpoint chính của người dùng: nghe stream riêng của chính mình. |
| `websocket_tenant_dashboard` | Admin xem mọi event trong tenant. |
| `websocket_job_events(job_id)` | Kiểm tra `job_id` bằng regex `_JOB_ID_RE` (1–200 ký tự, không khoảng trắng/ký tự điều khiển); sai → đóng 4400. Ticket phải được cấp cho **đúng** `job_id` này. |
| `websocket_global_events` | Event toàn hệ thống cho đội vận hành. |

## 5. `src/api/health_api.py` — kiểm tra sức khỏe

| Endpoint | Giải thích |
|---|---|
| `GET /health/live` → `liveness()` | Process còn chạy không. **Không** kiểm tra Redis, vì nếu endpoint này lỗi, Docker/Kubernetes sẽ khởi động lại container, mà restart thì Redis cũng không sống lại. |
| `GET /health/ready` → `readiness()` | Sẵn sàng nhận kết nối mới chưa: Redis PING được **và** server không đang tắt. Không sẵn sàng → HTTP 503 để load balancer tạm ngừng chuyển client mới vào. |
| `GET /health/stats` → `stats()` | Số liệu vận hành: số kết nối theo channel, số user, bộ đếm từ chối, số event đã gửi, trạng thái từng shard của Stream Hub. Nên chặn endpoint này ở proxy, chỉ cho mạng nội bộ truy cập. |

## 6. `src/controllers/noti_controller.py` — vòng đời 1 kết nối

### Hằng số và exception
| Tên | Giải thích |
|---|---|
| `_CLIENT_MSG_LIMIT = 30`, `_CLIENT_MSG_WINDOW_SECONDS = 10` | Client gửi quá 30 message trong 10 giây → bị ngắt (chống spam). |
| `SendTimeout` | Gửi 1 message quá `WS_SEND_TIMEOUT_SECONDS`. |
| `SlowConsumer` | Hàng đợi của client trong Hub bị đầy. |
| `ClientMessageFlood` | Client gửi quá nhiều message. |

### Lớp `_ClientSession`
Bọc WebSocket của 1 client.

| Hàm | Giải thích |
|---|---|
| `__init__` | Giữ `websocket`, `info` (ConnectionInfo) và một `asyncio.Lock` để gửi. Cần khóa vì có **2 task cùng gửi** (sender gửi event, receiver gửi pong); gửi đồng thời có thể làm lẫn frame. |
| `send_json(data, is_event)` | JSON hóa dict bằng `safe_json_dumps` rồi gọi `send_text`. |
| `send_text(text, is_event)` | Lấy khóa → gửi với `asyncio.wait_for(..., timeout=WS_SEND_TIMEOUT_SECONDS)`; quá giờ → ném `SendTimeout`. Nếu là event nghiệp vụ thì tăng `info.events_sent` để thống kê. |

### Lớp `NotiController` (toàn bộ là `@staticmethod`)

| Hàm | Giải thích |
|---|---|
| `safe_close(websocket, code, reason)` | Đóng WebSocket mà **không bao giờ ném lỗi**: đã đóng thì bỏ qua; có timeout 2 giây để không treo khi mạng client đã chết; `reason` được cắt còn tối đa 123 byte (giới hạn của chuẩn WebSocket). |
| `_detect_trimmed_history(stream_key, last_event_id)` | Phát hiện event client **chưa nhận nhưng đã bị Redis xóa** (do giới hạn `MAXLEN` khi XADD). Gọi `XINFO STREAM`, lấy `max-deleted-entry-id` (ID lớn nhất từng bị xóa, có từ Redis 7.0). Nếu ID đó > `last_event_id` thì client đã mất event → trả `True`. Stream chưa tồn tại → `ResponseError` → `False`. |
| `replay_stream(session, stream_key, last_event_id, channel)` | Gửi lại event có ID **lớn hơn** `last_event_id`. Cách làm: `XREVRANGE` (đọc ngược từ mới về cũ) với `min="(" + last_event_id` (dấu `(` nghĩa là **không** lấy chính nó) và `count = REPLAY_MAX_EVENTS + 1`. Lấy được N+1 dòng → client bỏ lỡ quá nhiều (`truncated`), chỉ giữ N dòng mới nhất. Đảo ngược để gửi theo thứ tự cũ → mới. Nếu client **đã có** cursor (khác `0-0`) và bị thiếu (`limit` hoặc `trimmed`) thì gửi trước một message `replay_incomplete` để client gọi REST API đồng bộ lại. Gửi từng event, cập nhật `cursor`. Trả `cursor` cho phần live. Trường hợp `REPLAY_MAX_EVENTS = 0`: không replay, trả ID mới nhất hiện có để chỉ nhận event mới. |
| `stream_live_events(session, subscription, cursor, channel)` | Vòng lặp vô hạn: (1) hàng đợi đã tràn → ném `SlowConsumer`; (2) chờ `queue.get()` tối đa `HEARTBEAT_INTERVAL_SECONDS`; hết giờ → gửi heartbeat (kèm `stream_status` ok/degraded) rồi lặp lại; (3) có event → nếu `stream_id <= cursor` thì bỏ qua (đã gửi lúc replay), ngược lại gửi chuỗi JSON có sẵn và cập nhật `cursor`. |
| `_sender(session, identity, stream_key, channel, last_event_id)` | Task gửi: `stream_hub.subscribe` (**trước**) → gửi `connected` → `replay_stream` → `stream_live_events`. Khối `finally` luôn gọi `stream_hub.unsubscribe` để Hub không giữ hàng đợi "mồ côi", kể cả khi task bị hủy. |
| `_receiver(session)` | Task nhận: `websocket.receive()` liên tục. `websocket.disconnect` → return (client đóng). Đếm số message trong cửa sổ 10 giây, vượt 30 → `ClientMessageFlood`. Nhận được `"ping"` hoặc `{"type":"ping"}` (text hoặc bytes) → trả `pong`. Message khác bị bỏ qua. Có receiver thì server biết **ngay** khi client đóng tab, thay vì chờ tới lần gửi heartbeat kế tiếp. |
| `_is_ping(text)` | `True` nếu text là `ping` (không phân biệt hoa thường) hoặc JSON ≤ 256 ký tự có `type == "ping"`. |
| `run_stream_websocket(websocket, identity, stream_key, channel)` | Hàm chính (xem mục III.2). Tạo `connection_id` 12 ký tự để gắn vào mọi dòng log. `connection_manager.register` → bị từ chối thì đóng với code tương ứng. Chuẩn hóa `last_event_id` từ query. Tạo 2 task `sender` và `receiver`, `asyncio.wait(FIRST_COMPLETED)`. Phân loại nguyên nhân kết thúc → close code: kết thúc bình thường hoặc `WebSocketDisconnect` → không cần đóng; `SendTimeout`/`SlowConsumer` → 4408; `ClientMessageFlood` → 1008; `RedisError` → gửi `server_error` rồi 1013; client đã ngắt sẵn → bỏ qua; lỗi lạ → log đầy đủ traceback, gửi thông báo chung chung, 1011. `finally`: hủy task còn chạy, `gather` để chờ chúng dừng hẳn, đóng kết nối nếu cần, `unregister`, ghi log tổng kết (close code, số event, thời lượng). |
| `_is_disconnected(websocket)` | `True` nếu phía client **hoặc** phía ứng dụng đã ở trạng thái DISCONNECTED. |
| `_try_send_error(session, message, code)` | Cố gửi `server_error`; lỗi thì bỏ qua (dù sao cũng sắp đóng). |

## 7. `src/services/` — giải thích TỪNG DÒNG

Ở mỗi đoạn code bên dưới, comment gốc trong source đã được bỏ bớt và thay bằng chú thích `# ◄ ...`
giải thích dòng đó làm gì. Số dòng trong tiêu đề là số dòng thật trong file.

### 7.1. `services/redis_client.py` — 2 pool kết nối Redis

**Ý tưởng:** tách lệnh nhanh (GETDEL, PING, XREVRANGE) và lệnh chờ lâu (XREAD BLOCK) ra 2 pool riêng, vì
chúng cần timeout khác nhau. Nếu dùng chung, timeout ngắn sẽ làm XREAD BLOCK lỗi liên tục, còn timeout dài
sẽ làm lệnh nhanh treo lâu khi Redis có sự cố.

#### Pool "fast" (dòng 38–49)
```python
_fast_pool = redis_async.BlockingConnectionPool.from_url(  # ◄ Tạo pool kiểu "Blocking": khi hết kết nối rảnh thì CHỜ, không lỗi ngay
    settings.REDIS_URL,                                    # ◄ Địa chỉ Redis, vd redis://127.0.0.1:6379/0
    decode_responses=True,                                 # ◄ Redis trả str thay vì bytes (dữ liệu là JSON/văn bản)
    socket_keepalive=True,                                 # ◄ Bật TCP keepalive: hệ điều hành tự phát hiện kết nối chết
    socket_connect_timeout=settings.REDIS_CONNECT_TIMEOUT, # ◄ Mở kết nối TCP quá 2s thì lỗi
    socket_timeout=settings.REDIS_SOCKET_TIMEOUT,          # ◄ Mỗi lệnh chờ trả lời tối đa 3s (lệnh nhanh, không nên lâu)
    max_connections=settings.REDIS_MAX_CONNECTIONS,        # ◄ Pool có tối đa 50 kết nối
    timeout=settings.REDIS_POOL_TIMEOUT,                   # ◄ Hết 50 kết nối: chờ tối đa 5s để có kết nối rảnh, quá thì lỗi
    health_check_interval=30,                              # ◄ Kết nối rảnh > 30s sẽ được PING lại trước khi dùng (tránh dùng kết nối đã chết)
)
redis_fast_client: Redis = Redis.from_pool(_fast_pool)     # ◄ Client "sở hữu" pool: khi gọi aclose() client thì pool cũng đóng theo
```
Vì sao `BlockingConnectionPool`? Sau khi deploy hoặc sau sự cố mạng, hàng nghìn client reconnect cùng lúc.
Pool thường sẽ báo "Too many connections" ngay; pool blocking cho request **xếp hàng** chờ vài giây nên
hầu hết vẫn thành công.

#### Pool "stream" (dòng 52–60)
```python
redis_stream_client: Redis = redis_async.from_url(          # ◄ Client + pool thường, CHỈ dùng cho XREAD BLOCK của Stream Hub
    settings.REDIS_URL,                                     # ◄ Cùng Redis
    decode_responses=True,                                  # ◄ Trả str
    socket_keepalive=True,                                  # ◄ Giữ TCP sống
    socket_connect_timeout=settings.REDIS_CONNECT_TIMEOUT,  # ◄ Mở kết nối tối đa 2s
    socket_timeout=settings.REDIS_STREAM_SOCKET_TIMEOUT,    # ◄ 10s: PHẢI lớn hơn thời gian BLOCK (1s), nếu không lệnh XREAD đang chờ hợp lệ bị coi là timeout
    max_connections=settings.redis_stream_max_connections,  # ◄ HUB_SHARDS + 2 = 6: mỗi shard chỉ cần 1 kết nối
    health_check_interval=30,                               # ◄ PING lại kết nối rảnh lâu
)
```
Đây là chìa khóa để phục vụ nhiều client: số kết nối Redis để đọc event **không phụ thuộc** số WebSocket.

Tạo client **chưa** mở kết nối. Kết nối chỉ mở ở lệnh đầu tiên, nên import module này khi Redis chưa
chạy cũng không lỗi.

#### `get_redis_fast()` / `get_redis_stream()` (dòng 63–70)
```python
def get_redis_fast() -> Redis:
    return redis_fast_client     # ◄ Trả về client đã tạo sẵn (singleton), KHÔNG tạo mới mỗi lần gọi
def get_redis_stream() -> Redis:
    return redis_stream_client   # ◄ Tương tự cho client stream
```
Dùng hàm thay vì import biến trực tiếp để sau này dễ thay thế (vd tiêm client giả khi viết test).

#### `ping_redis()` (dòng 73–83)
```python
async def ping_redis() -> bool:
    try:
        return bool(await redis_fast_client.ping())  # ◄ Gửi PING; Redis sống trả True → ép về bool cho chắc
    except Exception as exc:                         # ◄ Bắt MỌI lỗi (timeout, mất kết nối, sai mật khẩu...)
        logger.warning("Redis ping failed", extra={"error": repr(exc)})  # ◄ Ghi lý do vào log
        return False                                 # ◄ Trả False thay vì ném lỗi → /health/ready luôn trả lời được (503)
```

#### `close_redis_clients()` (dòng 86–97)
```python
async def close_redis_clients() -> None:
    for name, client in (("fast", redis_fast_client), ("stream", redis_stream_client)):  # ◄ Lặp qua 2 client
        try:
            await client.aclose()        # ◄ Đóng client và toàn bộ kết nối trong pool. redis.asyncio bắt buộc đóng rõ ràng,
                                         #   nếu không Python cảnh báo "Unclosed connection"
        except Exception as exc:         # ◄ Đang tắt server: lỗi khi đóng cũng không được làm hỏng quá trình tắt
            logger.warning("Error closing Redis client", extra={"client": name, "error": repr(exc)})  # ◄ Chỉ ghi log
```

---

### 7.2. `services/ws_ticket_service.py` — tiêu thụ ticket

**Phân chia trách nhiệm:** API Server **tạo** ticket (`ws:ticket:<ticket>`, TTL 60s); NS chỉ **đọc + xóa**.

#### Hằng số và exception (dòng 34–45)
```python
_MAX_TICKET_LENGTH = 256                     # ◄ Ticket thật (token_urlsafe(32)) dài ~43 ký tự. Chặn chuỗi rác/khổng lồ trước khi gửi vào Redis

class TicketServiceUnavailable(Exception):   # ◄ Lỗi riêng cho trường hợp "Redis hỏng" (lỗi server)
    ...                                      #   để phân biệt với "ticket sai" (lỗi client):
                                             #   ticket sai → đóng 4401, client xin ticket mới
                                             #   Redis hỏng → đóng 1013, client thử lại sau vài giây
```

#### `consume_ws_ticket(ticket)` (dòng 48–84)
```python
async def consume_ws_ticket(ticket: str) -> dict[str, Any] | None:
    if not ticket or len(ticket) > _MAX_TICKET_LENGTH:    # ◄ Ticket rỗng hoặc quá dài → chắc chắn không hợp lệ
        return None                                       # ◄ None = "ticket không hợp lệ"

    try:
        raw = await get_redis_fast().getdel(ws_ticket_key(ticket))
                                                          # ◄ ws_ticket_key("abc") = "ws:ticket:abc"
                                                          # ◄ GETDEL = đọc giá trị VÀ xóa key trong MỘT lệnh nguyên tử (Redis >= 6.2)
                                                          #   Redis xử lý lệnh tuần tự → nếu 2 kết nối dùng cùng 1 ticket
                                                          #   thì chỉ 1 kết nối nhận được payload, kết nối kia nhận None
    except RedisError as exc:                             # ◄ Redis lỗi (mất kết nối, timeout...)
        logger.error("Cannot read ws ticket from Redis", extra={"error": repr(exc)})  # ◄ Ghi log cho đội vận hành
        raise TicketServiceUnavailable("redis_unavailable") from exc                   # ◄ Báo "lỗi server", giữ lỗi gốc để truy vết

    if not raw:                                           # ◄ Key không tồn tại: ticket sai, đã dùng rồi, hoặc hết 60s
        return None

    try:
        payload = json.loads(raw)                         # ◄ Payload API Server lưu là chuỗi JSON → chuyển thành dict
    except (TypeError, ValueError):                       # ◄ Chuỗi không phải JSON hợp lệ
        logger.warning("Ws ticket payload is not valid JSON")
        return None                                       # ◄ Coi như ticket không hợp lệ

    if not isinstance(payload, dict) or not payload.get("tenant_id") or not payload.get("user_id"):
                                                          # ◄ Phải là object và có đủ tenant_id + user_id (bắt buộc để tính stream key)
        logger.warning("Ws ticket payload missing tenant_id/user_id")
        return None

    return payload                                        # ◄ Hợp lệ: trả dict, vd {"tenant_id": "system", "user_id": "...",
                                                          #   "channel": "user", "permissions": [...], "is_superuser": false, ...}
```
Vì sao không GET rồi DELETE (bản 1.0)? Hai kết nối gửi gần như đồng thời đều có thể GET thành công trước
khi DELETE chạy, nên ticket "dùng 1 lần" bị dùng 2 lần. `GETDEL` loại bỏ khe hở đó.

---

### 7.3. `services/connection_manager.py` — quản lý kết nối

Ba nhiệm vụ: **giới hạn tải**, **thống kê**, **tắt an toàn**.

#### `ConnectionRejected` (dòng 42–48)
```python
class ConnectionRejected(Exception):                  # ◄ Ném ra khi không nhận thêm kết nối
    def __init__(self, code: int, reason: str) -> None:
        super().__init__(reason)                      # ◄ Message của exception = reason (để in log)
        self.code = code                              # ◄ Close code dùng khi đóng WebSocket (1012, 1013, 4429)
        self.reason = reason                          # ◄ Lý do dạng snake_case, vd "server_at_capacity"
```

#### `ConnectionInfo` (dòng 51–61)
```python
@dataclass                                                    # ◄ Tự sinh __init__ từ các field bên dưới
class ConnectionInfo:
    connection_id: str                                        # ◄ Mã 12 ký tự, gắn vào mọi dòng log của kết nối
    websocket: WebSocket                                      # ◄ Đối tượng WebSocket (để đóng khi tắt server)
    identity: Identity                                        # ◄ Ai đang kết nối (tenant_id, user_id, quyền...)
    channel: str                                              # ◄ user / tenant_dashboard / job / global
    stream_key: str                                           # ◄ Redis Stream đang nghe
    connected_at: float = field(default_factory=time.monotonic)  # ◄ Thời điểm kết nối (đồng hồ đơn điệu, không bị ảnh hưởng khi đổi giờ hệ thống)
    events_sent: int = 0                                      # ◄ Số event đã gửi (tăng trong _ClientSession.send_text)
```

#### `ConnectionManager.__init__` (dòng 65–81)
```python
def __init__(self, *, max_connections: int, max_per_user: int) -> None:
    self._max_connections = max_connections          # ◄ Giới hạn tổng số kết nối của process (WS_MAX_CONNECTIONS)
    self._max_per_user = max_per_user                # ◄ Giới hạn mỗi user (WS_MAX_CONNECTIONS_PER_USER)
    self._connections: dict[str, ConnectionInfo] = {}  # ◄ connection_id → ConnectionInfo: danh sách kết nối đang mở
    self._per_user: Counter[tuple[str, str]] = Counter()  # ◄ (tenant_id, user_id) → số kết nối của user đó
    self._lock = asyncio.Lock()                      # ◄ asyncio chạy 1 luồng nhưng các coroutine vẫn xen kẽ nhau tại mỗi `await`;
                                                     #   khóa để thao tác "kiểm tra rồi thêm" không bị coroutine khác chen vào giữa
    self._shutting_down = False                      # ◄ True khi server đang tắt → từ chối kết nối mới
    self._stats: Counter[str] = Counter()            # ◄ Bộ đếm cộng dồn (total_accepted, rejected_...)
    self._started_at = time.time()                   # ◄ Thời điểm khởi động, để tính uptime
```

#### `register(info)` (dòng 86–109)
```python
async def register(self, info: ConnectionInfo) -> None:
    user_key = (info.identity.tenant_id, info.identity.user_id)   # ◄ Khóa định danh user (user_id chỉ duy nhất trong 1 tenant)

    async with self._lock:                                        # ◄ Vào vùng khóa: kiểm tra + thêm là 1 khối không bị chen ngang
        if self._shutting_down:                                   # ◄ Server đang tắt
            self._stats["rejected_shutting_down"] += 1            # ◄ Đếm số lần từ chối vì lý do này
            raise ConnectionRejected(close_codes.SERVICE_RESTART, "server_shutting_down")  # ◄ 1012: client reconnect sang instance khác

        if len(self._connections) >= self._max_connections:       # ◄ Process đã đầy
            self._stats["rejected_server_full"] += 1
            raise ConnectionRejected(close_codes.TRY_AGAIN_LATER, "server_at_capacity")    # ◄ 1013: thử lại sau

        if self._per_user[user_key] >= self._max_per_user:        # ◄ User này đã mở quá nhiều kết nối (nhiều tab, script lỗi...)
            self._stats["rejected_user_limit"] += 1
            raise ConnectionRejected(close_codes.TOO_MANY_CONNECTIONS, "too_many_connections_for_user")  # ◄ 4429

        self._connections[info.connection_id] = info              # ◄ Qua hết kiểm tra: thêm vào danh sách
        self._per_user[user_key] += 1                             # ◄ Tăng số kết nối của user
        self._stats["total_accepted"] += 1                        # ◄ Tăng tổng số kết nối đã nhận
```
Nếu không có khóa: 2 kết nối cùng thấy "còn 1 chỗ" rồi cùng được thêm vào, vượt giới hạn.

#### `unregister(connection_id)` (dòng 111–124)
```python
async def unregister(self, connection_id: str) -> ConnectionInfo | None:
    async with self._lock:                                     # ◄ Khóa vì sửa chung dữ liệu với register
        info = self._connections.pop(connection_id, None)      # ◄ Lấy ra và xóa; không có thì trả None (không lỗi)
        if info is None:
            return None                                        # ◄ Đã hủy trước đó → gọi nhiều lần vẫn an toàn

        user_key = (info.identity.tenant_id, info.identity.user_id)
        self._per_user[user_key] -= 1                          # ◄ Giảm số kết nối của user
        if self._per_user[user_key] <= 0:
            del self._per_user[user_key]                       # ◄ Về 0 thì xóa hẳn khóa, để Counter không phình to theo thời gian

        self._stats["total_events_sent"] += info.events_sent   # ◄ Cộng dồn số event kết nối này đã gửi vào thống kê tổng
        return info
```

#### `record_rejection(reason)` (dòng 126–128)
```python
def record_rejection(self, reason: str) -> None:
    self._stats[f"rejected_{reason}"] += 1   # ◄ Đếm kết nối bị từ chối ở bước XÁC THỰC (vd rejected_ticket_invalid_or_expired)
                                             #   Không cần khóa: hàm đồng bộ, không có await → không bị chen ngang
```

#### `active_count`, `is_shutting_down` (dòng 133–139)
```python
@property
def active_count(self) -> int:
    return len(self._connections)   # ◄ Số kết nối đang mở (dùng cho /health/ready)

@property
def is_shutting_down(self) -> bool:
    return self._shutting_down      # ◄ /health/ready trả 503 khi đang tắt
```

#### `snapshot()` (dòng 141–157)
```python
def snapshot(self) -> dict[str, Any]:
    by_channel = Counter(c.channel for c in self._connections.values())  # ◄ Đếm kết nối theo channel, vd {"user": 120, "job": 3}
    live_events = sum(c.events_sent for c in self._connections.values()) # ◄ Event đã gửi bởi các kết nối CÒN MỞ (chưa cộng vào _stats)
    stats = dict(self._stats)                                            # ◄ Sao chép bộ đếm để không sửa bản gốc
    stats["total_events_sent"] = stats.get("total_events_sent", 0) + live_events  # ◄ Tổng = đã đóng + đang mở

    return {
        "active_connections": self.active_count,           # ◄ Số kết nối hiện tại
        "active_users": len(self._per_user),               # ◄ Số user khác nhau đang kết nối
        "active_by_channel": dict(by_channel),
        "max_connections": self._max_connections,
        "max_connections_per_user": self._max_per_user,
        "uptime_seconds": int(time.time() - self._started_at),  # ◄ Đã chạy bao lâu
        "shutting_down": self._shutting_down,
        "counters": stats,
    }
```

#### `shutdown()` (dòng 162–188)
```python
async def shutdown(self) -> None:
    async with self._lock:
        self._shutting_down = True                         # ◄ Từ giờ register() từ chối mọi kết nối mới
        connections = list(self._connections.values())    # ◄ Chụp danh sách hiện tại (list mới, không bị đổi khi dict thay đổi)

    if not connections:
        return                                             # ◄ Không còn ai thì xong

    logger.info("Closing websocket connections for shutdown", extra={"count": len(connections)})

    async def _close(info: ConnectionInfo) -> None:        # ◄ Hàm đóng 1 kết nối
        try:
            if info.websocket.application_state == WebSocketState.CONNECTED:  # ◄ Chỉ đóng nếu còn đang mở
                await info.websocket.close(code=close_codes.SERVICE_RESTART, reason="server_restarting")
                                                           # ◄ 1012 = "server khởi động lại", client nên reconnect ngay
        except Exception:
            pass                                           # ◄ Kết nối có thể đã chết, bỏ qua

    await asyncio.wait(
        [asyncio.create_task(_close(c)) for c in connections],  # ◄ Đóng SONG SONG tất cả kết nối
        timeout=5,                                               # ◄ Tối đa 5 giây, không để việc tắt server bị treo
    )
```

#### Instance dùng chung (dòng 192–195)
```python
connection_manager = ConnectionManager(                     # ◄ Tạo 1 lần khi import: cả process dùng chung 1 manager
    max_connections=settings.WS_MAX_CONNECTIONS,
    max_per_user=settings.WS_MAX_CONNECTIONS_PER_USER,
)
```
Lưu ý: giới hạn tính **theo từng process**. Chạy nhiều process/instance thì mỗi cái có giới hạn riêng.

---

### 7.4. `services/stream_hub.py` — đọc Redis Stream dùng chung

**Vấn đề của cách cũ:** mỗi WebSocket tự chạy `XREAD BLOCK`, nên 5.000 client = 5.000 kết nối Redis, và mỗi
event bị JSON hóa lại cho từng client.
**Cách mới ("một người trực tổng đài"):** `HUB_SHARDS` task nền, mỗi task dùng **1 lệnh XREAD cho rất nhiều
stream** rồi phân phát event vào hàng đợi trong RAM của từng WebSocket.

#### `Subscription` (dòng 59–81) — 1 lượt đăng ký nghe của 1 WebSocket
```python
@dataclass(eq=False)                         # ◄ eq=False: 2 Subscription chỉ "bằng nhau" khi là CÙNG 1 object
class Subscription:                          #   → hash theo danh tính, nên bỏ được vào set() kể cả khi nội dung giống nhau
    stream_key: str                          # ◄ Stream đang nghe
    queue: asyncio.Queue[tuple[str, str]]    # ◄ Hàng đợi chứa (stream_id, chuỗi JSON đã sẵn sàng gửi)
    overflowed: bool = False                 # ◄ True khi hàng đợi từng bị đầy (client nhận quá chậm)

    def offer(self, stream_id: str, text: str) -> bool:
        if self.overflowed:                  # ◄ Đã tràn trước đó → không nhận thêm (client sắp bị ngắt)
            return False
        try:
            self.queue.put_nowait((stream_id, text))  # ◄ Đẩy vào hàng đợi KHÔNG chờ: shard không bao giờ bị 1 client chậm làm kẹt
            return True
        except asyncio.QueueFull:            # ◄ Hàng đợi đã đủ HUB_SUBSCRIBER_QUEUE_SIZE phần tử
            self.overflowed = True           # ◄ Bật cờ; stream_live_events thấy cờ sẽ ném SlowConsumer → đóng 4408
            return False
```
Vì sao ngắt client chậm thay vì chờ? Nếu chờ, một client mạng yếu sẽ làm shard đứng lại, kéo theo **hàng
nghìn** client khác cùng shard bị trễ. Nếu cứ đẩy không giới hạn, RAM sẽ phình vô hạn. Ngắt client đó là
an toàn nhất: khi reconnect, client gửi `last_event_id` và được replay đủ.

#### `_Shard` (dòng 84–94) — trạng thái của 1 shard
```python
@dataclass
class _Shard:
    index: int                                             # ◄ Số thứ tự shard (0, 1, 2, 3)
    cursors: dict[str, str] = field(default_factory=dict)  # ◄ stream_key → ID event cuối shard đã đọc của stream đó
                                                           #   Đây chính là tham số truyền cho XREAD: {stream: "đọc sau ID này"}
    has_streams: asyncio.Event = field(default_factory=asyncio.Event)  # ◄ "Cờ" báo shard có stream để nghe; shard rảnh ngủ chờ cờ này
    task: asyncio.Task | None = None                       # ◄ Task nền chạy _run_shard
    healthy: bool = True                                   # ◄ False khi đang mất kết nối Redis
```

#### `StreamHub.__init__` (dòng 98–103)
```python
def __init__(self, *, shards: int) -> None:
    self._shards = [_Shard(index=i) for i in range(shards)]   # ◄ Tạo N shard rỗng
    self._subscribers: dict[str, set[Subscription]] = {}      # ◄ stream_key → tập các Subscription đang nghe stream đó
                                                              #   (1 user mở 3 tab = 3 Subscription cùng 1 stream_key)
    self._running = False                                     # ◄ Hub đã chạy chưa
    self._stats: Counter[str] = Counter()                     # ◄ Bộ đếm: events_read, events_dispatched, redis_errors...
```

#### `start()` (dòng 108–114)
```python
def start(self) -> None:
    if self._running:
        return                                     # ◄ Gọi 2 lần thì lần 2 không làm gì (không tạo trùng task)
    self._running = True                           # ◄ Đánh dấu đang chạy (vòng lặp shard kiểm tra cờ này)
    for shard in self._shards:
        shard.task = asyncio.create_task(self._run_shard(shard), name=f"hub-shard-{shard.index}")
                                                   # ◄ Mỗi shard 1 task nền chạy song song; đặt tên để dễ debug
    logger.info("Stream hub started", extra={"shards": len(self._shards)})
```

#### `stop()` (dòng 116–122)
```python
async def stop(self) -> None:
    self._running = False                          # ◄ Vòng lặp sẽ không chạy lượt mới
    tasks = [s.task for s in self._shards if s.task]  # ◄ Lấy các task đã tạo
    for task in tasks:
        task.cancel()                              # ◄ Hủy ngay (kể cả đang chờ XREAD): ném CancelledError vào trong task
    await asyncio.gather(*tasks, return_exceptions=True)  # ◄ Chờ tất cả dừng hẳn; return_exceptions để CancelledError không lan ra ngoài
    logger.info("Stream hub stopped")
```

#### `_shard_for(stream_key)` (dòng 127–129)
```python
def _shard_for(self, stream_key: str) -> _Shard:
    return self._shards[zlib.crc32(stream_key.encode("utf-8")) % len(self._shards)]
    # ◄ Băm stream_key bằng CRC32 rồi chia lấy dư cho số shard → luôn ra CÙNG 1 shard cho CÙNG 1 stream.
    #   Dùng crc32 thay vì hash() vì hash() của Python cho chuỗi bị ngẫu nhiên hóa mỗi lần chạy.
    #   Các stream được rải đều qua các shard.
```

#### `subscribe(stream_key)` (dòng 131–157)
```python
async def subscribe(self, stream_key: str) -> Subscription:
    sub = Subscription(
        stream_key=stream_key,
        queue=asyncio.Queue(maxsize=settings.HUB_SUBSCRIBER_QUEUE_SIZE),  # ◄ Hàng đợi có giới hạn (mặc định 1000)
    )
    shard = self._shard_for(stream_key)            # ◄ Shard phụ trách stream này

    start_id: str | None = None
    if stream_key not in shard.cursors:            # ◄ Chưa ai nghe stream này → phải xác định điểm bắt đầu đọc
        latest = await get_redis_fast().xrevrange(stream_key, max="+", min="-", count=1)
                                                   # ◄ Lấy 1 event MỚI NHẤT của stream (XREVRANGE đọc ngược)
        start_id = latest[0][0] if latest else STREAM_START_ID
                                                   # ◄ Có event → bắt đầu từ ID đó (chỉ đọc event MỚI hơn). Stream rỗng → "0-0"
                                                   #   Event cũ hơn là việc của replay_stream, không phải của Hub

    # Từ đây tới hết hàm KHÔNG có await → không coroutine nào chen vào được
    if stream_key not in shard.cursors:            # ◄ Kiểm tra LẠI: trong lúc await ở trên, subscriber khác có thể đã thêm stream này
        shard.cursors[stream_key] = start_id or STREAM_START_ID  # ◄ Thêm stream vào danh sách XREAD của shard
        shard.has_streams.set()                    # ◄ Đánh thức shard nếu nó đang ngủ vì không có stream
    self._subscribers.setdefault(stream_key, set()).add(sub)  # ◄ Thêm subscription vào tập người nghe (tạo tập mới nếu chưa có)
    self._stats["subscribe_total"] += 1
    return sub                                     # ◄ Controller đọc sub.queue để lấy event
```
Nếu stream đã có người nghe (vd tab thứ 2 của cùng user), cursor giữ nguyên: Hub đang đọc stream đó rồi,
chỉ cần thêm một hàng đợi nữa.

#### `unsubscribe(sub)` (dòng 159–170)
```python
def unsubscribe(self, sub: Subscription) -> None:
    subs = self._subscribers.get(sub.stream_key)   # ◄ Tập người nghe của stream
    if not subs:
        return                                     # ◄ Không có → đã hủy rồi, không làm gì
    subs.discard(sub)                              # ◄ Bỏ subscription này (discard không lỗi nếu không có)
    if not subs:                                   # ◄ Stream không còn ai nghe
        del self._subscribers[sub.stream_key]      # ◄ Xóa khỏi bảng người nghe
        shard = self._shard_for(sub.stream_key)
        shard.cursors.pop(sub.stream_key, None)    # ◄ Bỏ stream khỏi lệnh XREAD của shard (không đọc vô ích nữa)
        if not shard.cursors:
            shard.has_streams.clear()              # ◄ Shard hết việc → hạ cờ, vòng lặp sẽ ngủ ở has_streams.wait()
```
Hàm đồng bộ (không `await`) nên chạy trọn vẹn, không bị chen ngang. Được gọi trong `finally` của `_sender`.

#### `_run_shard(shard)` (dòng 175–223) — vòng lặp chính của 1 shard
```python
async def _run_shard(self, shard: _Shard) -> None:
    redis_stream = get_redis_stream()              # ◄ Dùng pool "stream" (timeout dài, hợp với XREAD BLOCK)
    backoff = 0.5                                  # ◄ Thời gian chờ trước khi thử lại khi Redis lỗi (tăng dần)

    while self._running:                           # ◄ Chạy tới khi stop()
        if not shard.cursors:                      # ◄ Không có stream nào cần nghe
            await shard.has_streams.wait()         # ◄ Ngủ tới khi subscribe() gọi has_streams.set() → không gửi lệnh vô ích tới Redis
            continue                               # ◄ Thức dậy → quay lại đầu vòng lặp

        streams = dict(shard.cursors)              # ◄ CHỤP bản sao: trong lúc chờ XREAD, subscribe/unsubscribe có thể sửa dict gốc
        try:
            response = await redis_stream.xread(
                streams,                           # ◄ {stream_key: last_id, ...}: đọc event có ID > last_id ở TẤT CẢ stream trong 1 lệnh
                block=settings.HUB_BLOCK_MS,       # ◄ Không có event thì Redis giữ lệnh tối đa 1000ms rồi trả rỗng
                count=settings.STREAM_READ_COUNT,  # ◄ Mỗi stream lấy tối đa 100 event/lần
            )
            # ◄ HUB_BLOCK_MS ngắn để stream MỚI đăng ký được đưa vào lệnh XREAD kế tiếp nhanh (tối đa trễ ~1s)
        except asyncio.CancelledError:
            raise                                  # ◄ Bị stop() hủy → để lỗi lan ra cho task kết thúc (KHÔNG được nuốt)
        except (RedisConnectionError, RedisTimeoutError) as exc:   # ◄ Mất kết nối Redis
            if shard.healthy:                      # ◄ Chỉ log ở LẦN ĐẦU mất kết nối, tránh spam log mỗi 0.5s
                logger.error("Shard lost Redis connection", extra={"shard": shard.index, "error": repr(exc)})
            shard.healthy = False                  # ◄ Heartbeat sẽ báo stream_status="degraded"
            self._stats["redis_errors"] += 1
            await asyncio.sleep(backoff)           # ◄ Chờ rồi thử lại
            backoff = min(backoff * 2, 5.0)        # ◄ 0.5 → 1 → 2 → 4 → 5 → 5...: không dồn dập Redis đang hỏng
            continue                               # ◄ Cursor GIỮ NGUYÊN → Redis sống lại thì đọc tiếp đúng chỗ, client không mất event
        except RedisError as exc:                  # ◄ Lỗi logic, vd 1 key bị ghi đè thành kiểu khác (WRONGTYPE)
            logger.error("Shard XREAD error", extra={"shard": shard.index, "error": repr(exc)})
            self._stats["redis_errors"] += 1
            await self._drop_invalid_streams(shard, streams)  # ◄ Tìm và bỏ key hỏng để không làm hỏng cả shard
            await asyncio.sleep(0.5)
            continue
        except Exception:                          # ◄ Lỗi không lường trước
            logger.exception("Unexpected shard error", extra={"shard": shard.index})  # ◄ Ghi kèm traceback
            await asyncio.sleep(1.0)
            continue                               # ◄ Shard KHÔNG BAO GIỜ được chết, nếu chết thì mọi client của shard ngừng nhận event

        if not shard.healthy:                      # ◄ Tới đây là XREAD thành công; nếu trước đó đang lỗi...
            logger.info("Shard reconnected to Redis", extra={"shard": shard.index})  # ◄ ...thì log "đã kết nối lại"
        shard.healthy = True
        backoff = 0.5                              # ◄ Reset thời gian chờ

        if response:                               # ◄ Có event (hết giờ BLOCK mà không có event → response rỗng)
            self._dispatch(shard, response)        # ◄ Phân phát cho subscriber
```

#### `_dispatch(shard, response)` (dòng 225–242)
`response` của XREAD có dạng `[[stream_key, [(stream_id, {field: value}), ...]], ...]`.
```python
def _dispatch(self, shard: _Shard, response: list) -> None:
    for stream_key, entries in response:                 # ◄ Lặp từng stream có event mới
        if stream_key not in shard.cursors:
            continue                                     # ◄ Stream vừa bị hủy đăng ký trong lúc chờ XREAD → bỏ qua

        for stream_id, fields in entries:                # ◄ Lặp từng event của stream (theo thứ tự cũ → mới)
            self._stats["events_read"] += 1
            subs = self._subscribers.get(stream_key)     # ◄ Ai đang nghe stream này?
            if subs:
                text = safe_json_dumps(fields_to_event(stream_id, fields))
                                                         # ◄ Chuyển event thành chuỗi JSON đúng 1 LẦN, dùng chung cho mọi subscriber
                                                         #   (50 admin xem dashboard → vẫn chỉ JSON hóa 1 lần)
                for sub in tuple(subs):                  # ◄ tuple(...) chụp bản sao để duyệt an toàn
                    if sub.offer(stream_id, text):       # ◄ Đẩy vào hàng đợi (không chờ)
                        self._stats["events_dispatched"] += 1
                    else:
                        self._stats["events_dropped_slow_client"] += 1  # ◄ Hàng đợi đầy: client chậm sẽ bị ngắt
            shard.cursors[stream_key] = stream_id        # ◄ Dời cursor tới event vừa xử lý → lần XREAD sau chỉ đọc event mới hơn
```
Hàm đồng bộ, không `await`, nên chạy rất nhanh và không bị chen ngang giữa chừng.

#### `_drop_invalid_streams(shard, streams)` (dòng 244–254)
```python
async def _drop_invalid_streams(self, shard: _Shard, streams: dict[str, str]) -> None:
    redis_fast = get_redis_fast()
    for key in streams:                                   # ◄ Kiểm tra từng key trong lệnh XREAD vừa lỗi
        try:
            key_type = await redis_fast.type(key)         # ◄ Lệnh TYPE: "stream", "string", "hash", "none" (không tồn tại)...
        except RedisError:
            return                                        # ◄ Redis có vấn đề chung → để vòng lặp chính xử lý (backoff)
        if key_type not in ("stream", "none"):            # ◄ Key tồn tại nhưng KHÔNG phải stream → XREAD sẽ luôn lỗi WRONGTYPE
            logger.error("Key is not a stream, ignoring", extra={"stream_key": key, "type": key_type})
            shard.cursors.pop(key, None)                  # ◄ Ngừng đọc key này; subscriber của nó vẫn nhận heartbeat bình thường
```

#### `healthy` và `snapshot()` (dòng 259–273)
```python
@property
def healthy(self) -> bool:
    return all(s.healthy for s in self._shards)   # ◄ Hub khỏe khi MỌI shard khỏe; dùng cho heartbeat stream_status

def snapshot(self) -> dict:
    return {
        "healthy": self.healthy,
        "shards": [                                                     # ◄ Mỗi shard: đang nghe bao nhiêu stream, có khỏe không
            {"index": s.index, "streams": len(s.cursors), "healthy": s.healthy}
            for s in self._shards
        ],
        "active_streams": len(self._subscribers),                       # ◄ Số stream đang có người nghe
        "subscriptions": sum(len(v) for v in self._subscribers.values()),  # ◄ Tổng số lượt đăng ký (≈ số WebSocket)
        "counters": dict(self._stats),                                  # ◄ events_read, events_dispatched, redis_errors...
    }
```

#### Instance dùng chung (dòng 277)
```python
stream_hub = StreamHub(shards=settings.HUB_SHARDS)   # ◄ 1 process = 1 hub. main.py gọi start()/stop() trong lifespan
```

---

## 8. `src/utils/` — các hàm tiện ích

### 8.1. `utils/auth.py` — xác thực và phân quyền

| Hàm / lớp | Giải thích |
|---|---|
| `Identity` | Dataclass `frozen=True` (không sửa được sau khi tạo, tránh vô tình đổi quyền giữa chừng): `tenant_id`, `user_id`, `user_name`, `role_id`, `role_name`, `is_superuser`, `permissions` (frozenset), `channel`, `job_id`. |
| `Identity.from_ticket_payload(payload)` | Chuyển dict của ticket thành Identity, **ép kiểu an toàn**: `permissions` không phải list/tuple/set thì coi là rỗng; các field tùy chọn ép thành `str` hoặc `None`; `is_superuser` chỉ nhận đúng `True` (tránh chuỗi `"false"` bị `bool()` thành `True`). |
| `WebSocketAuthError(code, reason)` | Lỗi xác thực, mang close code + reason để router đóng kết nối. |
| `identity_has_permission(identity, permission)` | Superuser có mọi quyền; còn lại phải có đúng permission. |
| `check_origin(websocket)` | Chống **Cross-Site WebSocket Hijacking**: trình duyệt không áp CORS cho WebSocket nên trang web lạ vẫn mở được WebSocket tới server. `WS_ALLOWED_ORIGINS` rỗng → bỏ qua; không có header Origin (client Python/mobile) → cho qua; có Origin nhưng không nằm trong danh sách → 4403. |
| `authenticate_websocket(websocket, expected_channel, required_permission, job_id)` | Kiểm tra theo thứ tự từ rẻ đến đắt: Origin → có ticket không (thiếu → 4401) → `consume_ws_ticket` (Redis lỗi → 1013, ticket sai → 4401) → `Identity` → `identity.channel` phải bằng channel của endpoint (ticket `user` không dùng được cho dashboard → 4403) → với job: `identity.job_id` phải bằng `job_id` trên URL (4403) → kiểm tra quyền (4403). Trả `Identity`. |

### 8.2. `utils/redis_keys.py` — tên key Redis
Tên key phải **giống hệt** bên API Server, nếu không NS sẽ nghe nhầm stream.

| Hàm | Kết quả |
|---|---|
| `user_stream_key(t, u)` | `stream:noti:tenant:{t}:user:{u}` |
| `tenant_stream_key(t)` | `stream:noti:tenant:{t}:dashboard` |
| `job_stream_key(t, job_id)` | `stream:noti:tenant:{t}:job:{job_id}` |
| `global_stream_key()` | `stream:noti:global` |
| `ws_ticket_key(ticket)` | `ws:ticket:{ticket}` |

### 8.3. `utils/events.py` — chuyển dữ liệu Redis ↔ event

| Hàm | Giải thích |
|---|---|
| `fields_to_event(stream_id, fields)` | **Hàm quan trọng nhất của file.** Chuyển 1 entry Redis Stream thành message gửi client. `event_id` và `stream_id` **luôn** bằng Redis Stream ID (client lưu làm `last_event_id`). Nếu publisher có ghi field `event_id` riêng (ID nghiệp vụ, vd `evt_...`) thì đưa sang `source_event_id`, **không** cho ghi đè cursor. Các field khác đi qua `_decode_field`. |
| `_decode_field(value)` | bytes → str. Chỉ `json.loads` khi chuỗi trông giống object/array (`{...}`, `[...]`); JSON hỏng thì giữ nguyên chuỗi. Số/bool giữ **nguyên là chuỗi** (tránh `"123"` thành `123`, `"NaN"` thành JSON không hợp lệ). |
| `event_to_redis_fields(event)` | Chiều ngược lại (dành cho publisher): bỏ giá trị `None`, dict/list → JSON, còn lại → `str`. NS không gọi hàm này. |
| `utc_now_iso()` | Thời gian UTC dạng ISO. |
| `build_job_event(...)`, `build_role_event(...)` | Mẫu event cho job/role, dùng phía publisher. Hiện **không** được gọi trong NS (giữ để đồng bộ hợp đồng dữ liệu giữa các service). |

### 8.4. `utils/utils.py` — Stream ID và message hệ thống

| Hàm | Giải thích |
|---|---|
| `STREAM_START_ID = "0-0"` | Cursor "từ đầu stream". |
| `utc_now_iso()` | Thời gian UTC kết thúc bằng `Z`, vd `2026-11-24T03:00:00.123456Z`. |
| `safe_json_dumps(data)` | `ensure_ascii=False` (giữ tiếng Việt), `allow_nan=False` (chặn NaN/Infinity), `default=str` (kiểu lạ như datetime thành chuỗi thay vì lỗi). |
| `is_valid_stream_id(event_id)` | Đúng dạng `<số>` hoặc `<số>-<số>` (regex `_STREAM_ID_RE`, mỗi phần tối đa 20 chữ số, không âm). |
| `normalize_last_event_id(value)` | Hợp lệ → giữ (dạng `123` thành `123-0`); không gửi/sai → `0-0`. **Không bao giờ** đưa chuỗi chưa kiểm tra vào lệnh Redis. |
| `compare_stream_ids(a, b)` | So sánh 2 ID theo **số** (`ms`, `seq`), trả -1/0/1. Không so chuỗi được vì `"9-0" > "10-0"` theo thứ tự chữ. |
| `build_connected_message(...)` | `{"type":"connected", channel, connection_id, tenant_id, user_id, last_event_id, heartbeat_interval_ms, server_time}`. |
| `build_heartbeat_message(channel, stream_healthy)` | `{"type":"heartbeat", stream_status: "ok" hoặc "degraded", ...}`. |
| `build_replay_incomplete_message(...)` | Báo client đã mất event (`reason`: `limit`/`trimmed`), kèm ID yêu cầu và ID bắt đầu gửi lại. |
| `build_pong_message()` | `{"type":"pong", server_time}`. |
| `build_server_error_message(message, code)` | Chỉ gửi thông báo chung chung; chi tiết lỗi chỉ nằm trong log server (bảo mật). |

### 8.5. `utils/close_codes.py`
Hằng số close code (xem mục VI) và `safe_reason(reason)`: cắt reason cho vừa 123 byte UTF-8 mà không làm
vỡ ký tự tiếng Việt (`decode(errors="ignore")` bỏ phần byte dở dang ở cuối).

---

# VI. Hợp đồng message gửi cho client và close code

## 1. Message server gửi xuống

| `type` | Có `event_id`? | Ý nghĩa / client nên làm gì |
|---|---|---|
| `connected` | Không | Kết nối thành công, kèm `connection_id`, `heartbeat_interval_ms` |
| *(event nghiệp vụ)* | **Có** | `event_id` = `stream_id` = Redis Stream ID → **lưu lại làm `last_event_id`**. ID nghiệp vụ gốc nằm ở `source_event_id` |
| `heartbeat` | Không | Không có event trong `HEARTBEAT_INTERVAL_SECONDS`. `stream_status`: `ok` hoặc `degraded` (mất Redis tạm thời, event sẽ được gửi bù) |
| `replay_incomplete` | Không | Đã mất event (`reason`: `limit` hoặc `trimmed`) → gọi REST API (`GET /notifications/feed`) đồng bộ lại |
| `pong` | Không | Trả lời khi client gửi `ping` hoặc `{"type":"ping"}` |
| `server_error` | Không | Báo lỗi chung chung trước khi đóng |

Field dạng số/bool được giữ **nguyên là chuỗi** như publisher đã ghi; chỉ field là JSON object/array mới được parse.

## 2. Close code

| Code | Ý nghĩa | Client nên làm gì |
|---|---|---|
| 1000 | Đóng bình thường | Không cần làm gì |
| 1008 | Client gửi quá nhiều message | Sửa client, reconnect sau vài giây |
| 1011 | Lỗi nội bộ server | Backoff vài giây, xin ticket mới |
| 1012 | Server đang khởi động lại | Đợi 1–5 giây, xin ticket mới, reconnect |
| 1013 | Server quá tải / Redis lỗi | Backoff lâu hơn (5–30 giây) rồi thử lại |
| 4400 | Request sai (vd `job_id`) | Sửa request, **không** tự reconnect |
| 4401 | Ticket thiếu/sai/hết hạn | Xin ticket mới rồi reconnect |
| 4403 | Không đủ quyền / sai channel / Origin lạ | **Không** reconnect, báo người dùng |
| 4408 | Client nhận quá chậm | Kiểm tra mạng, reconnect với `last_event_id` |
| 4429 | Quá nhiều kết nối của 1 user | Đóng bớt tab/kết nối cũ |

---

# VII. Lịch sử sửa lỗi và hướng mở rộng

## Những lỗi đã sửa so với bản 1.0
- `event_id` của luồng Outbox ghi đè Stream ID → client replay toàn bộ stream mỗi lần reconnect.
- Ticket GET rồi DELETE (2 lệnh) → có thể dùng 1 ticket 2 lần. Nay dùng `GETDEL`.
- `FastAPI(path_prefix=...)` không tồn tại, bị bỏ qua im lặng → comment ghi sai đường dẫn.
- Không đọc từ client → phát hiện client ngắt chậm tới 15 giây, giữ tài nguyên vô ích.
- `json.loads` mọi field → `"123"` thành số, `"NaN"` sinh JSON không hợp lệ.
- `last_event_id="1--5"` lọt kiểm tra → lỗi Redis.
- Gửi nguyên `str(exception)` cho client → lộ thông tin hạ tầng.
- Không đóng Redis khi tắt, `print` thay cho log, không giới hạn số kết nối.

## Stream Hub (08/11/2026)
- Mỗi process chỉ có `HUB_SHARDS` task đọc Redis, mỗi task một lệnh `XREAD BLOCK` cho hàng nghìn stream.
- Mỗi event chỉ JSON hóa **một lần** rồi phát vào hàng đợi của mọi WebSocket đang nghe.
- Client chậm làm đầy hàng đợi → ngắt 4408, tránh phình RAM.
- Redis sự cố: shard tự thử lại, client không bị ngắt (heartbeat `degraded`) và được gửi bù khi Redis trở lại.
- Kiểm chứng: 2.000 kết nối nhận 20.000/20.000 event, server chỉ dùng 4 kết nối Redis cho XREAD (xem DEMO.md mục 6).

## Hướng mở rộng
- Cần hơn khoảng 10.000 kết nối: chạy nhiều instance sau load balancer (không cần sticky session vì mọi
  trạng thái nằm ở Redis).
- Giới hạn số kết nối mỗi user trên **toàn cụm**: đếm trong Redis thay vì trong `ConnectionManager`.

## Ghi chú nhỏ trong code
- Comment tại `controllers/noti_controller.py` (khối `finally` của `run_stream_websocket`) còn nhắc
  "hủy sender đang chờ XREAD BLOCK". Từ khi có Stream Hub, sender chờ trên **hàng đợi trong RAM**, không
  còn giữ kết nối Redis riêng; comment đó là của bản cũ.
