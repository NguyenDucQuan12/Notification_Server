# Máy chủ thông báo realtime bằng WebSocket

Dự án xây dựng hệ thống gửi thông báo **realtime** tới người dùng ứng dụng Python, gồm:
**FastAPI** (API Server), **Outbox Worker**, **Notification Server** (WebSocket), **Redis Stream**, **SQL Server**
và ứng dụng client có giao diện (**customtkinter**).

Hệ thống đã được kiểm thử tải end-to-end với **2.000 người dùng kết nối đồng thời**: 100% thông báo tới đủ
người nhận, độ trễ dưới 2 giây cho thông báo gửi tới cả 2.000 người (xem [mục 10](#10-quy-mô-2000-người-dùng)).

**Mục lục**

1. [Tổng quan kiến trúc](#1-tổng-quan-kiến-trúc)
2. [Hành trình của một thông báo](#2-hành-trình-của-một-thông-báo)
3. [API Server (fastapi_server)](#3-api-server-fastapi_server)
4. [Outbox Worker](#4-outbox-worker)
5. [Notification Server (noti_server)](#5-notification-server-noti_server)
6. [Client: script dòng lệnh và ứng dụng giao diện](#6-client)
7. [Thiết kế Database: bảng, index, truy vấn, khóa, dọn dữ liệu](#7-thiết-kế-database)
8. [Độ tin cậy: không mất, không trùng thông báo](#8-độ-tin-cậy)
9. [Chuẩn bị và khởi động](#9-chuẩn-bị-và-khởi-động)
10. [Quy mô 2.000 người dùng](#10-quy-mô-2000-người-dùng)
11. [Các kịch bản kiểm thử](#11-các-kịch-bản-kiểm-thử)
12. [Tham chiếu API](#12-tham-chiếu-api)
13. [Xử lý sự cố](#13-xử-lý-sự-cố)
14. [Chạy bằng Docker Compose](#14-chạy-bằng-docker-compose)
15. [Cách tạo một thông báo mới khi người dùng làm một tác vụ](#15-cách-tạo-một-thông-báo-mới-khi-người-dùng-làm-một-tác-vụ)
16. [Luồng xử lý file/job](#16-luồng-xử-lý-filejob)

---

## 1. Tổng quan kiến trúc

```
                         ┌──────────────────────────── SQL Server ────────────────────────────┐
                         │ notifications · notification_recipients · notification_deliveries │
                         │ notification_outbox · users · user_auth · roles                    │
                         └───────▲───────────────────────────────▲────────────────────────────┘
                                 │ ① ghi thông báo + outbox       │ ③ claim / đánh dấu published
                                 │   (1 transaction)              │
 ┌────────────┐  HTTP   ┌────────┴─────────┐   ② publish ngay    ┌┴─────────────────┐
 │            │────────►│   API Server     │──────(direct)──┐    │  Outbox Worker   │
 │            │  JWT    │  (FastAPI :8000) │                │    │ (lưới an toàn,   │
 │            │◄────────│                  │── wakeup ──────┼───►│  gửi số lượng    │
 │   Client   │         └────────┬─────────┘                │    │  lớn theo lô)    │
 │ (app GUI / │                  │ SET ws:ticket:<x>        ▼    └────────┬─────────┘
 │  script)   │                  ▼                 ┌──────────────────┐   │ XADD (pipeline)
 │            │          ┌──────────────┐  XADD    │      Redis       │◄──┘
 │            │          │    Redis     │─────────►│  Redis Stream:   │
 │            │          │ (ticket, rate│          │  stream:noti:... │
 │            │          │  limit, ...) │          └────────┬─────────┘
 │            │          └──────▲───────┘                   │ XREAD BLOCK (4 shard dùng chung)
 │            │  WebSocket      │ GETDEL ticket    ┌────────▼─────────┐
 │            │◄────────────────┴──────────────────│ Notification     │
 │            │   ws://.../noti/ws/events?ticket=  │ Server (:1234)   │
 └────────────┘                                    └──────────────────┘
```

| # | Service | Thư mục | Cổng | Nhiệm vụ |
|---|---|---|---|---|
| 0 | Redis | Docker | 6379 | Ticket WebSocket, Redis Stream chứa thông báo, rate limit, cache xác thực, tín hiệu đánh thức worker |
| 1 | API Server | `fastapi_server/src` | 8000 | Login (JWT), cấp ticket, tạo thông báo (DB + outbox), publish Redis, hộp thư (feed / đã đọc) |
| 2 | Outbox Worker | `fastapi_server/src/workers` | – | Publish các outbox còn `pending` theo lô: gửi số lượng lớn, gửi lại khi Redis/API lỗi |
| 3 | Notification Server | `noti_server/src` | 1234 | Giữ WebSocket, xác thực ticket, đọc Redis Stream, đẩy thông báo tới đúng người |
| 2b | Job Worker | `fastapi_server/src/workers` | – | Xử lý file người dùng upload ở nền, gửi thông báo tiến trình (mục 16) |
| 4 | Client | `client` | – | Ứng dụng giao diện `notification_app.py`, script `listen_noti_server.py`, công cụ test |

**Vì sao tách 3 service?**

- **API Server** là nơi duy nhất ghi dữ liệu nghiệp vụ và quyết định *ai* nhận thông báo. API là HTTP, không phải giữ hàng nghìn kết nối lâu dài.
- **Notification Server** chỉ làm một việc: giữ hàng nghìn WebSocket và đẩy event. Nó **không truy cập SQL Server**, nên có thể tăng/giảm số bản sao độc lập với API.
- **Outbox Worker** tách việc publish khối lượng lớn ra khỏi request HTTP, và đảm bảo thông báo đã ghi vào DB thì **chắc chắn** tới Redis (kể cả khi API crash ngay sau khi commit).
- **Redis Stream** đứng giữa làm "hộp thư tạm": Notification Server khởi động lại hay client mất mạng thì event vẫn còn trong stream để gửi bù.

---

## 2. Hành trình của một thông báo

Ví dụ: admin gửi thông báo "Bảo trì hệ thống lúc 22h" tới 2.000 nhân viên.

### Giai đoạn A: người dùng kết nối để nghe (làm 1 lần khi mở app)

| Bước | Ai làm | Chi tiết |
|---|---|---|
| A1 | Client → API | `POST /auth/login` (form `username`, `password`) → nhận **JWT** (sống 15 phút). |
| A2 | Client → API | `GET /notifications/feed` → lịch sử thông báo + số chưa đọc (đọc từ SQL Server). |
| A3 | Client → API | `POST /notifications/ws-ticket?channel=user` kèm `Authorization: Bearer <JWT>`. API kiểm tra quyền `notification:subscribe`, sinh ticket ngẫu nhiên, lưu `ws:ticket:<x>` vào Redis (**sống 60 giây, dùng 1 lần**). |
| A4 | Client → Notification Server | Mở `ws://<host>:1234/noti/ws/events?ticket=<x>&last_event_id=<id>`. |
| A5 | Notification Server | `GETDEL ws:ticket:<x>` (đọc và xóa trong 1 lệnh, không ai dùng lại được) → biết `tenant_id`, `user_id`, quyền. Kiểm tra channel + quyền + giới hạn số kết nối. |
| A6 | Notification Server | Đăng ký stream `stream:noti:tenant:<tenant>:user:<user_id>` vào **Stream Hub**, gửi bù các event sau `last_event_id`, rồi gửi `{"type":"connected"}`. |
| A7 | Notification Server | Không có event thì cứ 15 giây gửi `heartbeat`. |

> **Vì sao dùng ticket thay vì gửi JWT qua WebSocket?** URL WebSocket hay bị ghi vào log (proxy, server).
> Ticket chỉ sống 60 giây và dùng được đúng 1 lần, nên lộ ra cũng vô hại; JWT thì dùng được tới khi hết hạn.
> Notification Server cũng không cần biết `SECRET_KEY` của JWT.

### Giai đoạn B: tạo thông báo (API Server)

| Bước | Chi tiết |
|---|---|
| B1 | Admin gọi `POST /notifications/send` với `send_to_all_users: true` (hoặc một API nghiệp vụ như tạo role / upload file gọi `send_custom_notification`). |
| B2 | API xác thực JWT (có cache Redis 60 giây), kiểm tra quyền `notification:send`, lấy danh sách 2.000 `user_id` của tenant. |
| B3 | `notification_event_service` dựng một **NotificationEvent** chuẩn: `notification_id`, `event_id`, nội dung, người gửi, mức độ, chính sách gửi, trace... |
| B4 | `send_custom_notification` xác định các **stream đích**: 2.000 stream `user`, cộng thêm `dashboard`/`global`/`job` nếu được yêu cầu. |
| B5 | **1 transaction SQL** ghi: 1 dòng `notifications`, 2.000 dòng `notification_recipients` (trạng thái đã đọc của từng người), 4.000 dòng `notification_deliveries` (2 kênh `in_app` + `websocket` cho mỗi người), 2.000 dòng `notification_outbox` (mỗi stream đích 1 dòng, `status=pending`). Dùng bulk insert + `fast_executemany`: khoảng 0,7 giây cho 8.000 dòng. |
| B6 | Sau commit, chọn cách publish: ít hơn hoặc bằng `DIRECT_PUBLISH_MAX_OUTBOX` (200) stream đích → **API publish ngay** trong request; nhiều hơn → **giao cho Outbox Worker** và `LPUSH` tín hiệu đánh thức worker. API trả lời ngay (khoảng 0,6–0,9 giây). |

### Giai đoạn C: publish vào Redis (API hoặc Outbox Worker, cùng một hàm)

`publish_outbox_batch` xử lý tối đa 500 outbox mỗi lô, chỉ với **3 lần gọi**:

1. **SQL: claim nguyên tử.** 1 câu `UPDATE ... OUTPUT` chuyển `pending → processing`, ghi `locked_by=<worker_id>`. Có `READPAST`, nên nhiều worker chạy song song không bao giờ lấy trùng dòng.
2. **Redis: 1 pipeline** gồm 500 lệnh `XADD stream:noti:tenant:T:user:U ...` (cộng `EXPIRE` cho stream user).
3. **SQL: đánh dấu hàng loạt.** 1 câu `UPDATE ... JOIN OPENJSON` đặt `published` + `published_stream_id`, và 1 câu cập nhật `notification_deliveries` kênh websocket = `sent`.

Dòng nào `XADD` lỗi thì được đưa lại `pending` với backoff tăng dần; hết lượt thử thì chuyển `failed`.

### Giai đoạn D: đẩy tới người dùng (Notification Server)

| Bước | Chi tiết |
|---|---|
| D1 | 4 **shard** của Stream Hub, mỗi shard 1 kết nối Redis, chạy `XREAD BLOCK` trên tất cả stream đang có người nghe (chia theo hash của key). 2.000 người dùng vẫn chỉ cần 4 kết nối Redis. |
| D2 | Mỗi event được chuyển thành JSON **1 lần**, rồi đưa vào hàng đợi của mọi kết nối đang nghe stream đó (1 người mở 3 tab thì cả 3 tab nhận). |
| D3 | Mỗi kết nối có task riêng lấy từ hàng đợi và `send` qua WebSocket. Client quá chậm (hàng đợi đầy) thì bị ngắt (`4408`) để không làm phình RAM server. |
| D4 | Client nhận JSON, chống trùng theo `notification_id`, hiện popup + thêm vào danh sách, lưu `stream_id` làm `last_event_id` cho lần kết nối lại. |

### Giai đoạn E: người dùng đọc thông báo

`POST /notifications/{id}/read` cập nhật `notification_recipients.state = read`. Badge số chưa đọc lấy từ
`GET /notifications/unread-count` hoặc kèm trong `/feed`.

---

## 3. API Server (fastapi_server)

```
fastapi_server/src/
├── main.py                         # Khởi tạo FastAPI, middleware, router; chạy uvicorn (API_WORKERS tiến trình)
├── config/config.py                # Đọc env/.env.common + env/.env.api
├── api/                            # Router: auth_api, role_api, notification_api, job_api, health_check, bootstrap_api
├── auth/oauth2.py                  # Tạo/giải mã JWT, dependency required_token_user (cache Redis)
├── controllers/                    # Nghiệp vụ: role_controller, job_controller, notification_controller...
├── services/
│   ├── notification_event_service.py   # Dựng NotificationEvent (1 hàm dùng chung cho mọi loại thông báo)
│   ├── notification_orchestrator.py    # send_custom_notification(): HÀM DUY NHẤT để tạo thông báo
│   ├── job_notifier.py                 # Nội dung + người nhận cho từng giai đoạn của job (gọi hàm trên)
│   ├── job_storage.py / job_queue.py   # Lưu file upload/kết quả; hàng đợi job trên Redis List
│   ├── job_processor.py                # Bước xử lý file (bản demo, thay bằng xử lý thật)
│   ├── notification_publisher.py       # Stream target, publish theo lô (claim -> XADD pipeline -> mark)
│   ├── ws_ticket_service.py            # Cấp ticket WebSocket vào Redis
│   └── redis_client.py                 # 2 client Redis: fast (lệnh ngắn) và stream (lệnh chờ lâu)
├── db/
│   ├── database.py                     # Engine SQLAlchemy async (aioodbc), pool, LOCK_TIMEOUT, thread pool
│   ├── query_job_record.py             # job_record: tạo, claim, tiến độ, success/failed, khôi phục job kẹt
│   ├── query_notifications.py          # Ghi notification + recipients + deliveries + outbox (bulk insert)
│   ├── query_notification_outbox.py    # Claim / mark published / retry / recover (từng dòng và theo lô)
│   ├── query_notification_recipients.py# Đã đọc, đếm chưa đọc...
│   └── query_notification_views.py     # Feed thông báo của người dùng
├── middleware/security_guard.py   # Rate limit theo IP, ban IP nghi vấn (cấu hình qua env)
└── workers/
    ├── notification_outbox_worker.py   # Publish outbox còn pending (mục 4)
    └── job_worker.py                   # Xử lý file/job ở nền (mục 16)
```

**Các nguyên tắc thiết kế chính:**

- **Transactional Outbox.** Thông báo và "việc cần publish" (outbox) được ghi trong **cùng 1 transaction**. Hoặc cả hai cùng tồn tại, hoặc không cái nào tồn tại. Không bao giờ có cảnh DB đã ghi mà Redis chưa nhận và bị quên mất.
- **Thông báo không làm hỏng nghiệp vụ.** `send_custom_notification` dùng session DB riêng và tự bắt mọi lỗi (trả về `status: notification_failed`): role vẫn được tạo, file vẫn được nhận kể cả khi phần thông báo lỗi.
- **Payload gọn.** `build_stream_payload` bỏ danh sách người nhận (`audience.user_ids`) khỏi payload publish. Nếu để lại, gửi cho 2.000 người sẽ nhân bản khoảng 70KB × 2.000 bản sao (khoảng 140MB), và người nhận còn biết được ID của người khác.
- **Không giữ 2 connection DB trong 1 request.** Sau khi xác thực, connection được trả về pool ngay (`rollback`). Nếu không, khi nhiều request đồng thời, pool sẽ cạn và treo (đã xảy ra khi kiểm thử 50 request đồng thời và đã sửa).

**Chế độ publish** (`mode` trong `/notifications/send`, hoặc `NotificationDispatchMode` trong code):

| Chế độ | Khi nào dùng | Hành vi |
|---|---|---|
| `direct` (mặc định) | Thông báo cá nhân, gửi cho ít người | API publish ngay sau commit. Trên 200 stream đích thì tự chuyển sang worker. |
| `worker` | Gửi hàng loạt, không cần trả lời nhanh | API chỉ ghi outbox + đánh thức worker. |

---

## 4. Outbox Worker

`fastapi_server/src/workers/notification_outbox_worker.py`, chạy bằng lệnh `python fastapi_server\src\workers\notification_outbox_worker.py`.

```
loop:
    (mỗi 60s) recover_stale_processing_outbox     # outbox kẹt 'processing' > 300s (process chết) -> pending
    publish_outbox_batch(limit=500) x lanes        # lanes = 1 bình thường, = CONCURRENCY khi còn tồn đọng
    nếu lô không đầy: BLPOP noti:outbox:wakeup (tối đa POLL_SECONDS)   # API đánh thức ngay khi có việc
```

- **Đánh thức tức thì.** API `LPUSH noti:outbox:wakeup` sau khi ghi outbox. Worker đang chờ bằng `BLPOP` nên thức dậy ngay, không phải chờ hết chu kỳ poll. Nếu tín hiệu bị mất, worker vẫn tự quét theo `NOTIFICATION_WORKER_POLL_SECONDS`.
- **Chạy nhiều worker an toàn.** Claim dùng `UPDLOCK + READPAST`: mỗi worker lấy một phần khác nhau, không bao giờ publish trùng. Muốn nhanh hơn thì chạy thêm tiến trình worker.
- **Tự phục hồi.** Worker crash giữa chừng thì outbox của nó kẹt ở `processing`; worker khác sẽ trả các outbox này về `pending` sau `STALE_AFTER_SECONDS`.
- **Dừng êm.** Ctrl+C hoặc `docker stop`: worker xử lý nốt lô hiện tại rồi mới thoát.

---

## 5. Notification Server (noti_server)

```
noti_server/src/
├── main.py                       # FastAPI + uvicorn (ws ping, giới hạn message, graceful shutdown)
├── config/configs.py             # Đọc env/.env.common + env/.env.noti (pydantic-settings, có kiểm tra chéo)
├── api/noti_api.py               # 4 endpoint WebSocket: /ws/events, /ws/tenant-dashboard, /ws/jobs/{id}, /ws/global-events
├── api/health_api.py             # /health/live, /health/ready (kiểm tra Redis), /health/stats
├── controllers/noti_controller.py# Vòng đời 1 kết nối: xác thực -> subscribe -> replay -> gửi -> heartbeat
├── services/stream_hub.py        # Đọc Redis Stream dùng chung (shard), phát event tới các kết nối
├── services/connection_manager.py# Đếm/giới hạn kết nối (tổng, theo user), đóng tất cả khi tắt server
└── utils/auth.py, close_codes.py # Kiểm tra ticket/quyền, bảng mã đóng kết nối
```

**Các channel** (ticket xin cho channel nào thì chỉ dùng được cho channel đó):

| Channel | Đường dẫn WebSocket | Redis Stream | Quyền cần có |
|---|---|---|---|
| `user` | `/noti/ws/events` | `stream:noti:tenant:T:user:U` | `notification:subscribe` |
| `tenant_dashboard` | `/noti/ws/tenant-dashboard` | `stream:noti:tenant:T:dashboard` | `notification:tenant_dashboard` |
| `job` | `/noti/ws/jobs/{job_id}` | `stream:noti:tenant:T:job:J` | `notification:job_watch` + là chủ job (hoặc có `job:read_all`) |
| `global` | `/noti/ws/global-events` | `stream:noti:global` | `notification:global_subscribe` |

Superuser có mọi quyền. Client nghe channel nào thì **chỉ** nhận thông báo được ghi vào stream của channel đó.

**Vòng đời một kết nối** (`noti_controller.py`):

1. Kiểm tra Origin (nếu cấu hình `WS_ALLOWED_ORIGINS`) → `GETDEL` ticket → kiểm tra channel + quyền.
2. Kiểm tra giới hạn: tối đa `WS_MAX_CONNECTIONS` (10.000) mỗi tiến trình, `WS_MAX_CONNECTIONS_PER_USER` (10) mỗi người.
3. **Subscribe trước, replay sau.** Đăng ký vào Stream Hub trước, rồi đọc lịch sử từ `last_event_id`. Event trùng ở phần giao nhau được loại bằng cách so Stream ID, nên **không mất và không trùng**.
4. Replay tối đa `REPLAY_MAX_EVENTS` (1000) event. Lỡ nhiều hơn thì gửi `replay_incomplete`; client nên đồng bộ qua REST `/notifications/feed` (app giao diện làm việc này tự động).
5. Gửi event từ hàng đợi; 15 giây không có event thì gửi `heartbeat`; client có thể gửi `ping` và nhận `pong`.

**Mã đóng kết nối** (`utils/close_codes.py`), dùng để client biết nên làm gì:

| Code | Ý nghĩa | Client nên |
|---|---|---|
| 1012 | Server đang khởi động lại | Kết nối lại sau 1–5 giây |
| 1013 | Server quá tải / Redis lỗi | Thử lại sau 15–30 giây |
| 4400 | Request sai | Dừng, không thử lại |
| 4401 | Ticket sai / hết hạn / đã dùng | Xin ticket mới rồi kết nối lại ngay |
| 4403 | Không đủ quyền / sai channel | Dừng, không thử lại |
| 4408 | Client nhận quá chậm | Kết nối lại |
| 4429 | Quá nhiều kết nối của 1 user | Đóng bớt tab, thử lại sau |

**Khi Redis gặp sự cố**, các shard tự thử lại (backoff tối đa 5 giây) và **không ngắt client**. Cursor được giữ nguyên, nên khi Redis trở lại, client nhận đủ các event bị trễ.

---

## 6. Client

### 6.1. Ứng dụng giao diện `client/notification_app.py` (customtkinter)

```powershell
pip install customtkinter        # lần đầu (hoặc: pip install -r client\requirements.txt)
python client\notification_app.py
```

- **Đăng nhập**: nhập tài khoản. Bấm "⚙ Cấu hình server" để đổi địa chỉ API (`http://127.0.0.1:8000`) và WebSocket (`ws://127.0.0.1:1234/noti`). App nhớ địa chỉ server và username (không lưu mật khẩu) trong `%USERPROFILE%\.notification_app.json`.
- **Màn hình chính**: tên người dùng, trạng thái kết nối (xanh / vàng / đỏ), badge số chưa đọc, bộ lọc "Tất cả / Chưa đọc", nút "Đánh dấu đã đọc tất cả", nút làm mới ⟳, "Đăng xuất".
- **Thẻ thông báo**: thanh màu theo mức độ (info / success / warning / error), chưa đọc thì chữ đậm + chấm xanh. **Bấm vào thẻ** để đánh dấu đã đọc.
- **Popup** góc dưới phải khi có thông báo mới. Nhận dồn dập nhiều thông báo cùng lúc thì gộp thành 1 popup "Bạn có N thông báo mới".
- **Công việc (job)**: nút "📤 Tải file" chọn file và gọi `POST /jobs/upload`. Khung "Công việc" phía trên danh sách hiện tối đa 4 job (job đang chạy lên trước): tên file, trạng thái (Đang chờ / Đang xử lý / Đang thử lại / Hoàn tất / Thất bại), **thanh tiến trình realtime**, thông điệp hoặc lỗi, nút "⬇ Kết quả" khi xong, nút ✕ để ẩn thẻ. Khi đăng nhập, app tải 5 job gần nhất và tiếp tục theo dõi job chưa xong. Job tạo từ nơi khác (CLI, máy khác) cũng tự hiện khi nhận `job.queued`.

Cách app hoạt động bên trong:

| Thành phần | Chạy ở đâu | Nhiệm vụ |
|---|---|---|
| `ApiClient` | Thread nền (httpx) | Login, xin ticket, feed, đánh dấu đã đọc. Gặp 401 (token hết hạn sau 15 phút) thì **tự đăng nhập lại**. |
| `RealtimeWorker` | 1 thread riêng chạy asyncio | Giữ WebSocket. Mất kết nối thì kết nối lại với backoff 1, 2, 4… tối đa 30 giây, **cộng jitter ngẫu nhiên**. Xử lý theo close code. Mỗi lần kết nối lại sẽ yêu cầu UI đồng bộ lại `/feed`. |
| `JobWatcher` | 1 thread riêng / job đang chạy | Kế thừa `RealtimeWorker`, nghe `/ws/jobs/{job_id}` (ticket `channel=job`), đọc stream từ đầu để không lỡ bước tiến độ nào. Kết nối xong thì UI lấy trạng thái qua `GET /jobs/{job_id}`. Tự đóng khi nhận `job.succeeded` / `job.failed`; không có quyền (403/404) thì dừng hẳn. Tối đa 10 job theo dõi cùng lúc. |
| `NotificationApp` | Thread chính (Tk) | Chỉ thread này chạm vào widget. Mỗi 100ms lấy tối đa 50 sự kiện từ `queue`. Giới hạn 200 thẻ trên màn hình. Chống trùng theo `notification_id`. |

> **Jitter là gì, vì sao cần?** Khi Notification Server khởi động lại, 2.000 app mất kết nối cùng lúc.
> Nếu tất cả cùng thử lại sau đúng 1 giây, API sẽ nhận 2.000 request xin ticket trong cùng một khoảnh khắc
> (thundering herd). Cộng thêm một khoảng ngẫu nhiên giúp trải đều các lần kết nối lại.

### 6.2. Các script dòng lệnh

| Script | Công dụng |
|---|---|
| `client/listen_noti_server.py` | Nghe thông báo trên terminal: `--channel user\|tenant_dashboard\|job\|global`, `--job`, `--fresh`, `-v` |
| `client/client_call_api.py` | `notify` (gửi thông báo), `create-role`, `upload-file` |
| `client/load_test.py` | Kiểm thử tải end-to-end (mục 10) |
| `noti_server/demo/noti_demo.py` | Demo/kiểm thử riêng Notification Server, không cần API/SQL (xem `noti_server/DEMO.md`) |

---

## 7. Thiết kế Database

Phần này mô tả cách tổ chức dữ liệu trong **SQL Server** và **Redis** để chịu tải khoảng 2.000 người dùng:
DDL tạo bảng, index và lý do có mặt của từng index, các câu truy vấn chính, cấu hình đồng thời/khóa,
và chiến lược dọn dữ liệu.

> **Thiết kế trong mục này đã nằm sẵn trong thư mục [`database/`](database/).** Tạo database mới bằng
> các script đó (xem [mục 9.2](#92-sql-server-tạo-database-mới)) là có ngay cấu trúc tối ưu: index gọn, filtered index,
> ràng buộc `CHECK`, RCSI. Model SQLAlchemy (`fastapi_server/src/models/`) cũng đã được sửa khớp với script.

| Tệp | Nội dung |
|---|---|
| `database/00_create_database.sql` | Tạo database (collation `Vietnamese_CI_AS`), bật RCSI, recovery model, thống kê bất đồng bộ |
| `database/01_create_tables.sql` | **Xóa và tạo lại** 8 bảng + index + ràng buộc. Được sinh từ model SQLAlchemy nên luôn khớp code |
| `database/02_seed_data.sql` | Dữ liệu ban đầu: role `admin`, role `user`, 1 tài khoản admin. **Không** có thông báo nào |
| `database/init_database.py` | Chạy 3 file trên bằng 1 lệnh, tự mã hóa mật khẩu bằng bcrypt, tùy chọn xóa thông báo cũ trong Redis |

### 7.1. Khối lượng dữ liệu cần phục vụ

Mỗi thông báo gửi tới **N** người sinh ra số dòng như sau:

| Bảng | Số dòng | Với N = 2.000 |
|---|---|---|
| `notifications` | 1 | 1 |
| `notification_recipients` | N | 2.000 |
| `notification_deliveries` | N × số kênh (`in_app`, `websocket`) | 4.000 |
| `notification_outbox` | N + số stream chung (dashboard/global/job) | khoảng 2.000 |

Ví dụ ước lượng (thay số của bạn vào): 2.000 người × **20 thông báo/người/ngày** = 40.000 dòng
`recipients`, 80.000 dòng `deliveries` và 40.000 dòng `outbox` mỗi ngày, tức khoảng 14,6 triệu / 29 triệu / 14,6 triệu
dòng mỗi năm. Vì vậy thiết kế phải đảm bảo 4 điều:

1. **Ghi nhanh theo lô**: 1 lần gửi = hàng nghìn dòng trong 1 transaction.
2. **Đọc theo người dùng luôn nhanh**, dù bảng có hàng chục triệu dòng: hộp thư, số chưa đọc.
3. **Hàng đợi outbox không làm khóa lẫn nhau** khi API và nhiều worker cùng xử lý.
4. **Dữ liệu cũ được dọn định kỳ**, để bảng nóng (outbox, deliveries) luôn nhỏ.

### 7.2. Mô hình dữ liệu và các nguyên tắc

```
                         notifications (1 dòng / thông báo: nội dung CHUNG)
                         PK id · UNIQUE notification_id · UNIQUE event_id
                                     │ notification_id (liên kết logic, không có FOREIGN KEY)
           ┌─────────────────────────┼──────────────────────────────┐
           ▼                         ▼                              ▼
 notification_recipients    notification_deliveries         notification_outbox
 1 dòng / người nhận        1 dòng / người × kênh           1 dòng / Redis Stream đích
 trạng thái ĐỌC             trạng thái GỬI                  hàng đợi publish
 UNIQUE(notification_id,    UNIQUE(notification_id,         UNIQUE(event_id, stream_key)
        recipient_user_id)         recipient_user_id,       aggregate_id = notification_id
                                   channel)
```

| Nguyên tắc | Lý do |
|---|---|
| **Nội dung lưu 1 lần, trạng thái lưu theo từng người** | Gửi cho 2.000 người không nhân bản tiêu đề/nội dung 2.000 lần. `recipients` chỉ có các cột ngắn (ID, trạng thái, thời gian), nên mỗi dòng nhỏ và ghi rất nhanh. |
| **Khóa cụm (clustered) là `id INT IDENTITY`** tăng dần | Dòng mới luôn được ghi vào cuối bảng, không bị tách trang (page split). Nếu khóa cụm là ID ngẫu nhiên (`noti_...`, `outbox_...`), mỗi lần chèn sẽ rơi vào giữa B-tree, gây phân mảnh và chậm. |
| **ID công khai `VARCHAR(64)` + UNIQUE index riêng** | API và Redis dùng ID dạng chuỗi (`noti_8f3a…`) để không lộ số thứ tự và không đoán được. Khóa cụm vẫn là `id` số. |
| **Dùng `VARCHAR` cho ID/mã, `NVARCHAR` chỉ cho văn bản tiếng Việt** | `VARCHAR` tốn 1 byte/ký tự, `NVARCHAR` tốn 2. Kiểu tham số phải **khớp kiểu cột**: so `NVARCHAR` với cột `VARCHAR` làm SQL Server phải ép kiểu cả cột và **không dùng được index** (đây là lý do các câu `OPENJSON` ghi rõ `WITH (outbox_id varchar(64) '$')`). |
| **Không dùng FOREIGN KEY** giữa các bảng thông báo | Mỗi FK là 1 lần tra cứu thêm cho mỗi dòng được chèn: 8.000 dòng là 8.000 lần tra. Tính toàn vẹn được đảm bảo vì cả 4 bảng được ghi trong **cùng 1 transaction**. |
| **Thời gian là `DATETIMEOFFSET`, luôn UTC** | Không lệch giờ khi server và client khác múi giờ. |
| **JSON lưu trong `NVARCHAR(MAX)`** (`data_json`, `payload_json`…) | Dữ liệu mở rộng không cần đổi schema. Không đánh index cột JSON. (Model SQLAlchemy khai báo `UnicodeText`; khi tạo bảng mới nên dùng `NVARCHAR(MAX)` vì `NTEXT` đã lỗi thời. DB hiện tại đã là `NVARCHAR(MAX)`.) |

### 7.3. DDL các bảng (`database/01_create_tables.sql`)

**`notifications`**: nội dung chung của 1 thông báo.

```sql
CREATE TABLE notifications (
    id                           INT IDENTITY NOT NULL PRIMARY KEY,   -- khóa cụm, tăng dần
    notification_id              VARCHAR(64)   NOT NULL,              -- ID công khai: noti_...
    event_id                     VARCHAR(64)   NOT NULL,              -- ID sự kiện: evt_...
    schema_version               VARCHAR(20)   NOT NULL,
    tenant_id                    VARCHAR(64)   NOT NULL,              -- công ty / tổ chức
    notification_type            VARCHAR(128)  NOT NULL,              -- role.created, system.message...
    category                     VARCHAR(64)   NOT NULL,              -- security, system, job...
    source_service               VARCHAR(100)  NOT NULL,              -- service phát sinh
    source_instance              VARCHAR(100)  NULL,
    environment                  VARCHAR(32)   NOT NULL,
    actor_type                   VARCHAR(32)   NULL,                  -- ai gây ra sự kiện
    actor_id                     VARCHAR(64)   NULL,
    actor_name                   NVARCHAR(255) NULL,
    actor_avatar_url             VARCHAR(2048) NULL,
    resource_type                VARCHAR(64)   NULL,                  -- đối tượng bị tác động
    resource_id                  VARCHAR(128)  NULL,
    resource_version             INT           NULL,
    audience_type                VARCHAR(32)   NOT NULL,              -- user / users / tenant ...
    audience_json                NVARCHAR(MAX) NOT NULL,              -- danh sách người nhận gốc (chỉ lưu ở đây)
    channels_json                NVARCHAR(MAX) NOT NULL,
    template_key                 VARCHAR(128)  NULL,
    template_version             VARCHAR(32)   NULL,
    locale                       VARCHAR(20)   NOT NULL,
    title                        NVARCHAR(255) NOT NULL,              -- tiêu đề (tiếng Việt => NVARCHAR)
    body                         NVARCHAR(MAX) NOT NULL,
    short_body                   NVARCHAR(500) NULL,
    image_url                    VARCHAR(2048) NULL,
    variables_json               NVARCHAR(MAX) NOT NULL,
    action_type                  VARCHAR(32)   NOT NULL,              -- hành động khi bấm vào
    action_target                VARCHAR(2048) NULL,
    action_label                 NVARCHAR(100) NULL,
    action_method                VARCHAR(10)   NOT NULL,
    action_open_new_tab          BIT           NOT NULL,
    data_json                    NVARCHAR(MAX) NOT NULL,              -- dữ liệu nghiệp vụ cho client
    priority                     VARCHAR(20)   NOT NULL,              -- low/normal/high/urgent
    severity                     VARCHAR(20)   NOT NULL,              -- info/success/warning/error/critical
    deduplication_key            VARCHAR(255)  NULL,                  -- chống tạo trùng trong 1 khoảng thời gian
    deduplication_window_seconds INT           NULL,
    delivery_ttl_seconds         INT           NOT NULL,
    max_attempts                 INT           NOT NULL,
    retry_delay_seconds          INT           NOT NULL,
    require_ack                  BIT           NOT NULL,
    correlation_id               VARCHAR(128)  NULL,                  -- truy vết request
    trace_id                     VARCHAR(128)  NULL,
    causation_id                 VARCHAR(128)  NULL,
    status                       VARCHAR(20)   NOT NULL,              -- active / cancelled / expired
    available_at                 DATETIMEOFFSET NOT NULL,
    expires_at                   DATETIMEOFFSET NULL,
    created_at                   DATETIMEOFFSET NOT NULL,
    updated_at                   DATETIMEOFFSET NOT NULL
);
CREATE UNIQUE INDEX ix_notifications_notification_id ON notifications (notification_id);
CREATE UNIQUE INDEX ix_notifications_event_id        ON notifications (event_id);
CREATE INDEX ix_notifications_tenant_created  ON notifications (tenant_id, created_at);
CREATE INDEX ix_notifications_tenant_type     ON notifications (tenant_id, notification_type);
CREATE INDEX ix_notifications_deduplication   ON notifications (tenant_id, deduplication_key, created_at);
CREATE INDEX ix_notifications_resource        ON notifications (tenant_id, resource_type, resource_id);
CREATE INDEX ix_notifications_expires_at      ON notifications (expires_at);
-- Ràng buộc (trong CREATE TABLE): CHECK (status IN ('active', 'cancelled', 'expired'))
```

**`notification_recipients`**: trạng thái đọc của từng người. Đây là bảng phục vụ hộp thư.

```sql
CREATE TABLE notification_recipients (
    id                INT IDENTITY   NOT NULL PRIMARY KEY,
    notification_id   VARCHAR(64)    NOT NULL,
    tenant_id         VARCHAR(64)    NOT NULL,
    recipient_user_id VARCHAR(64)    NOT NULL,
    state             VARCHAR(20)    NOT NULL,   -- unread -> seen -> read (-> archived)
    is_pinned         BIT            NOT NULL,
    delivered_at      DATETIMEOFFSET NULL,
    seen_at           DATETIMEOFFSET NULL,
    read_at           DATETIMEOFFSET NULL,
    archived_at       DATETIMEOFFSET NULL,
    deleted_at        DATETIMEOFFSET NULL,       -- người dùng xóa khỏi hộp thư (xóa mềm)
    created_at        DATETIMEOFFSET NOT NULL,
    updated_at        DATETIMEOFFSET NOT NULL,
    CONSTRAINT uq_notification_recipient UNIQUE (notification_id, recipient_user_id)
);
CREATE INDEX ix_notification_recipients_user_created ON notification_recipients (tenant_id, recipient_user_id, created_at);
CREATE INDEX ix_notification_recipients_unread       ON notification_recipients (tenant_id, recipient_user_id)
    WHERE state = 'unread' AND deleted_at IS NULL;                       -- filtered index: đếm chưa đọc
-- Ràng buộc: CHECK (state IN ('unread', 'seen', 'read', 'archived', 'deleted'))
```

- `UNIQUE (notification_id, recipient_user_id)`: một người không thể nhận cùng một thông báo 2 lần (chặn ngay ở DB), và đây cũng là index để đánh dấu đã đọc theo `notification_id`.
- `(tenant_id, recipient_user_id, created_at)`: hộp thư của 1 người, sắp theo thời gian.

**`notification_deliveries`**: trạng thái gửi theo từng kênh.

```sql
CREATE TABLE notification_deliveries (
    id                  INT IDENTITY   NOT NULL PRIMARY KEY,
    delivery_id         VARCHAR(64)    NOT NULL,
    notification_id     VARCHAR(64)    NOT NULL,
    tenant_id           VARCHAR(64)    NOT NULL,
    recipient_user_id   VARCHAR(64)    NOT NULL,
    channel             VARCHAR(32)    NOT NULL,   -- in_app / websocket / email / push / sms
    status              VARCHAR(20)    NOT NULL,   -- pending -> sent -> delivered | failed
    provider            VARCHAR(100)   NULL,       -- redis-stream, smtp...
    provider_message_id VARCHAR(255)   NULL,       -- Redis Stream ID đã publish
    attempt_count       INT            NOT NULL,
    max_attempts        INT            NOT NULL,
    next_retry_at       DATETIMEOFFSET NULL,
    error_code          VARCHAR(100)   NULL,
    error_message       NVARCHAR(MAX)  NULL,
    queued_at           DATETIMEOFFSET NULL,
    sent_at             DATETIMEOFFSET NULL,
    delivered_at        DATETIMEOFFSET NULL,
    failed_at           DATETIMEOFFSET NULL,
    created_at          DATETIMEOFFSET NOT NULL,
    updated_at          DATETIMEOFFSET NOT NULL,
    CONSTRAINT uq_notification_delivery_channel UNIQUE (notification_id, recipient_user_id, channel)
);
CREATE UNIQUE INDEX ix_notification_deliveries_delivery_id   ON notification_deliveries (delivery_id);
CREATE INDEX ix_notification_deliveries_recipient     ON notification_deliveries (tenant_id, recipient_user_id, created_at);
CREATE INDEX ix_notification_deliveries_status_retry  ON notification_deliveries (status, next_retry_at);
-- Ràng buộc: CHECK (status IN ('pending','queued','sending','sent','delivered','failed','expired','cancelled'))
```

- `UNIQUE (notification_id, recipient_user_id, channel)`: câu cập nhật theo lô (`mark ... sent`) **seek** đúng dòng bằng index này.
- `(status, next_retry_at)`: tìm các delivery lỗi đến hạn gửi lại.

**`notification_outbox`**: hàng đợi bền vững để publish vào Redis (Transactional Outbox).

```sql
CREATE TABLE notification_outbox (
    id                  INT IDENTITY   NOT NULL PRIMARY KEY,
    outbox_id           VARCHAR(64)    NOT NULL,
    event_id            VARCHAR(64)    NOT NULL,
    tenant_id           VARCHAR(64)    NOT NULL,
    stream_scope        VARCHAR(20)    NOT NULL,   -- user / tenant / job / global
    target_tenant_id    VARCHAR(64)    NULL,
    target_user_id      VARCHAR(64)    NULL,
    target_job_id       VARCHAR(64)    NULL,
    aggregate_type      VARCHAR(64)    NOT NULL,   -- 'notification'
    aggregate_id        VARCHAR(64)    NOT NULL,   -- = notification_id
    event_type          VARCHAR(128)   NOT NULL,
    stream_key          VARCHAR(255)   NOT NULL,   -- stream:noti:tenant:T:user:U
    payload_json        NVARCHAR(MAX)  NOT NULL,   -- event đã gọn (không có danh sách người nhận)
    status              VARCHAR(20)    NOT NULL,   -- pending -> processing -> published | failed
    attempt_count       INT            NOT NULL,
    max_attempts        INT            NOT NULL,
    available_at        DATETIMEOFFSET NOT NULL,   -- thời điểm được phép xử lý (backoff khi lỗi)
    locked_at           DATETIMEOFFSET NULL,       -- ai đang xử lý, từ lúc nào
    locked_by           VARCHAR(100)   NULL,
    published_stream_id VARCHAR(100)   NULL,       -- Redis Stream ID sau khi XADD
    published_at        DATETIMEOFFSET NULL,
    last_error          NVARCHAR(MAX)  NULL,
    created_at          DATETIMEOFFSET NOT NULL,
    updated_at          DATETIMEOFFSET NOT NULL,
    CONSTRAINT uq_notification_outbox_event_stream UNIQUE (event_id, stream_key)
);
CREATE UNIQUE INDEX ix_notification_outbox_outbox_id    ON notification_outbox (outbox_id);
CREATE INDEX ix_notification_outbox_aggregate_id ON notification_outbox (aggregate_id);
CREATE INDEX ix_notification_outbox_pending      ON notification_outbox (available_at, id)
    INCLUDE (attempt_count, max_attempts) WHERE status = 'pending';     -- filtered index: worker lấy việc
-- Ràng buộc: CHECK (status IN ('pending','processing','published','failed'))
--            CHECK (stream_scope IN ('user','tenant','job','global'))
```

- `UNIQUE (event_id, stream_key)`: 1 event chỉ có 1 outbox cho mỗi stream, nên không bao giờ publish 1 event 2 lần vào cùng stream do lỗi ghi lặp.
- `locked_by` + `locked_at`: biết process nào đang giữ outbox, và phát hiện outbox kẹt khi process đó chết.
- `available_at`: khi lỗi, outbox được hẹn giờ thử lại (backoff) bằng cách đẩy lùi `available_at`.

**`roles`, `users`, `user_auth`**: phân quyền và đăng nhập (bảng nhỏ).

```sql
CREATE TABLE roles (
    id               INT IDENTITY NOT NULL PRIMARY KEY,
    role_id          VARCHAR(64)   NOT NULL,          -- admin, user, ...
    role_name        VARCHAR(64)   NOT NULL,
    description      NVARCHAR(500) NULL,
    permissions_json VARCHAR(MAX)  NOT NULL,          -- ["notification:subscribe", "job:read", ...]
    is_active        BIT NOT NULL,
    created_at       DATETIMEOFFSET NOT NULL,
    updated_at       DATETIMEOFFSET NOT NULL
);
CREATE UNIQUE INDEX ix_roles_role_id   ON roles (role_id);
CREATE UNIQUE INDEX ix_roles_role_name ON roles (role_name);

CREATE TABLE users (                                  -- hồ sơ người dùng
    id         INT IDENTITY NOT NULL PRIMARY KEY,
    user_id    VARCHAR(64)   NOT NULL,                -- 32 ký tự hex, dùng trong JWT, stream Redis...
    tenant_id  VARCHAR(64)   NULL,
    full_name  NVARCHAR(255) NULL,
    email      VARCHAR(255)  NULL,                    -- không bắt buộc
    phone      VARCHAR(32)   NULL,
    is_active  BIT NOT NULL,
    created_at DATETIMEOFFSET NOT NULL,
    updated_at DATETIMEOFFSET NOT NULL
);
CREATE UNIQUE INDEX ix_users_user_id   ON users (user_id);
CREATE INDEX        ix_users_tenant_id ON users (tenant_id);
CREATE INDEX        ix_users_email     ON users (email);

CREATE TABLE user_auth (                              -- thông tin đăng nhập
    id                  INT IDENTITY NOT NULL PRIMARY KEY,
    tenant_id           VARCHAR(64)  NOT NULL,
    user_id             VARCHAR(64)  NOT NULL,
    role_id             VARCHAR(64)  NULL,
    username            VARCHAR(128) NOT NULL,
    password_hash       VARCHAR(512) NOT NULL,        -- bcrypt, KHÔNG lưu mật khẩu gốc
    is_superuser        BIT NOT NULL,
    is_login_enabled    BIT NOT NULL,                 -- khóa tài khoản = đặt 0
    failed_login_count  INT NOT NULL,
    last_login_at       DATETIMEOFFSET NULL,
    password_changed_at DATETIMEOFFSET NULL,
    created_at          DATETIMEOFFSET NOT NULL,
    updated_at          DATETIMEOFFSET NOT NULL,
    CONSTRAINT uq_user_auth_tenant_username UNIQUE (tenant_id, username)   -- login + chống trùng username
);
CREATE UNIQUE INDEX ix_user_auth_user_id ON user_auth (user_id);
CREATE INDEX        ix_user_auth_role_id ON user_auth (role_id);
```

- `UNIQUE (tenant_id, username)`: thiết kế cũ chỉ có index thường trên `username`, nên về lý thuyết có thể tạo 2 tài khoản trùng tên. Giờ DB chặn việc đó, và index này cũng phục vụ luôn câu truy vấn đăng nhập.
- Bảng `job_record` (cho luồng upload/job, mục 15) cũng có trong script nhưng chưa được dùng.

### 7.4. Index: giữ cái gì, bỏ cái gì

**Mỗi index là một bản sao dữ liệu phải cập nhật khi ghi.** Hiện tại 1 lần gửi cho 2.000 người phải ghi:
2.000 dòng × 8 index (`recipients`) + 4.000 × 11 (`deliveries`) + 2.000 × 13 (`outbox`), tức **khoảng 86.000 mục index**.
Nhiều index trong số đó không được truy vấn nào dùng, vì một trong 3 lý do:

| Loại index thừa | Ví dụ | Vì sao thừa |
|---|---|---|
| **Trùng cột đầu** với index ghép | `ix_..._tenant_id` khi đã có `(tenant_id, recipient_user_id, created_at)` | Index ghép đã phục vụ được mọi truy vấn lọc theo `tenant_id` |
| **Trùng với UNIQUE** | `ix_notification_deliveries_notification_id` khi đã có `UNIQUE (notification_id, recipient_user_id, channel)` | UNIQUE cũng là index, cột đầu là `notification_id` |
| **Độ chọn lọc thấp** | `status`, `state`, `channel`, `category` (chỉ vài giá trị) | SQL Server gần như không bao giờ dùng: lọc `status='published'` vẫn ra hàng triệu dòng, quét bảng còn rẻ hơn |

**Index được giữ và truy vấn mà nó phục vụ:**

| Bảng | Index | Phục vụ |
|---|---|---|
| `notifications` | `UNIQUE notification_id` | Tra 1 thông báo, JOIN từ hộp thư |
| | `UNIQUE event_id` | Chống trùng event |
| | `(tenant_id, created_at)` | Danh sách thông báo của tenant (admin) |
| | `(tenant_id, notification_type)` | Lọc theo loại |
| | `(tenant_id, deduplication_key, created_at)` | Chống tạo trùng trong cửa sổ thời gian |
| | `(tenant_id, resource_type, resource_id)` | Thông báo của 1 đối tượng (vd 1 role) |
| | `(expires_at)` | Job đánh dấu hết hạn |
| `notification_recipients` | `UNIQUE (notification_id, recipient_user_id)` | Đánh dấu đã đọc, chống nhận trùng |
| | `(tenant_id, recipient_user_id, created_at)` | Hộp thư `/feed` |
| | **mới:** `(tenant_id, recipient_user_id) WHERE state='unread' AND deleted_at IS NULL` | Đếm chưa đọc (badge), gọi rất thường xuyên |
| `notification_deliveries` | `UNIQUE (notification_id, recipient_user_id, channel)` | Cập nhật `sent` theo lô |
| | `UNIQUE delivery_id` | Tra 1 delivery |
| | `(tenant_id, recipient_user_id, created_at)` | Lịch sử gửi của 1 người |
| | `(status, next_retry_at)` | Tìm delivery lỗi cần gửi lại |
| `notification_outbox` | `UNIQUE outbox_id` | Claim theo danh sách, đánh dấu published theo lô |
| | `UNIQUE (event_id, stream_key)` | Chống trùng |
| | **thay thế:** `(available_at, id) INCLUDE (...) WHERE status='pending'` | Worker lấy outbox đến hạn |
| | `(aggregate_id)` | Tìm/xóa toàn bộ outbox của 1 thông báo |

**Filtered index**, tức index có mệnh đề `WHERE`, là thay đổi đáng giá nhất:

- `ix_notification_outbox_pending`: bảng outbox sau vài tháng có hàng triệu dòng `published`, nhưng chỉ vài chục dòng `pending`. Index lọc chỉ chứa các dòng `pending`, nên **luôn nhỏ** bất kể bảng lớn tới đâu. Câu claim của worker (`WHERE status='pending' ... ORDER BY available_at, id`) đọc thẳng index này, không cần sắp xếp. `INCLUDE (attempt_count, max_attempts)` giúp không phải quay lại bảng để kiểm tra điều kiện.
- `ix_notification_recipients_unread`: badge chưa đọc được gọi mỗi lần mở app hoặc đồng bộ. Index chỉ chứa các dòng chưa đọc, nên đếm chỉ phải đọc vài chục mục, kể cả khi người đó có hàng nghìn thông báo cũ.

**Script nâng cấp cho DB CŨ** (chỉ dùng nếu muốn giữ dữ liệu cũ. Tạo DB mới bằng `database/` thì đã có sẵn, không cần chạy). Đã kiểm thử trong transaction + rollback; nên chạy ngoài giờ cao điểm:

```sql
-- ===== notifications: bỏ index 1 cột trùng / độ chọn lọc thấp =====
DROP INDEX ix_notifications_tenant_id          ON notifications;   -- trùng cột đầu (tenant_id, created_at)
DROP INDEX ix_notifications_status             ON notifications;   -- 3 giá trị, không dùng được
DROP INDEX ix_notifications_category           ON notifications;   -- vài giá trị
DROP INDEX ix_notifications_notification_type  ON notifications;   -- truy vấn luôn có tenant_id -> (tenant_id, notification_type)
DROP INDEX ix_notifications_deduplication_key  ON notifications;   -- trùng (tenant_id, deduplication_key, created_at)
DROP INDEX ix_notifications_resource_id        ON notifications;   -- trùng (tenant_id, resource_type, resource_id)
DROP INDEX ix_notifications_resource_type      ON notifications;
-- (tùy chọn) ix_notifications_actor_id / correlation_id / trace_id: giữ nếu cần tra cứu khi điều tra sự cố

-- ===== notification_recipients =====
DROP INDEX ix_notification_recipients_notification_id   ON notification_recipients; -- trùng UNIQUE (notification_id, ...)
DROP INDEX ix_notification_recipients_recipient_user_id ON notification_recipients; -- truy vấn luôn có tenant_id
DROP INDEX ix_notification_recipients_state             ON notification_recipients; -- độ chọn lọc thấp
DROP INDEX ix_notification_recipients_tenant_id         ON notification_recipients; -- trùng cột đầu
CREATE INDEX ix_notification_recipients_unread
    ON notification_recipients (tenant_id, recipient_user_id)
    WHERE state = 'unread' AND deleted_at IS NULL;

-- ===== notification_deliveries =====
DROP INDEX ix_notification_deliveries_channel           ON notification_deliveries;
DROP INDEX ix_notification_deliveries_status            ON notification_deliveries;
DROP INDEX ix_notification_deliveries_tenant_id         ON notification_deliveries;
DROP INDEX ix_notification_deliveries_recipient_user_id ON notification_deliveries;
DROP INDEX ix_notification_deliveries_notification_id   ON notification_deliveries; -- trùng UNIQUE
DROP INDEX ix_notification_deliveries_next_retry_at     ON notification_deliveries; -- trùng (status, next_retry_at)

-- ===== notification_outbox =====
DROP INDEX ix_notification_outbox_status       ON notification_outbox;
DROP INDEX ix_notification_outbox_available_at ON notification_outbox;
DROP INDEX ix_notification_outbox_stream_scope ON notification_outbox;
DROP INDEX ix_notification_outbox_scope_status ON notification_outbox;
DROP INDEX ix_notification_outbox_tenant_id    ON notification_outbox;
DROP INDEX ix_notification_outbox_event_id     ON notification_outbox;  -- trùng UNIQUE (event_id, stream_key)
DROP INDEX ix_notification_outbox_event_type   ON notification_outbox;
DROP INDEX ix_notification_outbox_aggregate    ON notification_outbox;  -- đã có (aggregate_id)
DROP INDEX ix_notification_outbox_pending      ON notification_outbox;
CREATE INDEX ix_notification_outbox_pending
    ON notification_outbox (available_at, id)
    INCLUDE (attempt_count, max_attempts)
    WHERE status = 'pending';
```

**Kết quả đo** (ghi 1 thông báo cho 2.000 người, tức 8.000 dòng, trung bình 3 lần): index hiện tại **0,61 giây** → sau khi tối ưu **0,46 giây** (nhanh hơn khoảng 25%). Bảng càng lớn, chênh lệch càng rõ, vì index càng sâu thì mỗi lần ghi càng tốn.

> Filtered index yêu cầu kết nối dùng `SET ANSI_NULLS ON`, `QUOTED_IDENTIFIER ON`… (mặc định của ODBC/pyodbc)
> và điều kiện trong truy vấn phải ghi **hằng số** (`status = 'pending'`), không dùng tham số. Các câu truy vấn của dự án đã đúng như vậy.

### 7.5. Các câu truy vấn chính

Đây là những câu chạy nhiều nhất. Mỗi câu kèm index mà nó dùng.

**(1) Ghi thông báo (bulk insert).** `db/query_notifications.py` → `add_notification_event_to_session`

```sql
INSERT INTO notifications (notification_id, event_id, tenant_id, ...) OUTPUT inserted.id VALUES (...);   -- 1 dòng
-- 3 câu dưới đây chạy bằng executemany + fast_executemany: cả mảng 2.000-4.000 dòng gửi trong 1 lần
INSERT INTO notification_recipients (notification_id, tenant_id, recipient_user_id, state, ...) VALUES (?, ?, ?, ?, ...);
INSERT INTO notification_deliveries (delivery_id, notification_id, recipient_user_id, channel, status, ...) VALUES (?, ...);
INSERT INTO notification_outbox (outbox_id, event_id, stream_key, payload_json, status, ...) VALUES (?, ...);
```
Các bảng con **không lấy lại `id`** (không có `OUTPUT inserted.id`). Nếu lấy lại, SQL Server bị giới hạn khoảng 2.100 tham số mỗi câu, và 2.000 người nhận sẽ thành khoảng 70 câu INSERT (4,6 giây thay vì 0,6 giây).

**(2) Worker claim outbox đến hạn.** `claim_outbox_batch(outbox_ids=None)`. Index: `ix_notification_outbox_pending`

```sql
WITH due AS (
    SELECT TOP (@limit) o.*
    FROM notification_outbox AS o WITH (ROWLOCK, UPDLOCK, READPAST)
    WHERE o.status = 'pending'
      AND o.available_at <= SYSUTCDATETIME()
      AND o.attempt_count < o.max_attempts
    ORDER BY o.available_at ASC, o.id ASC
)
UPDATE due
SET status = 'processing', locked_at = SYSUTCDATETIME(), locked_by = @worker_id,
    attempt_count = attempt_count + 1, updated_at = SYSUTCDATETIME()
OUTPUT inserted.outbox_id, inserted.event_id, inserted.aggregate_id AS notification_id,
       inserted.stream_scope, inserted.stream_key, inserted.target_user_id, inserted.payload_json,
       inserted.attempt_count, inserted.max_attempts /* ... */;
```
- `UPDLOCK`: khóa ghi ngay khi đọc, để 2 worker không thể cùng lấy 1 dòng.
- `READPAST`: dòng đang bị người khác khóa thì **bỏ qua** thay vì đứng chờ. Worker 1 lấy dòng 1–500, worker 2 tự lấy 501–1000.
- `UPDATE ... OUTPUT`: đọc và đổi trạng thái trong **1 câu nguyên tử**, không có khoảng hở giữa SELECT và UPDATE.

**(3) API claim đúng các outbox vừa tạo.** `claim_outbox_batch(outbox_ids=[...])`. Index: `UNIQUE outbox_id`

```sql
UPDATE o
SET status = 'processing', locked_at = SYSUTCDATETIME(), locked_by = @worker_id,
    attempt_count = o.attempt_count + 1, updated_at = SYSUTCDATETIME()
OUTPUT inserted.outbox_id /* ... */
FROM OPENJSON(@ids) WITH (outbox_id varchar(64) '$') AS j          -- @ids = '["outbox_a","outbox_b",...]'
INNER LOOP JOIN notification_outbox AS o WITH (ROWLOCK, UPDLOCK, READPAST, FORCESEEK)
    ON o.outbox_id = j.outbox_id
WHERE o.status = 'pending' AND o.available_at <= SYSUTCDATETIME() AND o.attempt_count < o.max_attempts;
```
- `OPENJSON`: truyền **cả danh sách trong 1 tham số**, không bị giới hạn 2.100 tham số của `IN (?, ?, ...)`.
- `INNER LOOP JOIN ... FORCESEEK`: ép SQL Server **đi từ danh sách và seek theo index**. Không có hint này, SQL Server có thể chọn quét cả bảng. Khi quét kèm `UPDLOCK`, mọi dòng bị quét đều bị khóa tới hết transaction và chặn các request khác. Đây chính là lỗi treo đã gặp khi kiểm thử 50 request đồng thời.

**(4) Đánh dấu published theo lô.** Index: `UNIQUE outbox_id`

```sql
UPDATE o
SET status = 'published', published_stream_id = j.stream_id, published_at = SYSUTCDATETIME(),
    locked_at = NULL, locked_by = NULL, last_error = NULL, updated_at = SYSUTCDATETIME()
FROM OPENJSON(@items) WITH (outbox_id varchar(64) '$.o', stream_id varchar(100) '$.s') AS j
INNER LOOP JOIN notification_outbox AS o WITH (ROWLOCK, FORCESEEK) ON o.outbox_id = j.outbox_id
WHERE o.status = 'processing' AND o.locked_by = @worker_id;      -- chỉ sửa dòng mình đang giữ
```

**(5) Đánh dấu delivery kênh websocket = sent.** Index: `UNIQUE (notification_id, recipient_user_id, channel)`

```sql
UPDATE d
SET status = 'sent', provider = 'redis-stream', provider_message_id = j.stream_id,
    sent_at = SYSUTCDATETIME(), error_code = NULL, error_message = NULL, next_retry_at = NULL,
    updated_at = SYSUTCDATETIME()
FROM OPENJSON(@items) WITH (notification_id varchar(64) '$.n',
                            recipient_user_id varchar(64) '$.u',
                            stream_id varchar(255) '$.s') AS j
INNER LOOP JOIN notification_deliveries AS d WITH (ROWLOCK, FORCESEEK)
    ON d.notification_id = j.notification_id
   AND d.recipient_user_id = j.recipient_user_id
   AND d.channel = 'websocket'
WHERE d.status IN ('pending', 'queued', 'sending', 'failed');
```

**(6) Publish lỗi → hẹn giờ thử lại** (`mark_outbox_retry`, từng dòng, chỉ chạy khi có lỗi):

```sql
UPDATE notification_outbox
SET status = @next_status,              -- 'pending' (còn lượt) hoặc 'failed' (hết lượt)
    available_at = @next_available_at,  -- bây giờ + backoff tăng dần
    locked_at = NULL, locked_by = NULL, last_error = @error, updated_at = @now
WHERE outbox_id = @outbox_id AND status = 'processing' AND locked_by = @worker_id;
```

**(7) Khôi phục outbox kẹt** (worker chạy mỗi 60 giây):

```sql
UPDATE notification_outbox
SET status = CASE WHEN attempt_count >= max_attempts THEN 'failed' ELSE 'pending' END,
    available_at = SYSUTCDATETIME(), locked_at = NULL, locked_by = NULL,
    last_error = CONCAT('Recovered stale processing lock from ', locked_by),
    updated_at = SYSUTCDATETIME()
WHERE status = 'processing'
  AND locked_at < DATEADD(SECOND, -@stale_after_seconds, SYSUTCDATETIME());
```

**(8) Hộp thư của người dùng** (`GET /notifications/feed`). Index: `ix_notification_recipients_user_created`, rồi seek `UNIQUE notification_id`

```sql
SELECT n.notification_id, n.notification_type, n.category, n.title, n.body, n.short_body,
       n.priority, n.severity, n.action_type, n.action_target, n.data_json, n.created_at, n.expires_at,
       r.state, r.is_pinned, r.seen_at, r.read_at
FROM notification_recipients AS r
JOIN notifications AS n ON n.notification_id = r.notification_id
WHERE r.tenant_id = @tenant_id
  AND r.recipient_user_id = @user_id
  AND n.tenant_id = @tenant_id
  AND n.status = 'active'
  AND r.deleted_at IS NULL
  -- AND r.state = 'unread'            (khi lọc "Chưa đọc")
ORDER BY r.is_pinned DESC,
         CASE n.priority WHEN 'urgent' THEN 1 WHEN 'high' THEN 2 WHEN 'normal' THEN 3 ELSE 4 END,
         n.created_at DESC
OFFSET @offset ROWS FETCH NEXT @limit ROWS ONLY;
```
Câu này đi **từ bảng `recipients`** (nhỏ, chỉ các dòng của 1 người, tìm bằng index) rồi mới JOIN sang `notifications`.
Vì `ORDER BY` có ghim và độ ưu tiên, SQL Server phải sắp xếp toàn bộ thông báo của người đó. Việc này nhanh khi
mỗi người có vài trăm đến vài nghìn dòng, và **chính sách dọn dữ liệu ở mục 7.7 giữ con số này luôn nhỏ**.

**(9) Đếm chưa đọc** (badge). Index: `ix_notification_recipients_unread` (filtered)

```sql
SELECT COUNT(*) FROM notification_recipients
WHERE tenant_id = @tenant_id AND recipient_user_id = @user_id
  AND state = 'unread' AND deleted_at IS NULL;
```

**(10) Đánh dấu 1 thông báo đã đọc / tất cả đã đọc.** Index: `UNIQUE (notification_id, recipient_user_id)` / `ix_..._unread`

```sql
UPDATE notification_recipients
SET state = 'read', seen_at = @now, read_at = @now, updated_at = @now
WHERE tenant_id = @tenant_id AND notification_id = @notification_id
  AND recipient_user_id = @user_id AND deleted_at IS NULL;

UPDATE notification_recipients
SET state = 'read', seen_at = @now, read_at = @now, updated_at = @now
WHERE tenant_id = @tenant_id AND recipient_user_id = @user_id
  AND state IN ('unread', 'seen') AND deleted_at IS NULL;
```

**(11) Chống tạo trùng** (`find_recent_notification_by_deduplication_key`). Index: `ix_notifications_deduplication`

```sql
SELECT TOP (1) * FROM notifications
WHERE tenant_id = @tenant_id AND deduplication_key = @key
  AND created_at >= @now_minus_window AND status = 'active'
ORDER BY created_at DESC;
```

### 7.6. Đồng thời và khóa

| Kỹ thuật | Ở đâu | Tác dụng |
|---|---|---|
| **Transaction ngắn, không gọi mạng bên trong transaction** | `publish_outbox_batch` | Claim (commit) → gọi Redis (ngoài transaction) → cập nhật (transaction mới). Không bao giờ giữ khóa SQL trong lúc chờ Redis. |
| **`UPDLOCK + READPAST`** | Claim outbox | Nhiều worker/API lấy việc song song mà không chờ nhau. |
| **`LOOP JOIN + FORCESEEK`** | Câu cập nhật theo lô | Chỉ chạm (và khóa) đúng các dòng của lô. |
| **`ROWLOCK`** | Câu cập nhật outbox / deliveries | Khóa mức dòng thay vì trang. |
| **`SET LOCK_TIMEOUT 10000`** | Mỗi kết nối (`db/database.py`) | Chờ khóa quá 10 giây thì báo lỗi và rollback, thay vì treo vô hạn. |
| **Thread pool ≥ số connection** | `configure_db_executor()` | aioodbc chạy lệnh SQL trên thread. Thiếu thread thì phiên đang giữ khóa không chạy được `COMMIT`, gây deadlock mà SQL Server không phát hiện được. |
| **Trả connection ngay sau xác thực** | `auth/oauth2.py` | Mỗi request chỉ giữ 1 connection tại một thời điểm. |

**RCSI** (Read Committed Snapshot Isolation): `database/00_create_database.sql` **bật sẵn**. Với DB cũ thì chạy:

```sql
-- Chạy khi ít người dùng: lệnh cần độc quyền DB trong chốc lát
ALTER DATABASE FastAPI SET READ_COMMITTED_SNAPSHOT ON WITH ROLLBACK IMMEDIATE;

-- Kiểm tra
SELECT name, is_read_committed_snapshot_on FROM sys.databases WHERE name = 'FastAPI';
```

- **Khi tắt (mặc định)**: câu đọc (`/feed`, đếm chưa đọc) phải **chờ** các transaction đang ghi. Lúc gửi cho 2.000 người, hàng nghìn người đang mở app sẽ bị chậm theo.
- **Khi bật**: câu đọc lấy **phiên bản đã commit gần nhất** từ version store trong `tempdb` và không chờ khóa. Ghi cũng không bị đọc chặn.
- Câu claim dùng `UPDLOCK + READPAST` **vẫn hoạt động đúng** khi bật RCSI. Mình đã kiểm thử cả 2 câu claim trên một database tạm có RCSI bật.
- Cái giá phải trả: `tempdb` lớn thêm một chút, và mỗi dòng có thêm 14 byte thông tin phiên bản.

### 7.7. Dọn dữ liệu cũ (retention)

Không dọn thì `outbox` và `deliveries` tăng khoảng 40.000–80.000 dòng mỗi ngày mãi mãi, và index ngày càng sâu.
Dữ liệu cần giữ bao lâu tùy nhu cầu; ví dụ:

| Bảng | Giữ | Lý do |
|---|---|---|
| `notification_outbox` (`published`) | 3 ngày | Chỉ cần cho điều tra sự cố gần đây. Redis đã nhận, lịch sử nằm ở `recipients` |
| `notification_outbox` (`failed`) | 30 ngày | Cần người kiểm tra `last_error` |
| `notification_deliveries` | 30 ngày | Thống kê gửi thành công/thất bại |
| `notification_recipients` + `notifications` | 90–180 ngày | Hộp thư người dùng (theo chính sách công ty) |

**Xóa theo lô nhỏ** (`TOP (5000)`), lặp tới khi hết. Không dùng 1 câu `DELETE` lớn, vì câu lớn sẽ khóa cả bảng (lock escalation khi vượt khoảng 5.000 khóa) và làm file log phình to:

```sql
-- 1) Outbox đã publish quá 3 ngày
WHILE 1 = 1
BEGIN
    DELETE TOP (5000) FROM notification_outbox
    WHERE status = 'published' AND published_at < DATEADD(DAY, -3, SYSUTCDATETIME());
    IF @@ROWCOUNT < 5000 BREAK;
    WAITFOR DELAY '00:00:00.200';   -- nhường tài nguyên cho truy vấn của người dùng
END;

-- 2) Outbox lỗi quá 30 ngày
DELETE TOP (5000) FROM notification_outbox
WHERE status = 'failed' AND updated_at < DATEADD(DAY, -30, SYSUTCDATETIME());

-- 3) Delivery quá 30 ngày
WHILE 1 = 1
BEGIN
    DELETE TOP (5000) FROM notification_deliveries
    WHERE created_at < DATEADD(DAY, -30, SYSUTCDATETIME());
    IF @@ROWCOUNT < 5000 BREAK;
    WAITFOR DELAY '00:00:00.200';
END;

-- 4) Thông báo hết hạn: chuyển trạng thái (ẩn khỏi hộp thư) ...
UPDATE TOP (5000) notifications SET status = 'expired', updated_at = SYSUTCDATETIME()
WHERE status = 'active' AND expires_at < SYSUTCDATETIME();

-- 5) ... và xóa hẳn sau 180 ngày (recipients trước, notifications sau)
DELETE TOP (5000) r FROM notification_recipients AS r
WHERE r.created_at < DATEADD(DAY, -180, SYSUTCDATETIME());
DELETE TOP (5000) FROM notifications
WHERE created_at < DATEADD(DAY, -180, SYSUTCDATETIME());
```

Chạy các lệnh trên mỗi đêm bằng **SQL Server Agent Job**, hoặc thêm 1 bước vào Outbox Worker (chạy mỗi giờ).
Câu `DELETE` (1) dùng được cột `published_at` nhưng **không có index** trên cột đó. Bảng đã dọn đều đặn thì
quét vẫn nhẹ; nếu bảng đã rất lớn, hãy tạo tạm `CREATE INDEX ix_outbox_published_at ON notification_outbox (published_at) WHERE status = 'published'`.

**Khi bảng vượt khoảng 50–100 triệu dòng**: chia **partition theo tháng** trên `created_at`
(`CREATE PARTITION FUNCTION ... RANGE RIGHT FOR VALUES ('2026-01-01', '2026-02-01', ...)`). Khi đó xóa dữ liệu
cũ là `TRUNCATE TABLE ... WITH (PARTITIONS (n))`, chạy tức thì, thay vì `DELETE` hàng triệu dòng.
Với 2.000 người dùng và chính sách giữ dữ liệu như trên, các bảng chỉ ở mức vài triệu dòng nên **chưa cần** partition.

### 7.8. Redis

| Key | Kiểu | Ghi bởi → đọc bởi | Ghi chú |
|---|---|---|---|
| `ws:ticket:<ticket>` | String | API → Notification Server | TTL 60 giây, `GETDEL` (dùng 1 lần) |
| `stream:noti:tenant:T:user:U` | Stream | API/Worker → Notification Server | Tối đa `STREAM_MAX_LENGTH_USER` (500) event, tự xóa nếu 7 ngày không có event mới |
| `stream:noti:tenant:T:dashboard` | Stream | → admin xem toàn tenant | Tối đa `STREAM_MAX_LENGTH` (10.000) |
| `stream:noti:tenant:T:job:J` | Stream | → người theo dõi 1 job | TTL 7 ngày |
| `stream:noti:global` | Stream | → superuser / giám sát | |
| `noti:outbox:wakeup` | List | API → Outbox Worker | Tín hiệu đánh thức worker |
| `auth:user_ok:T:U` | String | API | Cache "user còn hoạt động" 60 giây |
| các key `rl:*`, `ban:*`… | | Middleware bảo mật | Rate limit / ban IP |

Redis Stream chỉ là "bộ đệm" để gửi realtime và gửi bù khi mất kết nối ngắn. **Lịch sử đầy đủ nằm
trong SQL Server**, nên giới hạn độ dài stream không làm mất thông báo của người dùng.

Ước lượng RAM: 1 event khoảng 1,5–2KB × 500 event × 2.000 người, tức **tối đa khoảng 2GB** nếu ai cũng đầy stream.
Thực tế thấp hơn nhiều, vì stream tự xóa sau 7 ngày không hoạt động. Nên đặt `maxmemory` cho Redis và theo dõi bằng `INFO memory`.

### 7.9. Có nên dùng stored procedure như `yolo_paddleocr_server`?

Dự án `yolo_paddleocr_server` (`app/src/services/database_service.py`) dùng cách khác: **pyodbc đồng bộ**, mọi thao tác
đi qua **stored procedure** trong schema riêng `[Queue_job]` (`Enqueue`, `Claim`, `Finish`, `Recover`…), kèm whitelist
procedure, phân loại lỗi (tạm thời / nghiệp vụ / không chắc đã commit) và tự retry có backoff.

| Tiêu chí | Stored procedure (yolo) | SQL trong Python (dự án này) |
|---|---|---|
| Nơi chứa logic truy vấn | Trong SQL Server | Trong code Python (`db/query_*.py`) |
| Phù hợp với | Worker đồng bộ, hàng đợi job, thao tác ít mà quan trọng | FastAPI **async** (aioodbc + SQLAlchemy), nhiều API CRUD |
| Hiệu năng | Plan được cache, 1 round-trip cho cả nghiệp vụ | Tương đương: các câu nóng đã là SQL thuần, theo lô, 1 round-trip |
| Bảo mật | Có thể chỉ cấp quyền `EXECUTE` cho tài khoản ứng dụng | Tài khoản ứng dụng cần quyền đọc/ghi bảng |
| Triển khai / sửa đổi | Phải deploy procedure cùng code, dễ lệch phiên bản | Đổi code là xong; test và review cùng chỗ |
| Debug / test | Khó hơn (logic nằm ở 2 nơi) | Dễ, chạy được bằng pytest |

**Khuyến nghị: không chuyển toàn bộ sang thiết kế của yolo**, nhưng **nên học 4 ý tưởng hay** của nó:

1. **Schema riêng cho hệ thống thông báo** (ví dụ `noti.notifications` thay vì `dbo.notifications`): dễ phân quyền, tránh trùng tên khi chung DB với hệ thống khác. Nên làm ngay khi tạo DB mới, nếu muốn đổi thì phải sửa tên bảng trong model.
2. **Phân loại lỗi và retry có giới hạn** cho lỗi tạm thời (mất kết nối, deadlock 1205, lock timeout 1222). Hiện tại dự án dựa vào outbox + worker để tự gửi lại, nhưng API vẫn trả 500 khi gặp lỗi tạm thời; có thể thêm retry 1–2 lần cho các câu đọc.
3. **`verify_schema()` khi khởi động**: kiểm tra đủ bảng/index cần thiết rồi mới nhận request, để lỗi do quên chạy script hiện ra ngay khi khởi động thay vì lúc người dùng gọi API.
4. **Đặt rõ các tùy chọn phiên kết nối** (`SET ANSI_NULLS ON`… như `_QUEUE_SESSION_SQL`): bắt buộc cho filtered index. Dự án này đã có `SET LOCK_TIMEOUT` khi mở kết nối; các tùy chọn ANSI là mặc định của ODBC.

Nếu sau này cần, chỉ nên chuyển **vài thao tác hàng đợi nóng** (claim / mark published outbox) thành stored procedure,
còn phần CRUD giữ nguyên trong Python.

---

## 8. Độ tin cậy

| Tình huống | Điều gì xảy ra |
|---|---|
| API crash ngay sau khi ghi DB | Outbox còn `pending` → Outbox Worker publish. |
| Redis tạm thời không truy cập được | `XADD` lỗi → outbox quay lại `pending` với backoff; Worker thử lại tới `max_attempts` (10) lần. Notification Server giữ kết nối client và đọc tiếp khi Redis trở lại. |
| Worker crash giữa lô | Outbox kẹt `processing` → được trả về `pending` sau 300 giây → publish lại. Có thể trùng, nhưng client chống trùng bằng `notification_id`. |
| Nhiều Worker / nhiều tiến trình API chạy song song | Claim nguyên tử + `READPAST`: mỗi outbox chỉ được 1 bên xử lý. |
| Client mất mạng vài phút | Kết nối lại với `last_event_id` → server gửi bù phần bị lỡ (tối đa 1.000 event). |
| Client tắt lâu / stream đã bị cắt bớt | App gọi `/notifications/feed` mỗi lần kết nối lại → đồng bộ từ SQL Server. |
| Notification Server khởi động lại | Server đóng kết nối với code 1012 → client kết nối lại (có jitter) + gửi bù. Đã kiểm thử: thông báo gửi **trong lúc server tắt** vẫn tới client sau khi server bật lại. |
| Câu SQL bị khóa lâu | `LOCK_TIMEOUT` (10 giây) → câu lệnh báo lỗi thay vì treo vô hạn; transaction rollback, outbox được xử lý lại sau. |

---

## 9. Chuẩn bị và khởi động

### 9.1. Môi trường ảo và thư viện (làm 1 lần)

```powershell
py -3.12 -m venv .noti_server_venv --prompt="Notification Server"
Set-ExecutionPolicy -Scope Process -ExecutionPolicy RemoteSigned
.noti_server_venv\Scripts\activate
pip install -r fastapi_server\requirements.txt -r noti_server\requirements.txt -r client\requirements.txt
```

### 9.2. SQL Server: tạo database mới

**Yêu cầu:** SQL Server **2017 trở lên** (dùng `OPENJSON`, `STRING_AGG`, `DROP TABLE IF EXISTS`), đã cài
**ODBC Driver 17** (hoặc 18, nhớ sửa `SQL_SERVER_DRIVER` trong `env/.env.api`), và tài khoản trong `env/.env.api`
có quyền tạo database (`dbcreator`) hoặc là chủ database (`db_owner`).

> **Cảnh báo:** `--reset` **xóa toàn bộ bảng và dữ liệu** (người dùng, role, thông báo) của database
> trong `SQL_SERVER_DATABASE`. Muốn giữ dữ liệu cũ thì sao lưu trước:
> `BACKUP DATABASE FastAPI TO DISK = 'D:\backup\FastAPI.bak'`

**Bước 1: tắt các service** (API, Worker, Notification Server). Xóa bảng khi API đang chạy sẽ bị chặn bởi khóa, hoặc làm API lỗi.

**Bước 2: tạo database + bảng + dữ liệu ban đầu, và xóa sạch thông báo cũ trong Redis** (Redis phải đang chạy):

```powershell
cd D:\Project\Notification_Server
.noti_server_venv\Scripts\activate
python database\init_database.py --create-db --reset --clear-redis --username quan
```

Script sẽ hỏi gõ lại tên database để xác nhận xóa, rồi hỏi mật khẩu cho tài khoản admin (nhập 2 lần, tối thiểu 6 ký tự).
Kết quả mong đợi:

```
>>> 00_create_database.sql  (database: master)
   FastAPI | Vietnamese_CI_AS | True | SIMPLE | True | 160      <- RCSI = True
>>> 01_create_tables.sql  (database: FastAPI)
   ... danh sách 8 bảng và index ...
>>> 02_seed_data.sql  (database: FastAPI)
   admin | Admin | ["notification:subscribe", ..., "notification:send", ...]
   user  | User  | ["notification:subscribe","notification:job_watch","job:create","job:read","job:download"]
   Đã tạo tài khoản admin: quan (user_id=...)
>>> Redis (redis://127.0.0.1:6379/0): đã xóa N key thông báo cũ
Hoàn tất.
```

Các tham số của `init_database.py`:

| Tham số | Ý nghĩa |
|---|---|
| `--create-db` | Chạy `00_create_database.sql` trên `master`: tạo DB nếu chưa có, bật RCSI. Lần sau không cần |
| `--reset` | Chạy `01_create_tables.sql`: **xóa và tạo lại** toàn bộ bảng |
| `--yes` | Không hỏi xác nhận khi `--reset` |
| `--clear-redis` | Xóa `stream:noti:*`, ticket và cache xác thực cũ trong Redis, để client không nhận lại thông báo cũ |
| `--username`, `--password`, `--full-name`, `--email`, `--tenant` | Tài khoản admin (mặc định `quan`, tenant `system`). Mật khẩu cũng có thể truyền bằng biến môi trường `NOTI_ADMIN_PASSWORD` |
| `--skip-seed` | Chỉ tạo bảng, không tạo role/tài khoản |
| `--database` | Tên database (mặc định `SQL_SERVER_DATABASE` trong env) |

Chạy `python database\init_database.py --username admin2` (không có `--reset`) để **thêm** 1 admin nữa,
hoặc đồng bộ lại quyền của role `admin`/`user` mà không xóa gì.

**Cách khác: dùng SSMS / sqlcmd**
1. Mở `database\00_create_database.sql` → Execute (trên `master`).
2. Mở `database\01_create_tables.sql` → chọn database `FastAPI` → Execute.
3. Tạo bcrypt hash cho mật khẩu:
   `.noti_server_venv\Scripts\python.exe -c "import bcrypt;print(bcrypt.hashpw(b'MatKhau',bcrypt.gensalt()).decode())"`
4. Mở `database\02_seed_data.sql`, dán hash vào biến `@password_hash`, sửa `@username` → Execute.

**Bước 3: bật lại các service** (mục 9.4), rồi đăng nhập bằng tài khoản vừa tạo, trong app hoặc
`python client\listen_noti_server.py --username quan --password <mật khẩu>`.

> Các script client (`client_call_api.py`, `listen_noti_server.py`, `load_test.py`) mặc định dùng mật khẩu `123456789`.
> Nếu đặt mật khẩu khác, hãy truyền `--password`, hoặc đặt `$env:NOTI_PASSWORD = "..."` trước khi chạy.

**Thêm người dùng thường:** hiện chưa có API tạo user trong router. Tạm thời có thể chạy lại
`init_database.py --username <ten>` rồi đổi `role_id` sang `user` và `is_superuser` sang `0`:
`UPDATE user_auth SET role_id = 'user', is_superuser = 0 WHERE username = '<ten>';`

### 9.3. Cấu hình các tệp `.env` trong thư mục `env/`

| Tệp | Ai đọc | Nội dung |
|---|---|---|
| `env/.env.common` | Mọi service | `REDIS_URL`, `LOG_LEVEL` |
| `env/.env.api` | API Server + Outbox Worker | SQL Server, JWT, ticket, giới hạn stream, publish theo lô, worker, rate limit |
| `env/.env.noti` | Notification Server | Cổng 1234, Stream Hub, giới hạn WebSocket |

**Thứ tự ưu tiên** (cao → thấp):
biến môi trường thật / `environment:` trong docker-compose **>** `.env.api` hoặc `.env.noti` **>** `.env.common`.

Các tệp này ghi giá trị cho **chạy local**. Khi chạy Docker, các giá trị khác biệt (`REDIS_URL=redis://redis:6379/0`,
`SQL_SERVER_HOST=host.docker.internal`, `PORT=8000`, `ENVIRONMENT=production`) đã được ghi đè trong `docker-compose.yml`.

Quy ước viết: `KEY=value`, **không** có dấu cách quanh `=`, chú thích đặt trên dòng riêng
(python-dotenv và Docker Compose đều đọc đúng).

Trước khi chạy, kiểm tra 3 điều:
1. `REDIS_URL` trong `.env.common` trỏ đúng Redis ở bước 9.4. **API, Worker và Notification Server phải dùng cùng 1 Redis.**
2. Thông tin SQL Server trong `.env.api` đúng.
3. `PORT_HOST=8000` (API) và `PORT=1234` (Notification Server) khớp với địa chỉ client dùng.

### 9.4. Khởi động (mỗi service 1 cửa sổ terminal)

Mỗi terminal mới đều phải kích hoạt venv trước:

```powershell
cd D:\Project\Notification_Server
.noti_server_venv\Scripts\activate
```

**Redis (Docker Desktop).** Lần đầu:
```powershell
docker run -d --name noti-demo-redis -p 6379:6379 redis:7-alpine
```
Những lần sau (container đã có sẵn):
```powershell
docker start noti-demo-redis
docker exec noti-demo-redis redis-cli ping      # Phải in ra: PONG
```

> Container trên không lưu dữ liệu ra ổ đĩa: xóa container là mất stream cũ, nhưng thông báo vẫn còn trong SQL Server.
> Muốn giữ dữ liệu, tạo container với `-v D:\Project\Notification_Server\redis-data:/data redis:7-alpine redis-server --appendonly yes`.

**Terminal 1: API Server**
```powershell
python fastapi_server\src\main.py
```
Mong đợi: `Uvicorn running on http://0.0.0.0:8000`. Swagger: http://127.0.0.1:8000/myapi

**Terminal 2: Outbox Worker**
```powershell
python fastapi_server\src\workers\notification_outbox_worker.py
```
Mong đợi: `Outbox worker started: id=worker:... batch=500 concurrency=4 poll=1.0s`.
Mỗi khi publish sẽ có dòng `Batch done: lanes=1 claimed=500 published=500 failed=0`.

**Terminal 2b: Job Worker** (chỉ cần khi dùng luồng upload file/job, mục 16)
```powershell
python fastapi_server\src\workers\job_worker.py
```
Mong đợi: `Job worker started: concurrency=2 max_attempts=3 stale_after=600s`.
API và Job Worker phải dùng chung `UPLOAD_DIRECTORY` (bỏ comment `UPLOAD_DIRECTORY=../../uploads` trong `env/.env.api`).

**Terminal 3: Notification Server**
```powershell
python noti_server\src\main.py
```
Mong đợi:
```
noti.main | Notification service starting | version=2.0.0 environment=development
noti.main | Redis connection OK
noti.hub  | Stream hub started | shards=4
Uvicorn running on http://0.0.0.0:1234
```
Kiểm tra: http://127.0.0.1:1234/health/ready → `"status": "ready"`.

**Terminal 4: Client.** Chọn 1 trong 2:
```powershell
python client\notification_app.py                          # Ứng dụng giao diện
python client\listen_noti_server.py --channel user --fresh # Hoặc nghe trên terminal
```

Tham số của `listen_noti_server.py`:

| Tham số | Ý nghĩa |
|---|---|
| `--channel user\|tenant_dashboard\|job\|global` | Channel muốn nghe (mặc định `user`) |
| `--job JOB-001` | Bắt buộc khi `--channel job` |
| `--username`, `--password` | Tài khoản đăng nhập (mặc định `quan`) |
| `--fresh` | Chỉ nhận thông báo mới từ lúc này (bỏ qua `last_event_id` đã lưu) |
| `-v` | In toàn bộ JSON của mỗi event |

Không dùng `--fresh` thì client đọc `last_event_id` trong tệp `.last_event_id_<channel>.txt`
và server **gửi bù** các thông báo bị lỡ khi client tắt.

**Terminal 5: Gửi thông báo**
```powershell
python client\client_call_api.py notify --title "Xin chào" --body "Thông báo thử từ API" --count 3
```
App hoặc Terminal 4 hiện ngay:
```
--------------------------------------------------------------------------------
[14:57:19] THÔNG BÁO  type=system.message  stream_id=1794729439727-0
           Xin chào #1
           Thông báo thử từ API
```

---

## 10. Quy mô 2.000 người dùng

### 10.1. Kết quả kiểm thử tải

Đo bằng `client/load_test.py`, chạy end-to-end qua đúng luồng thật (API → SQL Server → Redis → Notification Server → WebSocket).
Tất cả thành phần (SQL Server, Redis, API, Worker, Notification Server, 2.000 client giả lập) chạy **trên cùng 1 máy Windows**,
nên đây là số liệu thận trọng. Đặt client test trên máy khác sẽ cho độ trễ thấp hơn.

| Kịch bản | Kết quả |
|---|---|
| Mở 2.000 WebSocket | 2.000/2.000 thành công trong khoảng 5 giây, 0 bị ngắt |
| 1 thông báo gửi tới **cả 2.000 người** (×3 lần) | 6.000/6.000 nhận (100%). API trả lời trong khoảng 0,65 giây. 100% người nhận được trong **1,3–1,4 giây** (p50 khoảng 1,1 giây) |
| **1.000 thông báo cá nhân**, 50 request đồng thời (1 tiến trình API) | 1.000/1.000 thành công, khoảng **88 thông báo/giây**. API p95 = 730ms. Client nhận p95 = 566ms |
| Cùng bài trên, `API_WORKERS=4`, 100 request đồng thời | Khoảng **135 thông báo/giây** |
| Notification Server tắt rồi bật lại | Client tự kết nối lại, nhận bù thông báo gửi trong lúc server tắt |

So với trước khi cải tiến, cùng kịch bản gửi cho 2.000 người: API mất 4,7–6,0 giây, 100% người nhận sau 5,4–7,6 giây.
Với 50 request đồng thời, hệ thống cũ **treo hoàn toàn** do cạn connection pool / deadlock thread pool.

### 10.2. Các cải tiến đã làm để chịu tải

| # | Vấn đề | Cải tiến | Tệp |
|---|---|---|---|
| 1 | Payload chứa danh sách 2.000 người nhận, nhân bản vào mỗi outbox + mỗi stream (khoảng 140MB/lần gửi), lộ ID người khác | `build_stream_payload` bỏ `audience.user_ids` | `db/query_notifications.py` |
| 2 | Ghi 8.000 dòng mất 4,6 giây (SQLAlchemy lấy lại `id` từng dòng) | Bulk insert không lấy `id` + `fast_executemany`: **0,7 giây** | `db/query_notifications.py`, `db/database.py` |
| 3 | Publish từng outbox: 2 transaction + 1 XADD mỗi người nhận | Publish theo lô: 1 SQL claim + 1 Redis pipeline + 1 SQL cập nhật cho 500 outbox | `services/notification_publisher.py`, `db/query_notification_outbox.py` |
| 4 | Gửi cho nhiều người làm treo request HTTP | Trên `DIRECT_PUBLISH_MAX_OUTBOX` thì giao Outbox Worker | `services/notification_orchestrator.py` |
| 5 | Worker chờ hết chu kỳ poll mới chạy | Đánh thức bằng `LPUSH`/`BLPOP`; chạy song song nhiều lô khi tồn đọng | `workers/notification_outbox_worker.py` |
| 6 | Câu cập nhật theo lô quét cả bảng → khóa dòng của request khác | Ép seek theo index (`LOOP JOIN` + `FORCESEEK`) | `db/query_notification_outbox.py` |
| 7 | aioodbc chỉ có khoảng 20 thread → các lệnh chờ khóa chiếm hết thread, `COMMIT` không chạy được → **deadlock vĩnh viễn** | Thread pool ≥ số connection; `LOCK_TIMEOUT` 10 giây | `db/database.py` |
| 8 | Mỗi request giữ 2 connection DB (xác thực + ghi thông báo) → cạn pool khi đồng thời | Trả connection ngay sau xác thực; cache "user hợp lệ" trong Redis 60 giây | `auth/oauth2.py` |
| 9 | 2.000 stream user × 10.000 event có thể chiếm hàng chục GB RAM Redis | Stream user giữ 500 event + TTL 7 ngày | `services/notification_publisher.py` |
| 10 | Rate limit 120 req/phút/IP: cả văn phòng chung IP (NAT) sẽ bị ban; `/auth/login` không nằm trong bucket login | Cấu hình qua env, danh sách IP miễn, thêm `/auth/login` | `security/config.py`, `middlerware/security_guard.py` |
| 11 | API chỉ dùng 1 nhân CPU | `API_WORKERS` (nhiều tiến trình uvicorn) | `main.py`, `Dockerfile` |
| 12 | Client mất kết nối đồng loạt rồi cùng kết nối lại một lúc | Backoff + jitter, xử lý theo close code, đồng bộ `/feed` khi kết nối lại | `client/notification_app.py`, `client/listen_noti_server.py` |

### 10.3. Gợi ý cấu hình khi triển khai khoảng 2.000 người dùng

| Biến | Tệp | Gợi ý | Lý do |
|---|---|---|---|
| `API_WORKERS` | `.env.api` | 2–4 | Dùng nhiều nhân CPU. Tổng kết nối SQL = `API_WORKERS × (DB_POOL + DB_MAX_OVERFLOW)` + worker |
| `DB_POOL` / `DB_MAX_OVERFLOW` | `.env.api` | 10 / 20 | Đủ cho khoảng 50 request đồng thời mỗi tiến trình |
| `RATE_LIMIT_GLOBAL_PER_MIN` | `.env.api` | 120 nếu mỗi người 1 IP; **tăng nhiều** nếu cả công ty đi qua 1 IP NAT/proxy | Tránh ban nhầm cả văn phòng |
| `RATE_LIMIT_LOGIN_PER_MIN` | `.env.api` | 10–30 | Đầu giờ sáng nhiều người đăng nhập cùng lúc qua cùng IP |
| `DIRECT_PUBLISH_MAX_OUTBOX` | `.env.api` | 200 | Gửi cho trên 200 người thì để worker xử lý |
| `STREAM_MAX_LENGTH_USER` | `.env.api` | 500 | Đủ gửi bù khi mất kết nối ngắn |
| `WS_MAX_CONNECTIONS_PER_USER` | `.env.noti` | 10 | Mỗi người mở tối đa 10 app/tab |
| `HUB_SHARDS` | `.env.noti` | 4 | 4 kết nối Redis phục vụ hết 10.000 WebSocket |
| Số tiến trình Outbox Worker | – | 1–2 | Chạy thêm tiến trình khi cần, không bị publish trùng |

Notification Server được thiết kế cho khoảng 10.000 kết nối mỗi tiến trình. Khi vượt mức đó, chạy thêm bản sao phía sau
load balancer (các bản sao không cần biết nhau, vì đều đọc từ cùng Redis). Trên Linux nhớ nâng `ulimit -n`
(docker-compose đã đặt `nofile: 65536`).

### 10.4. Tự chạy kiểm thử tải

```powershell
# 1. Cho phép máy chạy test không bị rate limit (thêm vào env/.env.api rồi khởi động lại API):
#    RATE_LIMIT_EXEMPT_IPS=127.0.0.1
# 2. Bật Redis, API, Worker, Notification Server như mục 9.4, rồi:
python client\load_test.py --users 2000 --rounds 3 --personal 1000
```

| Tham số | Ý nghĩa |
|---|---|
| `--users` | Số người dùng giả lập = số WebSocket (mặc định 2000) |
| `--rounds` | Số thông báo gửi tới **tất cả** người dùng |
| `--personal N` | Gửi thêm N thông báo cá nhân, `--personal-concurrency` request đồng thời (mặc định 50) |
| `--mode direct\|worker` | Cách publish |
| `--keep-data` | Không xóa dữ liệu thử (mặc định công cụ tự xóa dữ liệu trong SQL Server và Redis sau khi chạy) |

Công cụ tạo người dùng giả lập `lt_u00001…` bằng cách cấp ticket thẳng vào Redis, vì DB không có sẵn 2.000 tài khoản.
Thông báo được gửi qua **API thật** bằng tài khoản `quan`.

---

## 11. Các kịch bản kiểm thử

| Kịch bản | Lệnh (Terminal 5) | Kết quả ở Terminal 4 / app |
|---|---|---|
| Gửi cho chính mình (direct) | `python client\client_call_api.py notify` | Nhận ngay |
| Qua Outbox Worker | `python client\client_call_api.py notify --mode worker` | Nhận gần như ngay (worker được đánh thức), Terminal 2 in `published=1` |
| Tạo role | `python client\client_call_api.py create-role` | `type=role.created` |
| Upload file / job | `python client\client_call_api.py upload-file --file README.md` → chạy lệnh `listen ... --channel job --job <job_id>` được in ra | Channel `user`: `job.queued`, `job.succeeded`. Channel `job`: thêm `job.started`, `job.progress` 10/40/70/90% |
| Gửi cho cả tenant | Swagger: `POST /notifications/send` với `"send_to_all_users": true` | Mọi người dùng đang mở app đều nhận |
| Gửi vào tenant dashboard | `python client\client_call_api.py notify --dashboard` | Client phải nghe `--channel tenant_dashboard` |
| Gửi vào global | `python client\client_call_api.py notify --global` | Client phải nghe `--channel global` |
| Gửi theo job | `python client\client_call_api.py notify --job JOB-001 --type job.progress --title "50%"` | Client nghe `--channel job --job JOB-001` (job_id tự đặt chỉ nghe được với superuser / `job:read_all`) |
| Gửi cho user khác | `python client\client_call_api.py notify --to <user_id>` | Client của user đó (channel `user`) |
| Mất kết nối, không mất thông báo | Ctrl+C Terminal 4 → `notify --count 3` → chạy lại `listen_noti_server.py` **không** `--fresh` | Nhận bù đủ 3 thông báo |
| Server tắt, không mất thông báo | Đang mở app → tắt Terminal 3 → `notify` → bật lại Terminal 3 | App chuyển sang vàng "đang thử lại", rồi xanh và hiện thông báo |
| Worker là lưới an toàn | Tắt Terminal 2 → `notify --mode worker` (chưa nhận) → bật lại Worker | Nhận ngay khi Worker chạy lại |
| Chịu tải | `python client\load_test.py --users 2000 --rounds 3 --personal 1000` | Xem mục 10 |

---

## 12. Tham chiếu API

Tất cả cần header `Authorization: Bearer <JWT>` (lấy từ `POST /auth/login`, form `username`, `password`).
Thử trên Swagger: http://127.0.0.1:8000/myapi (bấm **Authorize**).

| Method | Đường dẫn | Mô tả |
|---|---|---|
| POST | `/notifications/ws-ticket?channel=user\|tenant_dashboard\|job\|global[&job_id=]` | Cấp ticket WebSocket (60 giây, dùng 1 lần) |
| POST | `/notifications/send` | Gửi thông báo (body bên dưới) |
| GET | `/notifications/feed?limit=30&offset=0&state=unread` | Hộp thư của người đang đăng nhập + `unread_count` |
| GET | `/notifications/unread-count` | Số thông báo chưa đọc |
| POST | `/notifications/{notification_id}/read` | Đánh dấu 1 thông báo đã đọc |
| POST | `/notifications/read-all` | Đánh dấu tất cả đã đọc |

**Body của `POST /notifications/send`** (chỉ `title` và `body` là bắt buộc):

```json
{
  "title": "Bảo trì hệ thống",
  "body": "Hệ thống sẽ bảo trì lúc 22h tối nay.",
  "target_user_ids": [],
  "send_to_all_users": false,
  "send_to_tenant_dashboard": false,
  "send_to_global": false,
  "job_id": null,
  "severity": "warning",
  "notification_type": "system.message",
  "data": {},
  "mode": "direct"
}
```

- `target_user_ids` rỗng (và không bật các tùy chọn khác) → gửi cho **chính người gọi**.
- `send_to_all_users: true` → gửi cho **mọi tài khoản đang hoạt động** của tenant.
- Gửi cho người khác / tất cả / dashboard / global / job cần **superuser** hoặc quyền `notification:send`.
- `severity`: `info`, `success`, `warning`, `error`, `critical`. `notification_type` phải có dạng `a.b`.
- Response: `notification_id`, `outbox_ids`, `stream_count`, `stream_keys` (tối đa 10), `publish`
  (gồm `published_count`; hoặc `deferred_to_worker: true` nếu giao cho worker).

**API luồng file/job** (chi tiết ở mục 16):

| Method | Đường dẫn | Mô tả |
|---|---|---|
| POST | `/jobs/upload` (multipart, field `file`) | Upload file, tạo job. Cần quyền `job:create` |
| GET | `/jobs?status=&all_users=false&limit=100` | Danh sách job của tôi (`all_users=true` cần `job:read_all`) |
| GET | `/jobs/{job_id}` | Trạng thái, tiến độ, lỗi của job |
| GET | `/jobs/{job_id}/result` | Tải file kết quả (khi `status=success`) |

Các API nghiệp vụ khác muốn gửi thông báo thì gọi `send_custom_notification(...)`, xem [mục 15](#15-cách-tạo-một-thông-báo-mới-khi-người-dùng-làm-một-tác-vụ).

---

## 13. Xử lý sự cố

| Hiện tượng | Nguyên nhân / cách xử lý |
|---|---|
| Client chỉ nhận `heartbeat` | Bình thường khi chưa có ai gửi thông báo. Chạy `client_call_api.py notify`. Nếu vẫn không nhận: client nghe sai channel (ví dụ nghe `user` nhưng thông báo chỉ gửi vào `dashboard`), hoặc 3 service dùng **khác Redis** (kiểm tra `REDIS_URL`). |
| Kiểm tra event đã vào Redis chưa | `docker exec noti-demo-redis redis-cli --scan --pattern "stream:*"` rồi `redis-cli XLEN <key>` |
| Kiểm tra outbox trong SQL | `SELECT TOP 10 outbox_id, status, stream_key, last_error FROM notification_outbox ORDER BY created_at DESC`. Còn `pending` hoặc `failed` thì xem log Worker. |
| Outbox kẹt `processing` | Process publish đã chết giữa chừng. Worker tự trả về `pending` sau `NOTIFICATION_WORKER_STALE_AFTER_SECONDS` (300 giây). |
| API trả `403 ... tần suất yêu cầu lớn` | IP bị rate limit / ban. Nhiều người chung IP thì tăng `RATE_LIMIT_*` hoặc thêm IP vào `RATE_LIMIT_EXEMPT_IPS`. |
| Request treo khoảng 30 giây rồi lỗi `QueuePool limit ... reached` | Hết connection DB. Tăng `DB_POOL`/`DB_MAX_OVERFLOW`, kiểm tra SQL Server có câu lệnh bị khóa lâu không. |
| Lỗi `Lock request time out period exceeded` | Một câu SQL chờ khóa quá `DB_LOCK_TIMEOUT_MS`. Giao dịch được rollback và thử lại sau; nếu xảy ra thường xuyên thì tìm transaction đang giữ khóa lâu. |
| `Login failed 422` | `/auth/login` nhận **form** (`data=`), không nhận JSON. |
| `HTTP 403 Thiếu quyền: notification:...` | Tài khoản thiếu quyền của channel (bảng channel ở mục 5) hoặc thiếu `notification:send`. |
| WebSocket đóng `4401` / `4403` / `4429` | Xem bảng mã đóng kết nối ở mục 5. |
| Khóa tài khoản nhưng người đó vẫn gọi được API | Cache xác thực có hiệu lực tối đa `AUTH_USER_CACHE_SECONDS` (60 giây). |
| `& $PY ...` báo *expression ... was not valid* | `$PY` chưa được gán trong terminal này. Dùng `python` sau khi kích hoạt venv. |
| `UnicodeEncodeError: 'charmap' codec` khi ghi log ra file | Đặt `$env:PYTHONIOENCODING = "utf-8"` trước khi chạy. |
| Cổng 8000/1234 đã bị chiếm | Một bản cũ còn chạy: `Get-NetTCPConnection -LocalPort 8000,1234 -State Listen` rồi tắt tiến trình đó. |
| Sửa code hoặc `.env` nhưng không thấy thay đổi | Phải **tắt và chạy lại** service (Ctrl+C rồi chạy lại). |
| `ModuleNotFoundError: customtkinter` | `pip install -r client\requirements.txt` |

---

## 14. Chạy bằng Docker Compose

`docker-compose.yml` chạy cả 5 container: `redis`, `docker_api` (`API_WORKERS=2`), `noti_server`, `notification-worker`, `job-worker`
(`docker_api` và `job-worker` dùng chung volume `./assets/file` → `/app/uploads`).
Cổng được publish theo biến `TAILSCALE_IP`, nên cần tạo tệp `.env` ở thư mục gốc dự án:

```
TAILSCALE_IP=0.0.0.0
```

```powershell
docker compose up -d --build
docker compose ps
docker compose logs -f noti_server notification-worker
docker compose up -d --scale notification-worker=2   # chạy thêm worker (cần bỏ container_name của service này)
```

Khi đó API ở `http://<TAILSCALE_IP>:8001`, WebSocket ở `ws://<TAILSCALE_IP>:1234/noti/...`.
Client chạy ngoài Docker thì đổi địa chỉ bằng biến môi trường, hoặc trong mục "Cấu hình server" của app:

```powershell
$env:API_BASE_URL = "http://127.0.0.1:8001"
$env:NOTI_WS_BASE_URL = "ws://127.0.0.1:1234/noti"
python client\listen_noti_server.py
```

Redis trong compose **không** publish cổng 6379 ra ngoài, nên không trộn service chạy local với Redis của compose.

---

## 15. Cách tạo một thông báo mới khi người dùng làm một tác vụ

Mọi thông báo trong hệ thống (tạo/sửa role, upload file, tiến trình job, thông báo thủ công) đều được tạo
bằng **một hàm duy nhất**:

```python
from services.notification_orchestrator import send_custom_notification
```

Hàm này làm trọn luồng chuẩn: dựng `NotificationEvent` → ghi `notifications` + `notification_recipients` +
`notification_deliveries` + `notification_outbox` trong 1 transaction (session DB riêng) → publish vào Redis
Stream (`direct`) hoặc giao Outbox Worker (`worker`) → Notification Server đẩy qua WebSocket.

**Hàm không bao giờ raise.** Lỗi (DB, Redis, không có người nhận...) được log và trả về
`{"status": "notification_failed", "error_type": ..., "error_message": ...}`. Vì vậy có thể gọi ngay sau khi
nghiệp vụ đã commit mà không sợ làm hỏng API.

### 15.1. Bước 1: đặt tên loại thông báo

`notification_type` có dạng `<đối_tượng>.<hành_động>`: chữ thường, số, `_`, và **ít nhất một dấu chấm**.
Ví dụ: `role.created`, `order.approved`, `file.shared`, `job.succeeded`.
Client dựa vào tên này để chọn cách hiển thị, nên mỗi loại sự kiện nghiệp vụ nên có tên riêng.

### 15.2. Bước 2: chọn người nhận

Có thể kết hợp nhiều tham số trong cùng một lần gọi. Stream trùng nhau sẽ tự được loại bỏ.

| Muốn gửi tới | Tham số | Redis Stream | Vào hộp thư (`/feed`)? |
|---|---|---|---|
| Một người dùng | `target_user_ids=["u1"]` | `stream:noti:tenant:{t}:user:u1` | Có |
| Một nhóm người | `target_user_ids=["u1", "u2", ...]` | mỗi người 1 stream `user` | Có |
| Toàn bộ admin của tenant | `include_tenant_admins=True` | stream `user` của từng admin | Có |
| Toàn bộ người dùng của tenant | `target_user_ids=await get_active_user_ids(db, tenant_id=...)` | stream `user` của từng người | Có |
| Màn hình dashboard của tenant | `send_to_tenant_dashboard=True` | `stream:noti:tenant:{t}:dashboard` | Không |
| Toàn hệ thống (monitoring) | `send_to_global=True` | `stream:noti:global` | Không |
| Người đang xem 1 job | `job_id="..."` | `stream:noti:tenant:{t}:job:{job_id}` | Không |

- Chỉ những người trong `target_user_ids` (và admin khi `include_tenant_admins=True`) mới có bản ghi
  trong `notification_recipients`, tức là thấy trong hộp thư và được tính vào số chưa đọc.
  Dashboard / global / job là kênh **chỉ realtime**, hợp với các thông báo trung gian (ví dụ tiến độ 10%, 40%...).
- Gửi cho **một nhóm theo tiêu chí** (phòng ban, role...): tự truy vấn danh sách `user_id` (theo mẫu
  `get_admin_user_ids` trong `db/query_users.py`) rồi truyền vào `target_user_ids`.
- Không có đích nào (danh sách rỗng và không bật tùy chọn nào) → `notification_failed` với `ValueError`.

### 15.3. Bước 3: gọi hàm trong controller, SAU KHI nghiệp vụ đã commit

```python
from schemas.noti_schema import (
    NotificationAction, NotificationActionType, NotificationResource, NotificationSeverity,
)
from services.notification_orchestrator import NotificationDispatchMode, send_custom_notification

# ... nghiệp vụ "duyệt đơn hàng" đã commit thành công ...
notification_result = await send_custom_notification(
    tenant_id=current_user["tenant_id"],
    current_user=current_user,                 # người thực hiện tác vụ (JWT payload)

    # Nội dung
    notification_type="order.approved",
    category="order",
    severity=NotificationSeverity.SUCCESS,     # info | success | warning | error | critical
    title="Đơn hàng đã được duyệt",
    body=f"Đơn {order_id} đã được duyệt bởi {current_user.get('user_name')}.",
    short_body=f"Đơn {order_id} đã được duyệt.",   # tùy chọn, cho toast

    # Người nhận
    target_user_ids=[order["created_by"]],     # người tạo đơn
    include_tenant_admins=False,
    send_to_tenant_dashboard=True,             # admin đang mở dashboard cũng thấy

    # Metadata (tùy chọn)
    resource=NotificationResource(type="order", id=order_id),
    action=NotificationAction(                 # bấm vào thông báo thì mở trang này
        type=NotificationActionType.NAVIGATE, target=f"/orders/{order_id}", label="Xem đơn",
    ),
    data={"order_id": order_id, "total": order["total"]},   # dữ liệu cho frontend
    deduplication_key=f"order:{current_user['tenant_id']}:{order_id}:approved",
    ttl_seconds=7 * 24 * 60 * 60,
    causation_id=f"order_approve:{order_id}",
    source_instance="order-controller",
    mode=NotificationDispatchMode.DIRECT,
)

result["data"]["notification"] = notification_result   # tùy chọn: trả kèm để debug
```

Ví dụ thật trong mã nguồn:

| Tác vụ | Nơi gọi | Người nhận |
|---|---|---|
| Tạo role (`role.created`) | `RoleController.create_new_role` | Tất cả admin + tenant dashboard + global |
| Sửa role (`role.updated`, kèm `changes` trước/sau) | `RoleController.update_role_info` | Tất cả admin + tenant dashboard + global |
| Gửi thủ công | `NotificationController.send_notification` (`POST /notifications/send`) | Theo body request |
| Upload file / job (`job.*`) | `services/job_notifier.py` (gọi từ API và Job Worker) | Chủ job + stream của job (mục 16) |

### 15.4. Bảng tham số

| Tham số | Mặc định | Ý nghĩa |
|---|---|---|
| `tenant_id`, `title`, `body`, `current_user` | bắt buộc | `current_user` cần `user_id`; có thể kèm `username`, `trace_id`, `correlation_id` |
| `notification_type` / `category` | `system.message` / `system` | Loại và nhóm thông báo |
| `severity` / `priority` | `info` / `normal` | Mức độ nội dung / mức ưu tiên gửi |
| `target_user_ids`, `include_tenant_admins`, `send_to_tenant_dashboard`, `send_to_global`, `job_id` | rỗng / `False` / `None` | Người nhận (mục 15.2) |
| `short_body`, `template_variables` | `None` | Nội dung rút gọn; biến cho template phía client |
| `resource` | `None` (hoặc `job` khi có `job_id`) | Đối tượng bị tác động |
| `action` | không có hành động | Hành động khi bấm (`NAVIGATE` + `target`) |
| `data` | `{}` | Dữ liệu nghiệp vụ tùy ý gửi kèm |
| `deduplication_key` | `None` | Chống trùng trong 300 giây (ví dụ khi client gửi lại request) |
| `ttl_seconds` | 1 ngày | Thời gian hiệu lực của thông báo |
| `causation_id`, `source_instance` | `notification_api:send`, `notification-api` | Truy vết: tác vụ / service nào sinh ra thông báo |
| `actor_type` | `USER` | `SYSTEM` khi tiến trình nền gửi (ví dụ Job Worker) |
| `mode` | `DIRECT` | `DIRECT`: publish ngay (trên `DIRECT_PUBLISH_MAX_OUTBOX`=200 stream thì tự giao worker). `WORKER`: chỉ ghi outbox, worker publish |

### 15.5. Lưu ý

1. **Gọi sau khi nghiệp vụ commit.** Thông báo dùng session riêng nên không rollback được nghiệp vụ, và
   ngược lại nghiệp vụ lỗi thì không được gửi thông báo "thành công".
2. **Trả connection DB trước khi gọi** nếu request vừa truy vấn mà chưa commit (`await db.rollback()`), để một
   request không giữ 2 connection cùng lúc (xem mục 3).
3. **Kiểm tra kết quả khi gửi thông báo là nghiệp vụ chính** (như `POST /notifications/send`): nếu
   `status == "notification_failed"` thì trả lỗi HTTP cho client. Với API nghiệp vụ khác thì chỉ cần log.
4. **Gửi rất nhiều người** (cả tenant) → dùng `mode=NotificationDispatchMode.WORKER` để request trả về nhanh.
5. **Không cần sửa Notification Server hay client.** Client đã nghe đúng channel sẽ nhận được ngay; frontend
   chỉ cần xử lý thêm `notification_type` mới (icon, điều hướng theo `action.target`).
6. **Quyền:** tự kiểm tra quyền của người gọi trước khi gửi cho người khác (mẫu: `notification:send` trong
   `NotificationController`).

---

## 16. Luồng xử lý file/job

Người dùng upload file → API trả lời ngay → Job Worker xử lý ở nền → người dùng nhận tiến trình realtime và
thông báo kết quả. Mọi thông báo đều đi qua `send_custom_notification` (thông qua `services/job_notifier.py`).

```
Client ──POST /jobs/upload (file + JWT)──► API Server
                                             │ 1. Lưu file: <UPLOAD_DIRECTORY>/<tenant>/<job_id>/input/<file>
                                             │ 2. INSERT job_record (status=queued)
                                             │ 3. LPUSH queue:jobs {"tenant_id","job_id"}
                                             │ 4. Thông báo job.queued ──► chủ job + stream job
                                             ▼
Client ◄── 200 {job_id, watch: {channel: "job"}}

Job Worker (N làn song song)
   │ 5. BRPOP queue:jobs
   │ 6. Claim nguyên tử: UPDATE job_record SET status=processing, attempts+1 WHERE status='queued'
   │ 7. Thông báo job.started
   │ 8. process_file(): mỗi mốc → UPDATE progress + thông báo job.progress (10/40/70/90%)
   │ 9a. Xong   → status=success, result_object_key → thông báo job.succeeded (chủ job + stream job)
   │ 9b. Lỗi tạm thời, còn lượt → status=queued, chờ rồi LPUSH lại → thông báo job.retrying
   │ 9c. Lỗi nghiệp vụ / hết lượt → status=failed → thông báo job.failed (chủ job + stream job + dashboard)
   ▼
SQL outbox ──► Redis Stream ──► Notification Server ──► Client
                                                         └─ job.succeeded → GET /jobs/{job_id}/result
```

### 16.1. Thông báo theo từng giai đoạn

| Giai đoạn | `notification_type` | Gửi tới | Vào hộp thư? | Severity |
|---|---|---|---|---|
| API nhận file | `job.queued` | Chủ job + stream job | Có | info |
| Worker bắt đầu | `job.started` | Stream job | Không | info |
| Đang xử lý | `job.progress` | Stream job | Không | info |
| Lỗi, sẽ thử lại | `job.retrying` | Stream job | Không | warning |
| Thành công | `job.succeeded` | Chủ job + stream job | Có | success |
| Thất bại hẳn | `job.failed` | Chủ job + stream job + tenant dashboard | Có | error |

Các thông báo trung gian chỉ đi vào stream của job, để hộp thư của người dùng không bị đầy bởi hàng chục
thông báo tiến độ. Màn hình chi tiết job nghe channel `job`; phần còn lại của app nghe channel `user` và chỉ nhận
kết quả cuối. `data` của mọi thông báo job có `job_id`, `filename`, `status`, `stage`, `progress`, `attempts`, `error`.

### 16.2. Các thành phần

| File | Vai trò |
|---|---|
| `api/job_api.py` | Router `/jobs` |
| `controllers/job_controller.py` | Upload, tra cứu, tải kết quả; kiểm tra quyền |
| `services/job_storage.py` | Lưu file (làm sạch tên file, chặn path traversal, giới hạn dung lượng) |
| `services/job_queue.py` | `enqueue_job` (LPUSH) / `dequeue_job` (BRPOP) trên `queue:jobs` |
| `services/job_notifier.py` | `notify_job(job, stage)`: chọn nội dung + người nhận, gọi `send_custom_notification` |
| `services/job_processor.py` | `process_file()`: bước xử lý (bản demo: SHA-256, đếm dòng/từ, ghi `result.json`) |
| `workers/job_worker.py` | Vòng lặp worker: claim, xử lý, thử lại, khôi phục job kẹt |
| `db/query_job_record.py` | `claim_job_for_processing`, `requeue_job_for_retry`, `recover_stale_jobs`... |

### 16.3. Quyền

| Hành động | Quyền |
|---|---|
| Upload file (`POST /jobs/upload`) | `job:create` |
| Xem / tải kết quả job của mình | Chỉ cần đăng nhập |
| Xem / tải job của người khác, `GET /jobs?all_users=true` | `job:read_all` |
| Nghe channel `job` (`ws-ticket?channel=job&job_id=`) | `notification:job_watch` **và** là chủ job (hoặc có `job:read_all`) |

Superuser có mọi quyền. Job của người khác luôn trả `404` (không tiết lộ job đó có tồn tại hay không).

### 16.4. Độ tin cậy

- **Nguồn sự thật là `job_record`**, Redis queue chỉ là tín hiệu. Đẩy trùng một job là vô hại vì bước claim
  (`WHERE status='queued'`) chỉ cho 1 worker nhận job. Có thể chạy nhiều tiến trình Job Worker.
- **Redis lỗi khi upload:** API vẫn trả thành công (`enqueued: false`), job nằm ở `queued` trong DB.
- **Worker chết giữa chừng / mất phần tử trong queue:** cứ `JOB_RECOVER_EVERY_SECONDS` (60 giây), worker tìm job
  `processing`/`queued` không cập nhật quá `JOB_STALE_AFTER_SECONDS` (600 giây), đưa về `queued` và đẩy lại hàng đợi.
  Mỗi mốc tiến độ đều cập nhật `updated_at`, nên job đang chạy lâu không bị coi là kẹt (miễn là khoảng cách giữa
  2 mốc nhỏ hơn 600 giây).
- **Thử lại:** lỗi bất kỳ được thử lại tối đa `JOB_MAX_ATTEMPTS` (3) lần, chờ `JOB_RETRY_DELAY_SECONDS × số lần đã thử`.
  Raise `JobProcessingError` để báo lỗi không nên thử lại (file hỏng, sai định dạng): job chuyển `failed` ngay.
- **File kết quả** được ghi ra file tạm rồi đổi tên, nên client không bao giờ tải phải file dở dang.

### 16.5. Cấu hình (`env/.env.api`)

| Biến | Mặc định | Ý nghĩa |
|---|---|---|
| `UPLOAD_DIRECTORY` | `/app/uploads` | Thư mục lưu file. Đường dẫn tương đối tính từ `fastapi_server/src`. **API và Job Worker phải dùng chung** |
| `JOB_MAX_UPLOAD_MB` | 50 | Dung lượng file tối đa (vượt → `413`) |
| `JOB_WORKER_CONCURRENCY` | 2 | Số job xử lý song song trong 1 tiến trình worker |
| `JOB_MAX_ATTEMPTS` | 3 | Số lần thử tối đa |
| `JOB_RETRY_DELAY_SECONDS` | 5 | Thời gian chờ trước khi thử lại (nhân theo số lần đã thử) |
| `JOB_STALE_AFTER_SECONDS` / `JOB_RECOVER_EVERY_SECONDS` | 600 / 60 | Ngưỡng coi job bị kẹt / chu kỳ quét |
| `JOB_WORKER_POLL_SECONDS` | 5 | Thời gian chờ BRPOP (cũng là độ trễ tối đa khi dừng worker) |
| `JOB_DEMO_STEP_SECONDS` | 1 | Thời gian nghỉ giả lập mỗi bước của bộ xử lý demo (đặt 0 khi xử lý thật) |

Upload đi qua bucket rate limit `upload` (`RATE_LIMIT_UPLOAD_PER_MIN`, mặc định 20 lần/phút/IP).

### 16.6. Chạy thử

```powershell
# Terminal 1, 2, 3 như mục 9.4, thêm Terminal 2b:
python fastapi_server\src\workers\job_worker.py

# Terminal 5: upload, lệnh in ra job_id và lệnh để theo dõi
python client\client_call_api.py upload-file --file README.md

# Terminal 4: theo dõi tiến trình của job vừa tạo
python client\listen_noti_server.py --channel job --job <job_id>

# Tra cứu trạng thái / tải kết quả (Swagger hoặc curl, kèm Authorization: Bearer <JWT>)
GET http://127.0.0.1:8000/jobs/<job_id>
GET http://127.0.0.1:8000/jobs/<job_id>/result
```

Client nghe channel `job` nhận lần lượt: `job.queued` → `job.started` → `job.progress` 10%, 40%, 70%, 90% →
`job.succeeded`. Client nghe channel `user` chỉ nhận `job.queued` và `job.succeeded`.

Hoặc dùng app giao diện (`python client\notification_app.py`): bấm "📤 Tải file", thanh tiến trình trong khung "Công việc" chạy theo thời gian thực, xong thì bấm "⬇ Kết quả" để lưu file (mục 6.1).

### 16.7. Thay bộ xử lý demo bằng xử lý thật

Viết lại `process_file()` trong `services/job_processor.py`, giữ nguyên chữ ký:

```python
async def process_file(*, job, input_path, output_path, report) -> dict:
    await report(10, "Đang đọc file")
    data = await asyncio.to_thread(read_excel, input_path)      # việc nặng/đồng bộ: chạy trong thread
    if data is None:
        raise JobProcessingError("File không đúng định dạng")    # không thử lại
    await report(50, "Đang nhận dạng")
    result = await asyncio.to_thread(run_ocr, data)
    await asyncio.to_thread(write_result, output_path, result)
    await report(90, "Đã ghi kết quả")
    return {"size_bytes": input_path.stat().st_size, "pages": len(result)}   # size_bytes dùng trong thông báo
```

Worker tự lo phần cập nhật `job_record`, gửi thông báo, thử lại và khôi phục. Nếu kết quả không phải JSON,
đổi `result_object_key()` trong `job_storage.py` và `media_type` trong `JobController.download_result`.
Muốn thêm giai đoạn hoặc đổi người nhận (ví dụ báo cả nhóm duyệt khi job xong), chỉ cần sửa `notify_job()`.

