Máy chủ Notification là nơi trung gian, luôn đọc các event từ Redis (Nơi lưu trữ thông báo chung của hệ thống) và gửi nó đến các client đang mở kết nối WebSocket, và client đó phải là người được phép nhận thông báo đó.  
Máy chủ Redis sử dụng đẩy thông báo có hai loại: `Redis Sream` hoặc `Redis Pub/Sub`.  
Ta chọn `Redis Stream` vì một số lý do sau:  
- Nó như một cuốn sổ, cho phép ghi lại các nhật ký, vì vậy khi cần ta có thể truy xuất thông tin cũ (trong trường hợp client mất kết nối, họ kết nối lại thì đọc các sự kiện cũ mà họ bị mất trong lúc disconnect)  
- Redis Streams hỗ trợ nhiều cách đọc như XREAD, XREADGROUP, XRANGE và trimming để tránh stream tăng vô hạn, nên nó phù hợp để lưu lịch sử event và replay khi client reconnect.  
Còn `Redis Pub/Sub`:  
- Nó như một loa phát thanh trực tiếp, có event nào sẽ trực tiếp gửi tới các client đang kết nối và phụ hợp (Không có cơ chế lưu lịch sử nên dẫn đến việc nếu client disconnect lúc có sự kiện thì client ko thể nhận lại)  

# I. Cách máy chủ thực hiện
Hiện tại với mỗi client kết nối tới `Notification Server`, thì máy chủ này mở một vòng lặp `XREAD BLOCK` tới `Redis Stream` để đọc các event và gửi tới client, nếu không có event nào thì máy chủ sẽ gửi một heartbeat để client biết kết nối vẫn sống.  
Kết nối WebSocket có thể bị proxy, load balancer hoặc mạng đóng nếu quá lâu không có dữ liệu. Heartbeat giúp giữ kết nối sống và giúp client biết server vẫn hoạt động.  
Có 2 cách để giữ kết nối lâu dài:  
```bash
Cách 1:
Server định kỳ gửi heartbeat

Cách 2:
Client định kỳ gửi ping, server trả pong
```

Tuy nhiên nếu nhiều client online, ví dụ 5.000 client, thì sẽ có rất nhiều vòng XREAD BLOCK. Vì vậy khi scale tốt hơn, nên chuyển sang `Connection Manager`:  
```bash
Mỗi WebSocket:
- chỉ đăng ký vào ConnectionManager

Một background Redis listener:
- nghe Pub/Sub một lần
- nhận event
- tìm đúng WebSocket trong ConnectionManager
- gửi event cho client
```
Tức là thay vì “mỗi người tự đứng ngoài kho Redis chờ thông báo”, ta có “một người trực tổng đài nghe Redis, rồi phát lại cho đúng người trong phòng”.  

> Tuy nhiên phương pháp này chưa có thời gian thực hiện, hãy cập nhật nó trong tương lai nhé Quân. 08/06/2026  


# II. Cách client kết nối tới websocket.

Chúng ta không thể để bất cứ ai có api là đều có thể lắng nghe được các sự kiện, vì vậy mỗi khi có 1 người gửi yêu cầu tới máy chủ, ta phải xác thực người đó là ai, và họ có quyền được lắng nghe thông báo cho danh mục/ công việc/ tổ chức đó hay không.  

Ta có thể sử dụng token của người dùng để xác thực người dùng và xác định quyền hạn của họ thông qua giải mã token họ gửi lên. Trong `WebSoket` thì không thể gửi token thông qua kiểu data như `Authortication: Bear xxx` được mà ta phải đính kèm nó vào `param query`:  

```bash
ws://.../ws/events?token=token_được_dán_vào_đây
```
Tuy nhiên việc dán token vào chính url sẽ dễ bị lộ vì:  

- URL có thể bị ghi vào access log của Nginx, API gateway, load balancer.  
- URL có thể xuất hiện trong browser history, monitoring, error tracking.  
- Nếu token là access token thật, người khác lấy được URL là có thể dùng token đó.  
- Query string dễ bị lộ hơn header hoặc cookie.  

Vì vậy ta sử dụng `Websocket ticket` như một tấm vé để cho phép xác thực và mở kết nối tới `Notification Server`.  
Mô hình chung cho kết nối tới máy chủ notification như sau:  

```bash
Bước 1:
Client đã login và có access token từ API Server

Bước 2:
Client gọi API từ API Server:
POST /notifications/ws-ticket?channel=user

Bước 3:
API kiểm tra access token bằng Authorization header

Bước 4:
API tạo ticket ngắn hạn và lưu trữ nó lên Redis để Notification Server có thể đọc được, ví dụ sống 60 giây

Bước 5:
Client mở WebSocket:
wss://example.com/ws/events?ticket=abc123&last_event_id=101-0

Bước 6:
Notification server kiểm tra ticket

Bước 7:
Nếu ticket hợp lệ:
- lấy tenant_id, user_id, permissions
- xóa ticket khỏi Redis
- accept WebSocket

Bước 8:
WebSocket sống lâu dài
```

Ticket hết hạn sau 60 giây không làm WebSocket bị ngắt. Ticket chỉ là vé vào cửa. Khi đã vào trong phòng monitor rồi thì bạn không cần đưa vé lại nữa. Chỉ khi mất kết nối và muốn vào lại thì xin vé mới.
Ta có luồng hoạt động chính như sau.  

```bash
Client đã login
    ↓
Client gọi HTTP API: POST /notifications/ws-ticket
    ↓
API kiểm tra JWT bằng Authorization header hoặc cookie
    ↓
API tạo ws_ticket ngẫu nhiên, TTL 30-60 giây, lưu Redis
    ↓
Client kết nối WebSocket bằng ticket:
    wss://example.com/ws/events?ticket=<WS_TICKET>
    ↓
Notification service kiểm tra ticket trong Redis
    ↓
Nếu hợp lệ thì xóa ticket để chống dùng lại
    ↓
WebSocket bắt đầu nhận event
```
T
# III. Phiên bản 2.0 — vận hành theo chuẩn production (08/11/2026)

## 1. Cấu trúc mã nguồn
```
noti_server/
├── Dockerfile, requirements.txt, .env.example
├── DEMO.md                           # Hướng dẫn demo từng bước
├── demo/noti_demo.py, demo/demo.html # Công cụ demo: ticket / listen / publish / load
└── src/
    ├── main.py                       # Khởi động app, lifespan (kiểm tra Redis, tắt an toàn)
    ├── config/configs.py             # Mọi cấu hình đọc từ biến môi trường + kiểm tra chéo
    ├── core/logging_config.py        # Log text/JSON, tự che ticket/token trong log
    ├── api/noti_api.py               # 4 endpoint WebSocket (dùng chung 1 luồng xác thực)
    ├── api/health_api.py             # /health/live, /health/ready, /health/stats
    ├── controllers/noti_controller.py# Vòng đời 1 kết nối: replay + live + nhận ping
    ├── services/connection_manager.py# Giới hạn kết nối, thống kê, đóng kết nối khi tắt
    ├── services/stream_hub.py        # Đọc Redis Stream dùng chung (vài shard) cho mọi WebSocket
    ├── services/redis_client.py      # 2 pool Redis: lệnh nhanh và XREAD BLOCK
    ├── services/ws_ticket_service.py # GETDEL ticket (nguyên tử, dùng 1 lần)
    └── utils/                        # auth, close_codes, events, redis_keys, utils
```

## 2. Chạy
```bash
# Local (đứng tại noti_server/src, cần Redis >= 6.2, khuyến nghị 7.x)
python main.py

# Docker (từ thư mục gốc dự án)
docker compose up -d --build noti_server
```

## 3. Hợp đồng message gửi cho client
| type | Có event_id? | Ý nghĩa |
|---|---|---|
| `connected` | Không | Kết nối thành công, kèm `connection_id`, `heartbeat_interval_ms` |
| *(event nghiệp vụ)* | **Có** | `event_id` = `stream_id` = Redis Stream ID → lưu làm `last_event_id`. ID nghiệp vụ gốc nằm ở `source_event_id` |
| `heartbeat` | Không | Không có event trong `HEARTBEAT_INTERVAL_SECONDS`; `stream_status` = `ok` hoặc `degraded` (mất Redis tạm thời) |
| `replay_incomplete` | Không | Đã mất event (`reason`: `limit` hoặc `trimmed`) → client gọi REST API đồng bộ lại |
| `pong` | Không | Trả lời khi client gửi `ping` hoặc `{"type":"ping"}` |
| `server_error` | Không | Báo lỗi chung chung trước khi đóng (chi tiết chỉ nằm trong log server) |

Field dạng số/bool được giữ **nguyên là chuỗi** như publisher đã ghi; chỉ field là JSON object/array mới được parse.

## 4. Close code
Xem bảng đầy đủ trong `src/utils/close_codes.py`. Tóm tắt: `4401` xin ticket mới; `4403`/`4400` dừng, không reconnect;
`1012` reconnect ngay; `1013`/`4429` reconnect sau một lúc; `1011`/`4408` reconnect với backoff.

## 5. Những lỗi đã sửa so với bản 1.0
- `event_id` của luồng Outbox ghi đè Stream ID → client replay toàn bộ stream mỗi lần reconnect.
- Ticket GET rồi DELETE (2 lệnh) → có thể dùng 1 ticket 2 lần. Nay dùng `GETDEL`.
- `FastAPI(path_prefix=...)` không tồn tại, bị bỏ qua im lặng → comment ghi sai đường dẫn `/quan/noti/...`.
- Không đọc từ client → phát hiện client ngắt chậm tới 15s, giữ kết nối Redis vô ích.
- `json.loads` mọi field → `"123"` thành số, `"NaN"` sinh JSON không hợp lệ.
- `last_event_id="1--5"` lọt kiểm tra → lỗi Redis.
- Gửi nguyên `str(exception)` cho client → lộ thông tin hạ tầng.
- Không đóng Redis khi tắt, `print` thay cho log, không giới hạn số kết nối (pool Redis cạn khi đông người).

## 6. Stream Hub — phục vụ hàng nghìn kết nối (đã làm 08/11/2026)
Phần "Connection Manager" dự định ở mục I đã được hiện thực trong `services/stream_hub.py`:
- Mỗi process chỉ có `HUB_SHARDS` (mặc định 4) task đọc Redis. Mỗi task dùng **một** lệnh `XREAD BLOCK`
  cho hàng nghìn stream, nên số kết nối Redis không còn tăng theo số client.
- Mỗi event chỉ chuyển sang JSON **một lần** rồi phát vào hàng đợi của mọi WebSocket đang nghe stream đó.
- Client nhận chậm làm đầy hàng đợi (`HUB_SUBSCRIBER_QUEUE_SIZE`) sẽ bị ngắt với mã 4408, tránh phình RAM.
- Redis sự cố: shard tự thử lại, client không bị ngắt (heartbeat `stream_status=degraded`) và nhận bù
  event khi Redis trở lại.
- Kiểm chứng: 2000 kết nối nhận 20.000/20.000 event, server chỉ dùng 4 kết nối Redis cho XREAD (xem DEMO.md mục 6).

Hướng mở rộng tiếp: khi cần hơn khoảng 10.000 kết nối, chạy nhiều instance sau load balancer (không cần sticky
session vì mọi trạng thái nằm ở Redis). Muốn giới hạn số kết nối mỗi user trên toàn cụm thì đếm trong Redis.
