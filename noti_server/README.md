Máy chủ Notification là nơi trung gian, luôn đọc các event từ Redis (Nơi lưu trữ thông báo chung của hệ thống) và gửi nó đến các client đang mở kết nối WebSocket, và client đó phải là người được phép nhận thông báo đó.  
Máy chủ Redis sử dụng đẩy thông báo có hai loại: `Redis Sream` hoặc `Redis Pub/Sub`.  
Ta chọn `Redis Stream` vì một số lý do sau:  
- Nó như một cuốn sổ, cho phép ghi lại các nhật ký, vì vậy khi cần ta có thể truy xuất thông tin cũ (trong trường hợp client mất kết nối, họ kết nối lại thì đọc các sự kiện cũ mà họ bị mất trong lúc disconnect)  
- Redis Streams hỗ trợ nhiều cách đọc như XREAD, XREADGROUP, XRANGE và trimming để tránh stream tăng vô hạn, nên nó phù hợp để lưu lịch sử event và replay khi client reconnect.  
Còn `Redis Pub/Sub`:  
- Nó nhưu một loa phát thanh trực tiếp, có event nào sẽ trực tiếp gửi tới các client đang kết nối và phụ hợp (Không có cơ chế lưu lịch sử nên dẫn đến việc nếu client disconnect lúc có sự kiện thì client ko thể nhận lại)  

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