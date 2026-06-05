import smtplib
from smtplib import SMTPException
import threading
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
import re
import datetime
import time
import os
from dotenv import load_dotenv
from log.system_log import system_logger


load_dotenv()  # Tự động tìm và nạp file .env ở thư mục hiện tại


# Các thông số cho email
EMAIL_SENDER = os.getenv("EMAIL_SENDER", "No-Reply-Terumo@terumo.com")
PASSWORD_EMAIL_SENDER = os.getenv("PASSWORD_EMAIL_SENDER", "0")
SMTP_HOST = os.getenv("SMTP_HOST", "12.34.56.78")
SMTP_PORT = os.getenv("SMTP_PORT", "910")
EMAIL_SERVICES = os.getenv("EMAIL_SERVICES", "Internal")



MAX_ATTACHMENT_SIZE_MB = 200  # Giới hạn dung lượng tệp đính kèm (200MB)
MAX_RETRY_ATTEMPTS = 3  # Số lần thử gửi email tối đa

class InternalEmailSender():
    """
    Gửi email tới người dùng  
    Khi sử dụng dịch vụ Internal thì cần có địa chỉ host SMTP và PORT email service của công ty
    """
    def __init__(self, email = EMAIL_SENDER, password = PASSWORD_EMAIL_SENDER, smtp_host = SMTP_HOST, smtp_port = SMTP_PORT, email_service = EMAIL_SERVICES ):
        """
        Phân loại dịch vụ mail và mở cổng tương ứng
        """
        self.email = email
        self.password = password
        self.email_service = email_service.lower()
        self.smtp_host = smtp_host
        self.smtp_port = smtp_port

        # Kết nối thử tới email
        # server = self.connect_to_email_server()
        # if server:
        #     system_logger.info(f"Kết nối thành công tới email: {self.email}")
        # else:
        #     system_logger.error(f"Không thể kết nối tới email: {self.email}")

        # Tạo chữ ký mặc định cho email
        self.signature_email = """
                <br>
                <p style="color: #000000; font-size: 17px; font-weight: bold; line-height: 1; font-family: Calibri, sans-serif;">
                    Thank and best regards,
                </p>
                <p style="color: #000000; font-size: 17px; font-weight: bold; line-height: 1; font-family: Calibri, sans-serif;">
                    Ứng dụng phát triển bởi Nguyễn Đức Quân
                </p>
                <br>
            """

    def connect_to_email_server(self):
        """
        Kết nối tới máy chủ email theo từng dịch vụ mail
        """
        try:
            # Nếu người dùng sử dụng gmail hoặc outlook
            if self.email_service == 'gmail' or self.email_service == 'outlook':
                # Thiết lập kết nối đến SMTP server
                server = smtplib.SMTP(self.smtp_host, self.smtp_port)
                server.starttls()  # Bật mã hóa TLS
                server.login(self.email, self.password)

            # Cấu hình cho Email nội bộ
            elif self.email_service == 'internal':
                # Thiết lập kết nối đến SMTP server nội bộ
                server = smtplib.SMTP(self.smtp_host, self.smtp_port)

            else:
                # Ghi log lại nếu dịch vụ không hợp lệ
                system_logger.error(f"Chương trình chỉ hỗ trợ Gmail, Outlook hoặc Internal, vui lòng chọn đúng dịch vụ. Không sử dụng {self.email_service}")
                return None
            
            return server

        except SMTPException as e:
            system_logger.error(f"Lỗi khi đăng nhập email: {str(e)}")
            return None

    def is_valid_email(self, email):
        """
        Kiểm tra tính hợp lệ của địa chỉ email bằng biểu thức chính quy.
        """
        regex = r'^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$'
        return re.match(regex, email) is not None
    
    def attach_file(self, msg, attachment_path):
        """
        Đính kèm tệp vào email.
        
        :param msg: Đối tượng email.
        :param attachment_path: Đường dẫn tệp đính kèm.
        """
        # Lấy tên tệp từ đường dẫn
        filename = os.path.basename(attachment_path)

        if not os.path.exists(attachment_path):
            system_logger.warning(f"Không tìm thấy tệp tin: {attachment_path}. Gửi mail mà không đính kèm tệp tin.")
            return
        
        # Kiểm tra dung lượng tệp (chuyển sang MB và so với giới hạn 200MB)
        file_size_mb = os.path.getsize(attachment_path) / (1024 * 1024)
        if file_size_mb > MAX_ATTACHMENT_SIZE_MB:
            system_logger.warning(f"Tệp đính kèm {filename} có dung lượng {file_size_mb:.2f}MB, vượt quá giới hạn {MAX_ATTACHMENT_SIZE_MB}MB. Không gửi tệp này.")
            return

        try:
            # Mở tệp và đính kèm vào email
            with open(attachment_path, 'rb') as attachment:
                part = MIMEBase('application', 'octet-stream')
                part.set_payload(attachment.read())
                encoders.encode_base64(part)
                part.add_header('Content-Disposition', f'attachment; filename={filename}')
                msg.attach(part)
        except Exception as e:
            system_logger.error("Không thể đính kèm tệp tin: %s. Lỗi xuất hiện: %s. Gửi mail mà không đính kèm tệp tin", attachment_path, e)

    def send_email(self, to_email, subject, body, signature='', attachment_path=None, cc_email=None):
        """
        Gửi email với nội dung và tệp đính kèm.  
        Thử gửi tối đa 3 lần nếu thất bại.  
        to_email: Địa chỉ email người nhận.  
        subject: Tiêu đề email.  
        body: Nội dung email.  
        signature: Chữ ký email (HTML).  
        attachment_path: Đường dẫn tệp đính kèm.  
        cc_email: Địa chỉ email CC (có thể là 1 string hoặc list)  
        """
        success_send_email = False
        retry_attempts = 0

        # Kiểm tra email hợp lệ
        if not self.is_valid_email(to_email):
            system_logger.error("Địa chỉ email %s không hợp lệ.", to_email)
            return success_send_email

        # Cố gắng gửi email tối đa 3 lần
        while retry_attempts < MAX_RETRY_ATTEMPTS and not success_send_email:
            try:
                # Tạo đối tượng email
                msg = MIMEMultipart()
                msg['From'] = self.email
                msg['To'] = to_email
                msg['Subject'] = subject

                # Nếu có CC
                recipients = [to_email]
                if cc_email:
                    if isinstance(cc_email, str):
                        msg['Cc'] = cc_email
                        recipients.append(cc_email)
                    elif isinstance(cc_email, list):
                        msg['Cc'] = ', '.join(cc_email)
                        recipients.extend(cc_email)

                # Thêm nội dung email với mã hóa UTF-8
                email_body = body + signature
                msg.attach(MIMEText(email_body, 'html', _charset='utf-8'))

                # Nếu có tệp đính kèm
                if attachment_path:
                    self.attach_file(msg, attachment_path)

                # Thiết lập kết nối đến SMTP server
                server = self.connect_to_email_server()

                if server:
                    # Gửi email
                    server.sendmail(self.email, recipients, msg.as_string())
                    # Đóng kết nối
                    server.quit()

                    # Trả về thành công
                    success_send_email = True
                    system_logger.info(f"Email gửi thành công tới {to_email}.")

                else:
                    raise ValueError("Không thể kết nối tới máy chủ mail")

            except Exception as e:
                system_logger.error(f"Gửi email tới {to_email} thất bại lần {retry_attempts + 1}. Lỗi: {e}")
                retry_attempts += 1
                time.sleep(5)  # Delay trước khi thử lại

        return success_send_email

    def send_email_async(self, to_email, subject, body, signature='', attachment_path=None, callback=None, cc_email=None):
        """
        Gửi email trong một luồng riêng biệt và gọi callback khi hoàn thành.
        Để xác nhận xem email gửi thành công hay không
        """
        def run():
            success = self.send_email(to_email, subject, body, signature, attachment_path, cc_email)

            # Gọi hàm callback nếu có (hàm này có thể là gửi lại email lần nữa nếu success trả về false chẳng hạn)
            if callback:
                callback(to_email, success)

            return success
        
        # Gọi hàm gửi mail trong thread
        email_thread = threading.Thread(target=run)
        email_thread.start()

    def send_mail_alert(self, to_email, subject_mail, ip, reason, path_api, user_agent, time_ban, attachment_path=None, callback=None):
        """
        Gửi email cảnh báo tới quản trị viên hệ thống.
        """
        body_alert = f"""
        <html>
            <body style="font-family: Arial, sans-serif; color: #333;">
                <p>Thông báo từ hệ thống,</p>
                <p>Hệ thống vừa ngăn chặn truy cập của thiết bị có IP: <strong>{ip}</strong></p>
                <p><strong>Lý do:</strong> {reason}</p>
                <p><strong>Đường dẫn truy cập:</strong> {path_api}</p>
                <p><strong>User-Agent:</strong> {user_agent or '-'}</p>
                <p><strong>Thời gian bị chặn (giây):</strong> {time_ban}</p>
                <br>
                <p>Đây là mail tự động, vui lòng không phản hồi mail này!</p>
            </body>
        </html>
        """
        # Gửi email với nội dung và chữ ký
        success_send_email = self.send_email_async(to_email=to_email, subject=subject_mail, 
                                                body=body_alert, signature=self.signature_email, 
                                                attachment_path=attachment_path, callback=callback)
    
        return success_send_email
    
    def send_mail_for_redis_crash(self, to_email, reason, attachment_path = None, callback = None):
        """
        Gửi mail thông báo về việc không thể kết nối tới redis
        
        :param self: Description
        :param to_email: Description
        :param attachment_path: Description
        :param callback: Description
        """
        subject_mail = f"Không thể kết nối tới Redis"
        body_alert = f"""
        <html>
            <body style="font-family: Arial, sans-serif; color: #333;">
                <p>Hệ thống không thể truy cập được Redis</p>
                <p>Hệ thống không thể kết nối tới Redis trong thời gian dài, vui lòng kiểm tra lại máy chủ Redis để đảm bảo các chức năng hoạt động bình thường.</p> 
                <p> {reason}</p>
                <br>
                <p>Đây là mail tự động, vui lòng không phản hồi mail này!</p>
            </body>
        </html>
        """
        # Gửi email với nội dung và chữ ký
        success_send_email = self.send_email_async(to_email = to_email, subject = subject_mail, 
                                             body= body_alert, signature = self.signature_email, 
                                             attachment_path = attachment_path, callback= callback)
        
        return success_send_email

    def send_email_for_new_account(self, to_email, name, website_name, attachment_path=None, callback=None):
        """
        Gửi email thông báo tài khoản và mật khẩu đến người dùng tạo mới mật khẩu
        """
        subject_mail = f"Tạo tài khoản mới tại {website_name}"
        body_account_creation = f"""
        <html>
            <body style="font-family: Arial, sans-serif; color: #333;">
                <h2 style="color: #007BFF;">Tạo tài khoản thành công!</h2>
                <p>Chào {name},</p>
                <p><strong>{website_name}</strong> thông báo rằng tài khoản của bạn đã được tạo mới thành công.</p>
                <p>Để có thể truy cập vào <strong>{website_name}</strong>, hãy liên hệ với nhà phát triển để kích hoạt tài khoản.</p>
                <p></p>
                <p>Đây là mail tự động, vui lòng không phản hồi mail này!</p>
            </body>
        </html>
        """
        # Gửi email với nội dung và chữ ký
        success_send_email = self.send_email_async(to_email = to_email, subject = subject_mail, 
                                             body= body_account_creation, signature = self.signature_email, 
                                             attachment_path = attachment_path, callback= callback)
        
        return success_send_email

    def send_mail_for_activate_account(self, to_email, name, website_name, username, email, attachment_path=None, callback=None):
        """
        Gửi email thông báo tài khoản đã được kích hoạt.
        """
        subject_mail = f"Tài khoản của bạn đã được kích hoạt tại {website_name}"
        body_account_creation = f"""
        <html>
            <body style="font-family: Arial, sans-serif; color: #333;">
                <h2 style="color: #007BFF;">Chúc mừng bạn! Tài khoản của bạn đã được kích hoạt thành công!</h2>
                <p>Chào {name},</p>
                <p>Chúng tôi xin thông báo rằng tài khoản của bạn tại <strong>{website_name}</strong> đã được kích hoạt thành công. Bạn có thể bắt đầu sử dụng dịch vụ ngay bây giờ.</p>
                <p><strong>Thông tin tài khoản của bạn:</strong></p>
                <ul>
                    <li><strong>Tên đăng nhập:</strong> {username}</li>
                    <li><strong>Email:</strong> {email}</li>
                </ul>
                <p>Hãy truy cập vào trang web của chúng tôi và đăng nhập để trải nghiệm các dịch vụ. Nếu bạn gặp bất kỳ vấn đề nào trong quá trình sử dụng, đừng ngần ngại liên hệ với chúng tôi.</p>
                <p><strong>Đây là email tự động, vui lòng không phản hồi trực tiếp email này.</strong></p>
                <br>
                <p style="font-size: 12px; color: #777;">Nếu bạn không phải là người yêu cầu kích hoạt tài khoản này, vui lòng liên hệ với chúng tôi ngay lập tức.</p>
            </body>
        </html>
        """
        # Gửi email với nội dung và chữ ký
        success_send_email = self.send_email_async(to_email=to_email, subject=subject_mail, 
                                                body=body_account_creation, signature=self.signature_email, 
                                                attachment_path=attachment_path, callback=callback)
    
        return success_send_email

    def send_email_for_password_reset(self, to_email, name, website_name, OTP, attachment_path=None, callback= None):
        """
        Gửi email thông báo thay đổi mật khẩu cho người dùng khi quên mật khẩu.
        """
        subject_mail = f"Thay đổi mật khẩu tài khoản tại {website_name}"
        body_password_reset = f"""
        <html>
            <body style="font-family: Arial, sans-serif; color: #333;">
                <h2 style="color: #b34700;">Mã OTP thay đổi mật khẩu!</h2>
                <p>Chào {name},</p>
                <p>Chúng tôi muốn thông báo rằng mật khẩu tài khoản của bạn tại <strong>{website_name}</strong> đã được yêu cầu thay đổi.</p>
                <p>Vui lòng sử dụng mã OTP dưới đây để thay đổi mật khẩu tài khoản của bạn:</p>
                <ul>
                    <li><strong>Mã OTP:</strong> {OTP}</li>
                </ul>
                <p>Lưu ý mã OTP chỉ có hiệu lực trong 10 phút!</p>
                <p>Vui lòng truy cập hệ thống nếu bạn không phải là người thực hiện thay đổi này.</p>
            </body>
        </html>
        """
        # Gửi email với nội dung và chữ ký
        success_send_email = self.send_email_async(to_email, subject = subject_mail,
                                              body = body_password_reset, signature = self.signature_email,
                                                attachment_path = attachment_path, callback= callback)
        return success_send_email

    def send_mail_on_startup(self, to_email, website_name, timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")):
        """
        Gửi email thông báo khi ứng dụng FastAPI đã được khởi động thành công.
        """
        subject_mail = f"Thông báo: Máy chủ {website_name} đã được khởi động"
        body_startup_notification = f"""
        <html>
            <body style="font-family: Arial, sans-serif; color: #333;">
                <h2 style="color: #007BFF;">Máy chủ {website_name} đã được khởi động thành công!</h2>
                <p>Chào Admin,</p>
                <p>Chúng tôi xin thông báo rằng máy chủ <strong>{website_name}</strong> đã được khởi động vào lúc <strong>{timestamp}</strong>. Các dịch vụ của chúng tôi hiện đã sẵn sàng và bạn có thể bắt đầu sử dụng ngay lập tức.</p>
                <p>Cảm ơn bạn đã tin tưởng và sử dụng dịch vụ của chúng tôi!</p>
                <br>
                <p style="font-size: 12px; color: #777;">Đây là email tự động, vui lòng không phản hồi trực tiếp email này.</p>
            </body>
        </html>
        """
        # Gửi email với nội dung và chữ ký
        success_send_email = self.send_email_async(to_email=to_email, subject=subject_mail, 
                                                    body=body_startup_notification, signature=self.signature_email)
        
        return success_send_email

    
    def send_mail_on_shutdown(self, to_email, website_name, timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")):
        """
        Gửi email thông báo khi ứng dụng FastAPI đã dừng.
        """
        subject_mail = f"Thông báo: Máy chủ {website_name} đã dừng hoạt động"
        body_shutdown_notification = f"""
        <html>
            <body style="font-family: Arial, sans-serif; color: #333;">
                <h2 style="color: #007BFF;">Máy chủ {website_name} đã dừng hoạt động!</h2>
                <p>Chào Admin,</p>
                <p>Chúng tôi xin thông báo rằng máy chủ <strong>{website_name}</strong> đã dừng hoạt động vào lúc <strong>{timestamp}</strong>. Các dịch vụ hiện không còn khả dụng cho đến khi ứng dụng được khởi động lại.</p>
                <p>Chúng tôi xin lỗi về sự bất tiện này và sẽ cố gắng khôi phục dịch vụ nhanh nhất có thể.</p>
                <br>
                <p style="font-size: 12px; color: #777;">Đây là email tự động, vui lòng không phản hồi trực tiếp email này.</p>
            </body>
        </html>
        """
        # Gửi email với nội dung và chữ ký
        success_send_email = self.send_email_async(to_email=to_email, subject=subject_mail, 
                                                    body=body_shutdown_notification, signature=self.signature_email)
        
        return success_send_email


# Ví dụ sử dụng
if __name__ == "__main__":


    email = InternalEmailSender()
    email.send_email_for_new_account(
                    to_email= "nguyenducquan2001@gmail.com",
                    name= "Quân",
                    website_name= "Hệ thống TEST"
                )