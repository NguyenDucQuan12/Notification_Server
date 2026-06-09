import requests

# Địa chỉ base của API FastAPI
BASE_URL = "http://127.0.0.1:8000/user_login"

# Hàm lấy token để xác thực các API cần token
def get_my_token():
    # Địa chỉ api lấy token
    url_get_token = "http://127.0.0.1:8000/auth/login"

    data = {
        "username": "nguyenducquan2001@gmail.com",  # Thay bằng thông tin người dùng thật
        "password": "1"  # Thay bằng mật khẩu thật
    }

    # Gọi api lấy token, tham số được truyền qua body với đối tượng là data
    response = requests.post(url=url_get_token, data=data)
    
    # Lấy token từ response
    my_token = response.json().get("Access_token")

    return my_token


# Hàm gọi API lấy danh sách người dùng
def get_list_users():
    url = f"{BASE_URL}/list_users"
    # Lấy token cho yêu cầu này
    token = get_my_token()
    headers = {"Authorization": f"Bearer {token}"}
    
    # Gửi yêu cầu GET đến API
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        print("Danh sách người dùng:", response.json())
    else:
        print("Lỗi khi lấy danh sách người dùng:", response.status_code)

# Hàm gọi API tạo người dùng mới
def create_user(user_data):
    url = f"{BASE_URL}/new_user"
    # Lấy token cho yêu cầu này
    token = get_my_token()
    headers = {"Authorization": f"Bearer {token}"}
    
    # Gửi yêu cầu POST để tạo người dùng mới
    response = requests.post(url, json=user_data, headers=headers)
    if response.status_code == 200:
        print("Người dùng mới đã được tạo:", response.json())
    else:
        print("Lỗi khi tạo người dùng:", response.status_code)

# Hàm gọi API kích hoạt hoặc hủy kích hoạt người dùng
def activate_user(email_user, activate):
    url = f"{BASE_URL}/activate_user/{email_user}?activate={activate}"
    # Lấy token cho yêu cầu này
    token = get_my_token()
    headers = {"Authorization": f"Bearer {token}"}
    
    # Gửi yêu cầu PUT để kích hoạt hoặc hủy kích hoạt người dùng
    response = requests.put(url, headers=headers)
    if response.status_code == 200:
        print(f"Người dùng {email_user} đã được kích hoạt:", response.json())
    else:
        print(f"Lỗi khi kích hoạt người dùng {email_user}:", response.status_code)

# Hàm gọi API thay đổi quyền hạn người dùng
def change_privilege_user(email_user, privilege):
    url = f"{BASE_URL}/change_privilege_user/{email_user}?privilege={privilege}"
    # Lấy token cho yêu cầu này
    token = get_my_token()
    headers = {"Authorization": f"Bearer {token}"}
    
    # Gửi yêu cầu PUT để thay đổi quyền hạn người dùng
    response = requests.put(url, headers=headers)
    if response.status_code == 200:
        print(f"Quyền hạn của người dùng {email_user} đã được thay đổi:", response.json())
    else:
        print(f"Lỗi khi thay đổi quyền hạn của {email_user}:", response.status_code)

# Hàm gọi API xóa người dùng
def delete_user(email_user):
    url = f"{BASE_URL}/delete_user/{email_user}"
    # Lấy token cho yêu cầu này
    token = get_my_token()
    headers = {"Authorization": f"Bearer {token}"}
    
    # Gửi yêu cầu DELETE để xóa người dùng
    response = requests.delete(url, headers=headers)
    if response.status_code == 200:
        print(f"Người dùng {email_user} đã được xóa:", response.json())
    else:
        print(f"Lỗi khi xóa người dùng {email_user}:", response.status_code)

# Ví dụ sử dụng các hàm
if __name__ == "__main__":
    # Lấy danh sách người dùng
    # get_list_users()

    # # Tạo người dùng mới (thay dữ liệu thực tế vào)
    # new_user_data = {
    #     "username": "newuser",
    #     "email": "newuser@example.com",
    #     "password": "password123"
    # }
    # create_user(new_user_data)

    # # Kích hoạt người dùng (thay email thực tế vào)
    # activate_user("newuser@example.com", True)

    # # Thay đổi quyền hạn của người dùng (thay email và quyền hạn thực tế vào)
    # change_privilege_user("newuser@example.com", "admin")

    # Xóa người dùng (thay email thực tế vào)
    delete_user("tvc_adm_it@terumo.co.jp")
