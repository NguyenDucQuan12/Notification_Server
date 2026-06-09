import requests

BASE_URL = "http://127.0.0.1:8000/file"

def upload_file(file_path):
    """
    Upload file lên server
    """
    # Địa chỉ api lấy token
    url_get_token = "http://127.0.0.1:8000/auth/login"

    data = {
        "username": "nguyenducquan2001@gmail.com",
        "password": "1"
    }

    # Gọi api lấy token, tham số được truyền qua body với đối tượng là data
    response = requests.post(
        url= url_get_token,
        data= data
    )
    my_token = response.json().get("Access_token")

    print(my_token)

    # Thêm headers( token xác thực)
    headers = {
        "Authorization": f"Bearer {my_token}"
    }

    url = f"{BASE_URL}/upload/"
    with open(file_path, "rb") as file:

        files = {
            'file': (file_path, file, 'multipart/form-data')
        }

        response = requests.post(
            url, 
            files=files,
            headers= headers
        )

    print("Upload:", response.status_code, response.json())

def list_files():
    """
    Lấy danh sách file trên server
    """
    url = f"{BASE_URL}/list_file_in_folder/"
    response = requests.get(url)
    print("List files:", response.status_code, response.json())

def download_file(file_name, save_path):
    """
    Tải file từ server về local
    """
    url = f"{BASE_URL}/download/{file_name}"

    response = requests.get(url)

    if response.status_code == 200:
        with open(save_path, "wb") as f:
            f.write(response.content)
        print(f"File {file_name} đã được tải về {save_path}")

    else:
        print("Download:", response.status_code, response.text)

def delete_file(file_name):
    """
    Xóa file trên server
    """
    # Địa chỉ api lấy token
    url_get_token = "http://127.0.0.1:8000/auth/login"

    data = {
        "username": "nguyenducquan2001@gmail.com",
        "password": "1"
    }

    # Gọi api lấy token, tham số được truyền qua body với đối tượng là data
    response = requests.post(
        url= url_get_token,
        data= data
    )
    my_token = response.json().get("Access_token")

    # Thêm headers( token xác thực)
    headers = {
        "Authorization": f"Bearer {my_token}"
    }

    url = f"{BASE_URL}/delete/{file_name}"
    response = requests.delete(
        url,
        headers = headers
        )

    print("Delete:", response.status_code, response.json())

def rename_file(old_name, new_name):
    """
    Đổi tên file trên server
    """
    url = f"{BASE_URL}/rename_file/"
    params = {"old_name": old_name, "new_name": new_name}
    response = requests.put(url, params=params)
    print("Rename:", response.status_code, response.json())

def file_info(file_name):
    """
    Lấy thông tin chi tiết file trên server
    """
    url = f"{BASE_URL}/info/{file_name}"
    response = requests.get(url)
    print("File info:", response.status_code, response.json())

if __name__ == "__main__":


    # Ví dụ sử dụng
    # upload_file("C:\\Users\\Server_Quan_IT\\Pictures\\Screenshots\\Screenshot 2024-09-20 093219.png")
    # list_files()
    # download_file("Nguyễn Đức Quân\\Screenshot 2024-09-20 093219.png", "downloaded_test.png")
    # file_info("Nguyễn Đức Quân\\Screenshot 2024-09-20 093219.png")
    # rename_file("test.txt", "test_renamed.txt")
    delete_file("Guest/Screenshot 2024-09-20 093219.png")