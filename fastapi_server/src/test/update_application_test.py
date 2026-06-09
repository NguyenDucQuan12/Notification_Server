import requests

BASE_URL = "http://127.0.0.1:8000/update_application"

def get_my_token ():
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

    return my_token

def upload_update(app_name, version, platform, file_path, release_notes=None, token=None):
    """
    Upload gói cập nhật ứng dụng
    """
    url = f"{BASE_URL}/upload/{app_name}/{version}"
    headers = {}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    params = {
        "platform": platform,
        "release_notes": release_notes or ""
    }
    with open(file_path, "rb") as f:
        files = {"file": (file_path, f, "application/octet-stream")}
        response = requests.post(url, params=params, files=files, headers=headers)
    print("Upload:", response.status_code, response.json())

def check_update(app_name, current_version, platform):
    """
    Kiểm tra có bản cập nhật mới không
    """
    url = f"{BASE_URL}/check/{app_name}"
    params = {
        "current_version": current_version,
        "platform": platform
    }
    response = requests.get(url, params=params)
    print("Check update:", response.status_code, response.json())

def list_versions(app_name, platform=None):
    """
    Liệt kê các phiên bản đã upload
    """
    url = f"{BASE_URL}/versions/{app_name}"
    params = {}
    if platform:
        params["platform"] = platform
    response = requests.get(url, params=params)
    print("List versions:", response.status_code, response.json())

def download_update(app_name, platform, version, file_path, save_path):
    """
    Tải gói cập nhật về local
    """
    url = f"{BASE_URL}/download/{app_name}/{platform}/{version}/{file_path}"
    response = requests.get(url)
    if response.status_code == 200:
        with open(save_path, "wb") as f:
            f.write(response.content)
        print(f"Đã tải về {save_path}")
    else:
        print("Download:", response.status_code, response.text)

def delete_version(app_name, platform, version, token):
    """
    Xoá một phiên bản ứng dụng
    """
    url = f"{BASE_URL}/version/{app_name}/{platform}/{version}"
    headers = {"Authorization": f"Bearer {token}"}
    response = requests.delete(url, headers=headers)
    print("Delete version:", response.status_code, response.json())

if __name__ == "__main__":
    # Ví dụ sử dụng
    # Thay đổi các giá trị bên dưới cho phù hợp với hệ thống của bạn
    # token = get_my_token()

    # upload_update(
    #     app_name="test",
    #     version="1.0.0",
    #     platform="win",
    #     file_path="my_app_update.zip",
    #     release_notes="Cập nhật tính năng mới",
    #     token=token
    # )

    check_update("test", "0.0.1", "win")
    # list_versions("test", "win")
    # download_update("test", "win", "0.0.1", "font-awesome-6-pro-main.zip", "downloaded_update.zip")
    # delete_version("test", "win", "0.0.3", token)