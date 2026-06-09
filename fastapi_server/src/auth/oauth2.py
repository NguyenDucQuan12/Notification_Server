from sqlalchemy.orm.session import Session
from fastapi.security import OAuth2PasswordBearer
from typing import Optional
from fastapi import Depends, HTTPException, status, Request
from datetime import datetime, timedelta
from jose import jwt # pip install python-jose
from jose.exceptions import JWTError
from db.database import get_db
from db import query_users
from dotenv import load_dotenv
import os

load_dotenv()  # Tự động tìm và nạp file .env ở thư mục hiện tại
 

# Khóa bí mật, nên tạo nó ngẫu nhiên bằng cách sau
# mở terminal và chạy lệnh: openssl rand -hex 32
# Khóa này chỉ dành cho việc phát triển API, không ai khác có thể sử dụng
# Chỉ những bên có SECRET_KEY mới có thể xác thực và giải mã token.
SECRET_KEY = os.getenv("SECRET_KEY", "77407c7339a6c00544e51af1101c4abb4aea2a31157ca5f7dfd87da02a628107") # Nếu ko tìm thấy giá trị trong tệp .env thì lấy giá trị mặc định này
ALGORITHM =  os.getenv("ALGORITHM", "HS256")  # Nếu ko tìm thấy thuật toán mã hóa trong .env thì mặc định sử dụng kiểu mã hóa HS256
ACCESS_TOKEN_EXPIRE_MINUTES = int(os.getenv("ACCESS_TOKEN_EXPIRE_MINUTES", "15")) # Nếu ko có thời gian hết hạn thì mặc định để 15 phút tồn tại cho token

# Chỉ định nơi lấy token bằng hàm login
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="quan/api/auth/login")        # Địa chỉ này phải đúng với đường dẫn của hàm login tạo token ở router auth, nếu không sẽ bị lỗi: 422 Unprocessable Entity {"detail": [{"loc": ["body", "username"], "msg": "field required", "type": "value_error.missing"}, {"loc": ["body", "password"], "msg": "field required", "type": "value_error.missing"}]}

# Chỉ định lấy token bằng hàm tự tạo
async def get_optional_token(request: Request):
    """
    Lấy token người dùng trong chuỗi truyền vào header
    """
    # Kiểm tra trong request gửi lên có trường headers và có trường "Authorization" không
    auth: str = request.headers.get("Authorization")
    # Nếu tồn tại thì bắt đầu lấy token từ đoạn mã phía sau chữ: Bearer 
    if auth and auth.lower().startswith("bearer "):
        # ví dụ: headers = {
        #            "Authorization": "Bearer token_từ_đây"
        #        }
        return auth[7:]
    return None
 
def create_access_token(data: dict, expires_delta: Optional[timedelta] = None):
    """
    Tạo token với tiêu chuẩn JWT (RFC 7591)  
    - **data: dict**: Là dữ liệu mà bạn muốn mã hóa và lưu trữ trong JWT. 
    Nó thường chứa thông tin về người dùng như `user_id`, `username`, hoặc bất kỳ dữ liệu nào khác mà bạn muốn bao gồm trong token.  
    - **expires_delta**: Thời gian hết hạn của token, mặc định là 15 phút
    """
    # Tạo một bản sao data để thao tác, ko ảnh hưởng đến data gốc
    to_encode = data.copy()

    # Thêm thời gian tồn tại cho token, nếu không cung cấp thì mặc định nó sẽ là 15 phút
    if expires_delta:
        expire = datetime.now() + expires_delta
    else:
        expire = datetime.now() + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)

    # Chuyển datetime thành chuỗi ISO 8601
    to_encode.update({"exp": expire})

    # Tạo token với khóa bí mật và phương thức tạo
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)

    return encoded_jwt

# Khi api yêu cầu token, nếu người dùng ko truyền token thì vẫn trả về None thay vì đưa ra ngoại lệ luôn và ko thực thi hàm nữa
async def get_info_user_via_token(token: Optional[str] = Depends(get_optional_token), db: Session = Depends(get_db)):

    """
    Lấy thông tin người dùng hiện tại dựa vào `token`  
    - `payload = jwt.decode(token, SECRET_KEY, algorithms= [ALGORITHM])` sẽ giải mã token dựa vào khóa bí mật và thuật toán đã sử dụng
    """
    if not token:
        return None  # Không truyền token thì trả về None

    credentials_exception = HTTPException(
        status_code= status.HTTP_401_UNAUTHORIZED,
        detail= {
            "Message": "Không thể xác thực token người dùng"
        },
        headers= {"WWW-Authenticate": "Bearer"}
    )
    try:
        # ví dụ ta có token: eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJlbWFpbCI6InR2Y19hZG1faXRAdGVydW1vLmNvLmpwIiwiZXhwIjoxNzI5ODcxMjQ2fQ.G-m2PjheT-zIQ7R9TkD9LWngHbZSeKF1LK8obtmE93k
        # Các giá trị ở giữa 2 dấu chấm sẽ là payload, từ đó ta có thể giải mã được giá trị ta đính kèm vào đó.
        # ta thu được email và thời gian token hết hạn, vì vậy không được để lộ token, vì khi đó người khác có thể giải mã và thu được thông tin từ token
        # Giải mã token dựa vào khóa bí mật và phương thức tạo
        payload = jwt.decode(token, SECRET_KEY, algorithms= [ALGORITHM])

        # Trích xuất thông tin người dùng từ payload
        tenant_id: str = payload.get("tenant_id")
        user_id: str = payload.get("user_id")
        user_name: str = payload.get("user_name")
        email: str = payload.get("email")
        role_id: str = payload.get("role_id")
        permission = payload.get("permissions")
        is_superuser: bool = payload.get("is_superuser")

        # Kiểm tra các thông tin có tồn tại trong payload không
        if not user_id or not email:
            raise credentials_exception
        
        # Kiểm tra xem người dùng có tồn tại trong cơ sở dữ liệu không
        user = await query_users.get_user_by_user_id(db= db, tenant_id= tenant_id, user_id= user_id)

        # Nếu không tồn tại người dùng hoặc tài khoản người dùng chưa được kích hoạt thì trả về ngoại lệ
        if not user["success"] or user["data"]["is_active"] is None:
            raise credentials_exception
        
        # Trả về thông tin người dùng đã giải mã từ token
        return {
            "tenant_id": tenant_id,
            "user_id": user_id,
            "user_name": user_name,
            "email": email,
            "role_id": role_id,
            "permission": permission,  # Lấy thông tin permission từ payload
            "is_superuser": is_superuser
        }

    except JWTError:
        raise credentials_exception
    

# Khi sử dụng hàm này lấy token thì bất cứ api nào yêu cầu xác thực, nếu người dùng ko truyền token vào thì trả về lỗi: 401 {'detail': 'Not authenticated'}
async def required_token_user(token: str = Depends(oauth2_scheme), db: Session = Depends(get_db)):

    """
    Lấy thông tin người dùng hiện tại dựa vào `token`  
    - `payload = jwt.decode(token, SECRET_KEY, algorithms= [ALGORITHM])` sẽ giải mã token dựa vào khóa bí mật và thuật toán đã sử dụng
    """
    credentials_exception = HTTPException(
        status_code= status.HTTP_401_UNAUTHORIZED,
        detail= {
            "Message": "Không thể xác thực token người dùng"
        },
        headers= {"WWW-Authenticate": "Bearer"}
    )
    try:
        # ví dụ ta có token: eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJlbWFpbCI6InR2Y19hZG1faXRAdGVydW1vLmNvLmpwIiwiZXhwIjoxNzI5ODcxMjQ2fQ.G-m2PjheT-zIQ7R9TkD9LWngHbZSeKF1LK8obtmE93k
        # Các giá trị ở giữa 2 dấu chấm sẽ là payload, từ đó ta có thể giải mã được giá trị ta đính kèm vào đó.
        # ta thu được email và thời gian token hết hạn, vì vậy không được để lộ token, vì khi đó người khác có thể giải mã và thu được thông tin từ token
        # Giải mã token dựa vào khóa bí mật và phương thức tạo
        payload = jwt.decode(token, SECRET_KEY, algorithms= [ALGORITHM])

        # Trích xuất thông tin người dùng từ payload
        tenant_id: str = payload.get("tenant_id")
        user_id: str = payload.get("user_id")
        user_name: str = payload.get("user_name")
        email: str = payload.get("email")
        role_id: str = payload.get("role_id")
        permission = payload.get("permissions")
        is_superuser: bool = payload.get("is_superuser")
        
        # Kiểm tra các thông tin có tồn tại trong payload không
        if not user_id or not email:
            raise credentials_exception
        
        # Kiểm tra xem người dùng có tồn tại trong cơ sở dữ liệu không
        user =await query_users.get_user_by_user_id(db= db, tenant_id= tenant_id, user_id= user_id)

        # Nếu không tồn tại người dùng hoặc tài khoản người dùng chưa được kích hoạt thì trả về ngoại lệ
        if not user["success"] or user["data"]["is_active"] is None:
            raise credentials_exception
        
        # Trả về thông tin người dùng đã giải mã từ token
        return {
            "tenant_id": tenant_id,
            "user_id": user_id,
            "user_name": user_name,
            "email": email,
            "role_id": role_id,
            "permissions": permission,  # Lấy thông tin permissiona từ payload
            "is_superuser": is_superuser
        }

    except JWTError:
        raise credentials_exception