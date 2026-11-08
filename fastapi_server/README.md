D:\Project\Backend_System\
│
├── fastapi_server\
│   ├── main.py
│   ├── src
|   │   ├── api\
|   │   ├── auth\
|   │   ├── controllers\
|   │   ├── database\
|   │   ├── models\
|   │   ├── services\
|   │   └──  requirements.txt
│   ├── Dockerfile
│   └── .dockerignore
│
├── noti_server\
│   ├── main.py
│   ├── src
|   │   ├── api\
|   │   ├── controllers\
|   │   ├── services\
|   │   └── utils\
│   ├── requirements.txt
│   ├── Dockerfile
│   └── .dockerignore
│
├── redis_compose.yml
├── .env
├── env\
│   ├── api.env
│   ├── noti.env
│   ├── api.env.example
│   └── noti.env.example
│
├── data\
│   ├── uploads\
│   ├── api-logs\
│   └── noti-logs\
│
├── compose.yaml
└── .gitignore