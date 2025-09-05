from fastapi import APIRouter

from app.api.v1.endpoints import upload, files

api_router = APIRouter()

# 包含上传相关路由
api_router.include_router(
    upload.router,
    prefix="/upload",
    tags=["upload"]
)

# 包含文件管理相关路由
api_router.include_router(
    files.router,
    prefix="/files",
    tags=["files"]
)