from contextlib import asynccontextmanager
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from loguru import logger

from app.config import settings
from app.api.v1.router import api_router
from app.core.exceptions import (
    FileManagerException, FileNotFoundError, FileSizeExceedError,
    FileTypeNotAllowedError, UploadFailedError, S3OperationError,
    ChunkUploadError
)

# 生命周期事件处理器
@asynccontextmanager
async def lifespan(app: FastAPI):
    # 启动事件
    logger.info(f"Starting {settings.project_name}")
    logger.info(f"S3 Endpoint: {settings.s3_endpoint_url}")
    logger.info(f"S3 Bucket: {settings.s3_bucket_name}")
    yield
    # 关闭事件
    logger.info(f"Shutting down {settings.project_name}")


# 创建FastAPI应用
app = FastAPI(
    title=settings.project_name,
    description="S3/R2 文件管理系统 - 支持上传、下载、删除、修改等完整功能",
    version="1.0.0",
    openapi_url=f"{settings.api_v1_prefix}/openapi.json",
    lifespan=lifespan
)

# 配置CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # 生产环境中应该限制为特定域名
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# 全局异常处理器
@app.exception_handler(FileNotFoundError)
async def file_not_found_handler(request: Request, exc: FileNotFoundError):
    return JSONResponse(
        status_code=404,
        content={"error": "File not found", "detail": str(exc)}
    )


@app.exception_handler(FileSizeExceedError)
async def file_size_exceed_handler(request: Request, exc: FileSizeExceedError):
    return JSONResponse(
        status_code=400,
        content={"error": "File size exceeded", "detail": str(exc)}
    )


@app.exception_handler(FileTypeNotAllowedError)
async def file_type_not_allowed_handler(request: Request, exc: FileTypeNotAllowedError):
    return JSONResponse(
        status_code=400,
        content={"error": "File type not allowed", "detail": str(exc)}
    )


@app.exception_handler(UploadFailedError)
async def upload_failed_handler(request: Request, exc: UploadFailedError):
    return JSONResponse(
        status_code=500,
        content={"error": "Upload failed", "detail": str(exc)}
    )


@app.exception_handler(ChunkUploadError)
async def chunk_upload_error_handler(request: Request, exc: ChunkUploadError):
    return JSONResponse(
        status_code=400,
        content={"error": "Chunk upload error", "detail": str(exc)}
    )


@app.exception_handler(S3OperationError)
async def s3_operation_error_handler(request: Request, exc: S3OperationError):
    return JSONResponse(
        status_code=500,
        content={"error": "S3 operation failed", "detail": str(exc)}
    )


@app.exception_handler(FileManagerException)
async def file_manager_exception_handler(request: Request, exc: FileManagerException):
    return JSONResponse(
        status_code=500,
        content={"error": "File manager error", "detail": str(exc)}
    )


# 健康检查端点
@app.get("/health")
async def health_check():
    """健康检查"""
    return {
        "status": "healthy",
        "project": settings.project_name,
        "version": "1.0.0"
    }


# 根路径
@app.get("/")
async def root():
    """根路径信息"""
    return {
        "project": settings.project_name,
        "version": "1.0.0",
        "description": "S3/R2 文件管理系统",
        "docs_url": "/docs",
        "api_prefix": settings.api_v1_prefix
    }


# 包含API路由
app.include_router(api_router, prefix=settings.api_v1_prefix)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "app.main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info"
    )