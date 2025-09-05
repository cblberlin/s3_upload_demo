from fastapi import Depends
from functools import lru_cache

from app.domains.s3_client import S3Client
from app.domains.upload_service import UploadService
from app.domains.file_service import FileService
from app.domains.download_service import DownloadService


@lru_cache()
def get_s3_client() -> S3Client:
    """获取S3客户端实例"""
    return S3Client()


def get_upload_service(s3_client: S3Client = Depends(get_s3_client)) -> UploadService:
    """获取上传服务实例"""
    return UploadService(s3_client)


def get_file_service(s3_client: S3Client = Depends(get_s3_client)) -> FileService:
    """获取文件服务实例"""
    return FileService(s3_client)


def get_download_service(
    s3_client: S3Client = Depends(get_s3_client),
    file_service: FileService = Depends(get_file_service)
) -> DownloadService:
    """获取下载服务实例"""
    return DownloadService(s3_client, file_service)