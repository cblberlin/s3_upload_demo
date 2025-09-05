import uuid
from typing import List, Dict
from fastapi import UploadFile
from loguru import logger

from app.domains.s3_client import S3Client
from app.config import settings
from app.core.models import (
    UploadResponse, ChunkUploadInit, ChunkUploadInitResponse, 
    ChunkUploadComplete
)
from app.core.exceptions import (
    FileSizeExceedError, FileTypeNotAllowedError, 
    UploadFailedError, ChunkUploadError
)

class UploadService:
    def __init__(self, s3_client: S3Client):
        self.s3_client = s3_client

    def _validate_file(self, filename: str, file_size: int) -> None:
        """验证文件"""
        # 检查文件大小
        if file_size > settings.max_file_size:
            raise FileSizeExceedError(
                f"File size {file_size} exceeds maximum allowed size {settings.max_file_size}"
            )
        
        # 检查文件扩展名
        if '.' in filename:
            ext = filename.rsplit('.', 1)[1].lower()
            if ext not in settings.allowed_extensions_list:
                raise FileTypeNotAllowedError(
                    f"File extension '{ext}' is not allowed. "
                    f"Allowed extensions: {', '.join(settings.allowed_extensions_list)}"
                )

    def _generate_file_key(self, filename: str, user_id: str = None) -> str:
        """生成文件键名"""
        # 生成唯一ID
        file_id = str(uuid.uuid4())
        
        # 构建文件路径
        if user_id:
            base_path = f"users/{user_id}"
        else:
            base_path = "public"
        
        # 保持原始文件名，但添加唯一前缀
        safe_filename = filename.replace(' ', '_')
        return f"{base_path}/{file_id}_{safe_filename}"

    async def upload_single_file(self, file: UploadFile, 
                               user_id: str = None) -> UploadResponse:
        """单文件上传"""
        try:
            # 读取文件内容获取大小
            content = await file.read()
            file_size = len(content)
            
            # 验证文件
            self._validate_file(file.filename, file_size)
            
            # 生成文件键
            file_key = self._generate_file_key(file.filename, user_id)
            
            # 重置文件指针
            await file.seek(0)
            
            # 准备元数据
            metadata = {
                'original_name': file.filename,
                'upload_method': 'single',
                'file_size': str(file_size)
            }
            if user_id:
                metadata['user_id'] = user_id
            
            # 上传到S3
            result = await self.s3_client.upload_file(
                file_obj=file.file,
                key=file_key,
                content_type=file.content_type,
                metadata=metadata
            )
            
            logger.info(f"Successfully uploaded file: {file_key}")
            
            return UploadResponse(
                success=True,
                message="File uploaded successfully",
                file_key=file_key,
                file_url=result['url']
            )
            
        except (FileSizeExceedError, FileTypeNotAllowedError) as e:
            logger.warning(f"File validation failed: {e}")
            return UploadResponse(success=False, message=str(e))
        except Exception as e:
            logger.error(f"Upload failed: {e}")
            raise UploadFailedError(f"Upload failed: {e}")

    async def upload_multiple_files(self, files: List[UploadFile], 
                                  user_id: str = None) -> List[UploadResponse]:
        """多文件上传"""
        results = []
        
        for file in files:
            try:
                result = await self.upload_single_file(file, user_id)
                results.append(result)
            except Exception as e:
                logger.error(f"Failed to upload {file.filename}: {e}")
                results.append(UploadResponse(
                    success=False,
                    message=f"Failed to upload {file.filename}: {str(e)}"
                ))
        
        return results

    async def init_chunk_upload(self, init_data: ChunkUploadInit, 
                              user_id: str = None) -> ChunkUploadInitResponse:
        """初始化分块上传"""
        try:
            # 验证文件
            self._validate_file(init_data.filename, init_data.file_size)
            
            # 生成文件键
            file_key = self._generate_file_key(init_data.filename, user_id)
            
            # 创建分块上传
            upload_id = await self.s3_client.create_multipart_upload(
                key=file_key,
                content_type=init_data.content_type
            )
            
            # 生成每个分块的上传URL
            chunk_urls = []
            for i in range(1, init_data.total_chunks + 1):
                url = await self.s3_client.generate_presigned_upload_url(
                    key=file_key,
                    upload_id=upload_id,
                    part_number=i,
                    expiration=3600  # 1小时有效期
                )
                chunk_urls.append({
                    'part_number': i,
                    'upload_url': url
                })
            
            logger.info(f"Initialized chunk upload: {file_key} with {init_data.total_chunks} chunks")
            
            return ChunkUploadInitResponse(
                upload_id=upload_id,
                file_key=file_key,
                chunk_urls=chunk_urls
            )
            
        except (FileSizeExceedError, FileTypeNotAllowedError) as e:
            logger.warning(f"Chunk upload validation failed: {e}")
            raise ChunkUploadError(str(e))
        except Exception as e:
            logger.error(f"Failed to initialize chunk upload: {e}")
            raise ChunkUploadError(f"Failed to initialize chunk upload: {e}")

    async def complete_chunk_upload(self, complete_data: ChunkUploadComplete, 
                                  user_id: str = None) -> UploadResponse:
        """完成分块上传"""
        try:
            # 完成多部分上传
            await self.s3_client.complete_multipart_upload(
                key=complete_data.file_key,
                upload_id=complete_data.upload_id,
                parts=complete_data.parts
            )
            
            logger.info(f"Completed chunk upload: {complete_data.file_key}")
            
            return UploadResponse(
                success=True,
                message="Chunk upload completed successfully",
                file_key=complete_data.file_key,
                file_url=f"{settings.s3_endpoint_url}/{settings.s3_bucket_name}/{complete_data.file_key}"
            )
            
        except Exception as e:
            logger.error(f"Failed to complete chunk upload: {e}")
            # 尝试中止上传
            await self.s3_client.abort_multipart_upload(
                complete_data.file_key, 
                complete_data.upload_id
            )
            raise ChunkUploadError(f"Failed to complete chunk upload: {e}")

    async def abort_chunk_upload(self, file_key: str, upload_id: str) -> bool:
        """中止分块上传"""
        try:
            result = await self.s3_client.abort_multipart_upload(file_key, upload_id)
            logger.info(f"Aborted chunk upload: {file_key}")
            return result
        except Exception as e:
            logger.error(f"Failed to abort chunk upload: {e}")
            return False

    async def upload_single_file_with_progress(self, file: UploadFile, 
                                             user_id: str = None) -> UploadResponse:
        """带进度的单文件上传（强制使用分块上传）"""
        try:
            # 获取文件大小
            file_size = await self._get_file_size(file)
            
            # 验证文件
            self._validate_file(file.filename, file_size)
            
            # 生成文件键
            file_key = self._generate_file_key(file.filename, user_id)
            
            # 准备元数据
            metadata = {
                'original_name': file.filename,
                'upload_method': 'multipart_progressive',
                'file_size': str(file_size)
            }
            if user_id:
                metadata['user_id'] = user_id
            
            # 强制使用分块上传以便显示进度
            logger.info(f"Using progressive multipart upload for file: {file.filename} ({file_size} bytes)")
            result = await self._upload_large_file_auto(file, file_key, metadata)
            
            logger.info(f"Successfully uploaded file with progress: {file_key}")
            
            return UploadResponse(
                success=True,
                message="File uploaded successfully with progress tracking",
                file_key=file_key,
                file_url=result['url']
            )
            
        except (FileSizeExceedError, FileTypeNotAllowedError) as e:
            logger.warning(f"File validation failed: {e}")
            return UploadResponse(success=False, message=str(e))
        except Exception as e:
            logger.error(f"Progressive upload failed: {e}")
            raise UploadFailedError(f"Progressive upload failed: {e}")

    def calculate_chunks(self, file_size: int, chunk_size: int = None) -> Dict[str, int]:
        """计算分块信息"""
        if chunk_size is None:
            chunk_size = settings.chunk_size
            
        total_chunks = (file_size + chunk_size - 1) // chunk_size
        
        return {
            'total_chunks': total_chunks,
            'chunk_size': chunk_size,
            'last_chunk_size': file_size % chunk_size or chunk_size
        }