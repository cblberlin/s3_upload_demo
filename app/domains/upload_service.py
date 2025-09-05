import uuid
import asyncio
from typing import List, Dict
from fastapi import UploadFile
from loguru import logger
import time

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
        if file_size > settings.max_file_size:
            raise FileSizeExceedError(
                f"File size {file_size} exceeds maximum allowed size {settings.max_file_size}"
            )
        
        if '.' in filename:
            ext = filename.rsplit('.', 1)[1].lower()
            if ext not in settings.allowed_extensions_list:
                raise FileTypeNotAllowedError(
                    f"File extension '{ext}' is not allowed. "
                    f"Allowed extensions: {', '.join(settings.allowed_extensions_list)}"
                )

    def _generate_file_key(self, filename: str, user_id: str = None) -> str:
        """生成文件键名"""
        file_id = str(uuid.uuid4())
        
        if user_id:
            base_path = f"users/{user_id}"
        else:
            base_path = "public"
        
        safe_filename = filename.replace(' ', '_')
        return f"{base_path}/{file_id}_{safe_filename}"

    async def _get_file_size(self, file: UploadFile) -> int:
        """获取文件大小而不读取全部内容"""
        current_pos = file.file.tell()
        file.file.seek(0, 2)
        file_size = file.file.tell()
        file.file.seek(current_pos)
        return file_size

    async def _upload_small_file(self, file: UploadFile, file_key: str, 
                               metadata: Dict[str, str]) -> Dict[str, str]:
        """小文件直接上传"""
        result = await self.s3_client.upload_file(
            file_obj=file.file,
            key=file_key,
            content_type=file.content_type,
            metadata=metadata
        )
        return result

    async def _upload_chunk_async(self, file_key: str, upload_id: str, 
                                part_number: int, chunk_data: bytes) -> Dict:
        """异步上传单个分块"""
        try:
            start_time = time.time()
            etag = await self.s3_client.upload_part(
                key=file_key,
                upload_id=upload_id,
                part_number=part_number,
                data=chunk_data
            )
            
            upload_time = time.time() - start_time
            chunk_size_mb = len(chunk_data) / (1024 * 1024)
            speed_mbps = chunk_size_mb / upload_time if upload_time > 0 else 0
            
            logger.info(f"Part {part_number}: {chunk_size_mb:.1f}MB uploaded in {upload_time:.2f}s ({speed_mbps:.1f}MB/s)")
            
            return {
                'ETag': etag,
                'PartNumber': part_number,
                'Size': len(chunk_data),
                'UploadTime': upload_time
            }
        except Exception as e:
            logger.error(f"Failed to upload part {part_number}: {e}")
            raise e

    async def _upload_large_file_concurrent(self, file: UploadFile, file_key: str, 
                                          metadata: Dict[str, str], file_size: int) -> Dict[str, str]:
        """高性能并发分块上传"""
        # 使用配置获取最优分块大小
        chunk_size = settings.get_optimal_chunk_size(file_size)
        total_chunks = (file_size + chunk_size - 1) // chunk_size
        
        logger.info(f"Starting concurrent multipart upload: {total_chunks} chunks of {chunk_size // (1024*1024)}MB each")
        
        # 创建分块上传
        upload_id = await self.s3_client.create_multipart_upload(
            key=file_key,
            content_type=file.content_type
        )
        
        try:
            start_time = time.time()
            
            # 预读所有分块到内存
            chunks = []
            for i in range(total_chunks):
                chunk_data = file.file.read(chunk_size)
                if not chunk_data:
                    break
                chunks.append((i + 1, chunk_data))
            
            # 使用配置计算最优并发数
            concurrency = settings.get_optimal_concurrency(len(chunks))
            logger.info(f"Using {concurrency} concurrent uploads for {len(chunks)} chunks")
            
            # 创建semaphore来限制并发数
            semaphore = asyncio.Semaphore(concurrency)
            
            async def upload_with_semaphore(part_number: int, chunk_data: bytes):
                async with semaphore:
                    return await self._upload_chunk_async(file_key, upload_id, part_number, chunk_data)
            
            # 并发上传所有分块
            tasks = [
                upload_with_semaphore(part_number, chunk_data) 
                for part_number, chunk_data in chunks
            ]
            
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # 检查是否有失败的上传
            parts = []
            failed_parts = []
            
            for i, result in enumerate(results):
                if isinstance(result, Exception):
                    failed_parts.append(i + 1)
                    logger.error(f"Part {i + 1} failed: {result}")
                else:
                    parts.append({
                        'ETag': result['ETag'],
                        'PartNumber': result['PartNumber']
                    })
            
            if failed_parts:
                raise Exception(f"Failed to upload parts: {failed_parts}")
            
            # 按part number排序
            parts.sort(key=lambda x: x['PartNumber'])
            
            # 完成分块上传
            result = await self.s3_client.complete_multipart_upload(
                key=file_key,
                upload_id=upload_id,
                parts=parts
            )
            
            # 计算总体性能
            total_time = time.time() - start_time
            total_size_mb = file_size / (1024 * 1024)
            overall_speed = total_size_mb / total_time if total_time > 0 else 0
            
            logger.info(f"Concurrent upload completed: {total_size_mb:.1f}MB in {total_time:.2f}s ({overall_speed:.1f}MB/s)")
            
            return {
                'key': file_key,
                'bucket': self.s3_client.bucket,
                'url': f"{settings.s3_endpoint_url}/{self.s3_client.bucket}/{file_key}"
            }
            
        except Exception as e:
            # 出错时中止上传
            logger.error(f"Concurrent upload failed, aborting: {e}")
            await self.s3_client.abort_multipart_upload(file_key, upload_id)
            raise e

    async def _upload_large_file_streaming(self, file: UploadFile, file_key: str, 
                                         metadata: Dict[str, str], file_size: int) -> Dict[str, str]:
        """流式分块上传（适用于超大文件）"""
        chunk_size = settings.get_optimal_chunk_size(file_size)
        
        # 创建分块上传
        upload_id = await self.s3_client.create_multipart_upload(
            key=file_key,
            content_type=file.content_type
        )
        
        parts = []
        part_number = 1
        
        try:
            start_time = time.time()
            
            while True:
                chunk = file.file.read(chunk_size)
                if not chunk:
                    break
                
                # 流式上传每个分块
                result = await self._upload_chunk_async(file_key, upload_id, part_number, chunk)
                
                parts.append({
                    'ETag': result['ETag'],
                    'PartNumber': part_number
                })
                
                part_number += 1
            
            # 完成分块上传
            result = await self.s3_client.complete_multipart_upload(
                key=file_key,
                upload_id=upload_id,
                parts=parts
            )
            
            total_time = time.time() - start_time
            total_size_mb = file_size / (1024 * 1024)
            overall_speed = total_size_mb / total_time if total_time > 0 else 0
            
            logger.info(f"Streaming upload completed: {total_size_mb:.1f}MB in {total_time:.2f}s ({overall_speed:.1f}MB/s)")
            
            return {
                'key': file_key,
                'bucket': self.s3_client.bucket,
                'url': f"{settings.s3_endpoint_url}/{self.s3_client.bucket}/{file_key}"
            }
            
        except Exception as e:
            await self.s3_client.abort_multipart_upload(file_key, upload_id)
            raise e

    async def upload_single_file(self, file: UploadFile, 
                               user_id: str = None) -> UploadResponse:
        """智能单文件上传 - 根据文件大小自动选择最优上传方式"""
        try:
            file_size = await self._get_file_size(file)
            self._validate_file(file.filename, file_size)
            
            file_key = self._generate_file_key(file.filename, user_id)
            file.file.seek(0)
            
            metadata = {
                'original_name': file.filename,
                'file_size': str(file_size)
            }
            if user_id:
                metadata['user_id'] = user_id
            
            # 使用配置的策略选择上传方式
            if not settings.should_use_multipart(file_size):
                # 小文件：标准上传
                logger.info(f"Using standard upload for small file: {file.filename} ({file_size} bytes)")
                metadata['upload_method'] = 'single'
                result = await self._upload_small_file(file, file_key, metadata)
                
            elif not settings.should_use_streaming(file_size):
                # 中等大小文件：并发分块上传
                logger.info(f"Using concurrent multipart upload: {file.filename} ({file_size} bytes)")
                metadata['upload_method'] = 'multipart_concurrent'
                result = await self._upload_large_file_concurrent(file, file_key, metadata, file_size)
                
            else:
                # 超大文件：流式分块上传
                logger.info(f"Using streaming multipart upload: {file.filename} ({file_size} bytes)")
                metadata['upload_method'] = 'multipart_streaming'
                result = await self._upload_large_file_streaming(file, file_key, metadata, file_size)
            
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
                    expiration=settings.upload_timeout
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
            
            # 重置文件指针
            file.file.seek(0)
            
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
            
            if settings.should_use_streaming(file_size):
                result = await self._upload_large_file_streaming(file, file_key, metadata, file_size)
            else:
                result = await self._upload_large_file_concurrent(file, file_key, metadata, file_size)
            
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
            chunk_size = settings.get_optimal_chunk_size(file_size)
            
        total_chunks = (file_size + chunk_size - 1) // chunk_size
        
        return {
            'total_chunks': total_chunks,
            'chunk_size': chunk_size,
            'last_chunk_size': file_size % chunk_size or chunk_size
        }