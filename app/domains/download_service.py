from typing import Dict, Any
from fastapi.responses import StreamingResponse
from loguru import logger
import io

from app.domains.s3_client import S3Client
from app.domains.file_service import FileService
from app.core.exceptions import FileNotFoundError, S3OperationError



class DownloadService:
    def __init__(self, s3_client: S3Client, file_service: FileService):
        self.s3_client = s3_client
        self.file_service = file_service

    async def download_file_direct(self, file_key: str) -> bytes:
        """直接下载文件内容"""
        try:
            logger.info(f"Starting direct download for file: {file_key}")
            
            # 下载文件内容
            content = await self.s3_client.download_file(file_key)
            
            logger.info(f"Successfully downloaded file: {file_key}, size: {len(content)} bytes")
            return content
            
        except FileNotFoundError:
            logger.warning(f"File not found for download: {file_key}")
            raise
        except Exception as e:
            logger.error(f"Failed to download file {file_key}: {e}")
            raise S3OperationError(f"Download failed: {e}")

    async def download_file_stream(self, file_key: str) -> StreamingResponse:
        """流式下载文件"""
        try:
            logger.info(f"Starting stream download for file: {file_key}")
            
            # 获取文件信息
            file_info = await self.file_service.get_file_info(file_key)
            
            # 下载文件内容
            content = await self.s3_client.download_file(file_key)
            
            # 设置响应头
            headers = {
                'Content-Disposition': f'attachment; filename="{file_info.name}"',
                'Content-Length': str(file_info.size),
                'Content-Type': file_info.content_type or 'application/octet-stream'
            }
            
            logger.info(f"Successfully prepared stream for file: {file_key}")
            
            # 创建流式响应
            return StreamingResponse(
                io.BytesIO(content),
                media_type=file_info.content_type or 'application/octet-stream',
                headers=headers
            )
            
        except FileNotFoundError:
            logger.warning(f"File not found for stream download: {file_key}")
            raise
        except Exception as e:
            logger.error(f"Failed to stream download file {file_key}: {e}")
            raise S3OperationError(f"Stream download failed: {e}")

    async def get_download_url(self, file_key: str, expiration: int = 3600, 
                              disposition: str = "attachment") -> str:
        """获取预签名下载URL"""
        try:
            logger.info(f"Generating download URL for file: {file_key}")
            
            # 生成预签名URL
            url = await self.s3_client.generate_presigned_url(file_key, expiration)
            
            logger.info(f"Successfully generated download URL for file: {file_key}")
            return url
            
        except FileNotFoundError:
            logger.warning(f"File not found for URL generation: {file_key}")
            raise
        except Exception as e:
            logger.error(f"Failed to generate download URL for {file_key}: {e}")
            raise S3OperationError(f"URL generation failed: {e}")

    async def download_file_range(self, file_key: str, start: int, 
                                 end: int = None) -> Dict[str, Any]:
        """范围下载文件（支持断点续传）"""
        try:
            logger.info(f"Starting range download for file: {file_key}, range: {start}-{end}")
            
            # 获取文件信息
            file_info = await self.file_service.get_file_info(file_key)
            
            # 验证范围
            if start >= file_info.size:
                raise ValueError(f"Start position {start} exceeds file size {file_info.size}")
            
            if end is None:
                end = file_info.size - 1
            else:
                end = min(end, file_info.size - 1)
            
            # 下载整个文件（S3不直接支持范围下载，需要在应用层处理）
            content = await self.s3_client.download_file(file_key)
            
            # 提取指定范围的内容
            range_content = content[start:end + 1]
            
            logger.info(f"Successfully downloaded range for file: {file_key}, "
                       f"range: {start}-{end}, size: {len(range_content)} bytes")
            
            return {
                'content': range_content,
                'start': start,
                'end': end,
                'total_size': file_info.size,
                'content_type': file_info.content_type
            }
            
        except FileNotFoundError:
            logger.warning(f"File not found for range download: {file_key}")
            raise
        except Exception as e:
            logger.error(f"Failed to download range for file {file_key}: {e}")
            raise S3OperationError(f"Range download failed: {e}")

    async def preview_file(self, file_key: str, max_size: int = 1024 * 1024) -> Dict[str, Any]:
        """预览文件内容（适用于文本文件等）"""
        try:
            logger.info(f"Starting file preview for: {file_key}")
            
            # 获取文件信息
            file_info = await self.file_service.get_file_info(file_key)
            
            # 检查文件大小
            if file_info.size > max_size:
                # 只下载部分内容用于预览
                preview_data = await self.download_file_range(file_key, 0, max_size - 1)
                content = preview_data['content']
                is_truncated = True
            else:
                # 下载完整文件
                content = await self.s3_client.download_file(file_key)
                is_truncated = False
            
            # 尝试解码为文本（如果是文本文件）
            text_content = None
            is_text = False
            
            if file_info.content_type and ('text/' in file_info.content_type or 
                                         'application/json' in file_info.content_type or
                                         'application/xml' in file_info.content_type):
                try:
                    text_content = content.decode('utf-8')
                    is_text = True
                except UnicodeDecodeError:
                    # 尝试其他编码
                    for encoding in ['gbk', 'gb2312', 'latin-1']:
                        try:
                            text_content = content.decode(encoding)
                            is_text = True
                            break
                        except UnicodeDecodeError:
                            continue
            
            logger.info(f"Successfully generated preview for file: {file_key}")
            
            return {
                'file_info': file_info,
                'content': content if not is_text else None,
                'text_content': text_content,
                'is_text': is_text,
                'is_truncated': is_truncated,
                'preview_size': len(content)
            }
            
        except FileNotFoundError:
            logger.warning(f"File not found for preview: {file_key}")
            raise
        except Exception as e:
            logger.error(f"Failed to preview file {file_key}: {e}")
            raise S3OperationError(f"File preview failed: {e}")

    def _get_content_disposition(self, filename: str, disposition: str = "attachment") -> str:
        """生成Content-Disposition头"""
        # 处理文件名中的特殊字符
        safe_filename = filename.encode('ascii', 'ignore').decode('ascii')
        if not safe_filename:
            safe_filename = "download"
        
        return f'{disposition}; filename="{safe_filename}"; filename*=UTF-8\'\'{filename}'