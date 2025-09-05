from typing import List
from loguru import logger

from app.domains.s3_client import S3Client
from app.config import settings
from app.core.models import (
    FileInfo, FileListResponse, DeleteResponse, 
    FileUpdateRequest
)
from app.core.exceptions import FileNotFoundError, S3OperationError



class FileService:
    def __init__(self, s3_client: S3Client):
        self.s3_client = s3_client

    async def get_file_info(self, file_key: str) -> FileInfo:
        """获取文件信息"""
        try:
            info = await self.s3_client.get_file_info(file_key)
            
            # 从元数据中获取原始文件名
            original_name = info.get('metadata', {}).get('original_name')
            if not original_name:
                # 从文件键中提取文件名
                original_name = file_key.split('/')[-1]
                if '_' in original_name:
                    original_name = '_'.join(original_name.split('_')[1:])
            
            return FileInfo(
                key=file_key,
                name=original_name,
                size=info['size'],
                content_type=info['content_type'],
                etag=info['etag'],
                last_modified=info['last_modified'],
                url=f"{settings.s3_endpoint_url}/{settings.s3_bucket_name}/{file_key}"
            )
            
        except FileNotFoundError:
            logger.warning(f"File not found: {file_key}")
            raise
        except Exception as e:
            logger.error(f"Failed to get file info for {file_key}: {e}")
            raise S3OperationError(f"Failed to get file info: {e}")

    async def list_files(self, prefix: str = "", max_keys: int = 100, 
                        marker: str = None, user_id: str = None) -> FileListResponse:
        """列出文件"""
        try:
            # 如果指定了用户ID，则添加到前缀中
            if user_id:
                if prefix:
                    search_prefix = f"users/{user_id}/{prefix}"
                else:
                    search_prefix = f"users/{user_id}/"
            else:
                search_prefix = prefix
            
            result = await self.s3_client.list_files(
                prefix=search_prefix,
                max_keys=max_keys,
                marker=marker
            )
            
            files = []
            for file_data in result['files']:
                # 从元数据获取原始文件名（需要额外请求）
                try:
                    file_info = await self.get_file_info(file_data['key'])
                    files.append(file_info)
                except Exception as e:
                    logger.warning(f"Failed to get detailed info for {file_data['key']}: {e}")
                    # 使用基本信息
                    name = file_data['key'].split('/')[-1]
                    if '_' in name:
                        name = '_'.join(name.split('_')[1:])
                    
                    files.append(FileInfo(
                        key=file_data['key'],
                        name=name,
                        size=file_data['size'],
                        content_type="application/octet-stream",
                        etag=file_data['etag'],
                        last_modified=file_data['last_modified']
                    ))
            
            return FileListResponse(
                files=files,
                total_count=result['total_count'],
                has_more=result['is_truncated'],
                next_marker=result['next_marker']
            )
            
        except Exception as e:
            logger.error(f"Failed to list files: {e}")
            raise S3OperationError(f"Failed to list files: {e}")

    async def delete_file(self, file_key: str) -> DeleteResponse:
        """删除单个文件"""
        try:
            # 检查文件是否存在
            await self.get_file_info(file_key)
            
            # 删除文件
            success = await self.s3_client.delete_file(file_key)
            
            if success:
                logger.info(f"Successfully deleted file: {file_key}")
                return DeleteResponse(
                    success=True,
                    message="File deleted successfully",
                    deleted_keys=[file_key]
                )
            else:
                return DeleteResponse(
                    success=False,
                    message="Failed to delete file",
                    deleted_keys=[]
                )
                
        except FileNotFoundError:
            logger.warning(f"Attempted to delete non-existent file: {file_key}")
            return DeleteResponse(
                success=False,
                message="File not found",
                deleted_keys=[]
            )
        except Exception as e:
            logger.error(f"Failed to delete file {file_key}: {e}")
            raise S3OperationError(f"Failed to delete file: {e}")

    async def delete_files(self, file_keys: List[str]) -> DeleteResponse:
        """批量删除文件"""
        try:
            if not file_keys:
                return DeleteResponse(
                    success=True,
                    message="No files to delete",
                    deleted_keys=[]
                )
            
            # 批量删除
            result = await self.s3_client.delete_files(file_keys)
            
            if result['errors']:
                error_keys = [error['Key'] for error in result['errors']]
                logger.warning(f"Failed to delete some files: {error_keys}")
            
            logger.info(f"Successfully deleted {len(result['deleted'])} files")
            
            return DeleteResponse(
                success=result['success'],
                message=f"Deleted {len(result['deleted'])} of {len(file_keys)} files",
                deleted_keys=result['deleted']
            )
            
        except Exception as e:
            logger.error(f"Failed to delete files: {e}")
            raise S3OperationError(f"Failed to delete files: {e}")

    async def copy_file(self, source_key: str, dest_key: str, 
                       new_name: str = None) -> FileInfo:
        """复制文件"""
        try:
            # 检查源文件是否存在
            source_info = await self.get_file_info(source_key)
            
            # 准备新的元数据
            metadata = {
                'original_name': new_name or source_info.name,
                'copied_from': source_key,
                'file_size': str(source_info.size)
            }
            
            # 复制文件
            success = await self.s3_client.copy_file(
                source_key=source_key,
                dest_key=dest_key,
                metadata=metadata
            )
            
            if success:
                # 返回新文件信息
                return await self.get_file_info(dest_key)
            else:
                raise S3OperationError("Copy operation failed")
                
        except FileNotFoundError:
            logger.warning(f"Source file not found: {source_key}")
            raise
        except Exception as e:
            logger.error(f"Failed to copy file {source_key} to {dest_key}: {e}")
            raise S3OperationError(f"Failed to copy file: {e}")

    async def move_file(self, source_key: str, dest_key: str, 
                       new_name: str = None) -> FileInfo:
        """移动文件（复制后删除原文件）"""
        try:
            # 复制文件
            new_file = await self.copy_file(source_key, dest_key, new_name)
            
            # 删除原文件
            await self.delete_file(source_key)
            
            logger.info(f"Successfully moved file from {source_key} to {dest_key}")
            return new_file
            
        except Exception as e:
            logger.error(f"Failed to move file {source_key} to {dest_key}: {e}")
            raise S3OperationError(f"Failed to move file: {e}")

    async def rename_file(self, file_key: str, new_name: str) -> FileInfo:
        """重命名文件"""
        try:
            # 获取当前文件信息
            await self.get_file_info(file_key)
            
            # 生成新的文件键（保持同一目录）
            path_parts = file_key.split('/')
            if len(path_parts) > 1:
                # 保持目录结构，只改变文件名
                directory = '/'.join(path_parts[:-1])
                # 生成新的唯一文件名
                import uuid
                file_id = str(uuid.uuid4())
                new_key = f"{directory}/{file_id}_{new_name}"
            else:
                import uuid
                file_id = str(uuid.uuid4())
                new_key = f"{file_id}_{new_name}"
            
            # 复制到新位置并删除原文件
            return await self.move_file(file_key, new_key, new_name)
            
        except Exception as e:
            logger.error(f"Failed to rename file {file_key}: {e}")
            raise S3OperationError(f"Failed to rename file: {e}")

    async def update_file_metadata(self, file_key: str, 
                                 update_data: FileUpdateRequest) -> FileInfo:
        """更新文件元数据"""
        try:
            # 获取当前文件信息
            current_info = await self.get_file_info(file_key)
            
            # 如果需要重命名
            if update_data.new_name and update_data.new_name != current_info.name:
                return await self.rename_file(file_key, update_data.new_name)
            
            # 如果只是更新content_type，需要复制文件
            if update_data.new_content_type and update_data.new_content_type != current_info.content_type:
                # S3不支持直接修改content_type，需要复制文件
                _ = file_key  # 使用相同的键
                temp_key = f"{file_key}_temp"
                
                # 先复制到临时位置
                await self.copy_file(file_key, temp_key)
                # 删除原文件
                await self.delete_file(file_key)
                # 再复制回原位置
                await self.copy_file(temp_key, file_key)
                # 删除临时文件
                await self.delete_file(temp_key)
            
            # 返回更新后的文件信息
            return await self.get_file_info(file_key)
            
        except Exception as e:
            logger.error(f"Failed to update file metadata for {file_key}: {e}")
            raise S3OperationError(f"Failed to update file metadata: {e}")

    async def get_download_url(self, file_key: str, expiration: int = 3600) -> str:
        """生成下载URL"""
        try:
            # 检查文件是否存在
            await self.get_file_info(file_key)
            
            # 生成预签名URL
            url = await self.s3_client.generate_presigned_url(file_key, expiration)
            
            logger.info(f"Generated download URL for {file_key}")
            return url
            
        except FileNotFoundError:
            logger.warning(f"File not found for download URL: {file_key}")
            raise
        except Exception as e:
            logger.error(f"Failed to generate download URL for {file_key}: {e}")
            raise S3OperationError(f"Failed to generate download URL: {e}")

    async def search_files(self, query: str, user_id: str = None, 
                          max_results: int = 50) -> FileListResponse:
        """搜索文件"""
        try:
            # 获取文件列表
            all_files = await self.list_files(user_id=user_id, max_keys=1000)
            
            # 过滤匹配的文件
            matching_files = []
            query_lower = query.lower()
            
            for file_info in all_files.files:
                if (query_lower in file_info.name.lower() or 
                    query_lower in file_info.key.lower()):
                    matching_files.append(file_info)
                    
                    if len(matching_files) >= max_results:
                        break
            
            return FileListResponse(
                files=matching_files,
                total_count=len(matching_files),
                has_more=False,
                next_marker=None
            )
            
        except Exception as e:
            logger.error(f"Failed to search files with query '{query}': {e}")
            raise S3OperationError(f"Failed to search files: {e}")