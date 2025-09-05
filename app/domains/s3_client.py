import boto3
import asyncio
import concurrent.futures
from boto3.s3.transfer import TransferConfig
from botocore.exceptions import ClientError, NoCredentialsError
from typing import List, Dict, Any, BinaryIO
from loguru import logger
from app.config import settings
from app.core.exceptions import S3OperationError, FileNotFoundError


class S3Client:
    def __init__(self):
        try:
            self.client = boto3.client(
                's3',
                endpoint_url=settings.s3_endpoint_url,
                aws_access_key_id=settings.aws_access_key_id,
                aws_secret_access_key=settings.aws_secret_access_key,
                region_name=settings.aws_region
            )
            self.bucket = settings.s3_bucket_name
            
            # 配置传输参数
            self.transfer_config = TransferConfig(
                multipart_threshold=1024 * 25,  # 25MB
                max_concurrency=10,
                multipart_chunksize=1024 * 25,
                use_threads=True
            )
            
            # 创建线程池用于并发上传
            self.thread_pool = concurrent.futures.ThreadPoolExecutor(
                max_workers=settings.max_concurrent_uploads
            )
            
        except NoCredentialsError:
            logger.error("AWS credentials not found")
            raise S3OperationError("AWS credentials not configured")
        except Exception as e:
            logger.error(f"Failed to initialize S3 client: {e}")
            raise S3OperationError(f"S3 client initialization failed: {e}")

    def __del__(self):
        """清理线程池"""
        if hasattr(self, 'thread_pool'):
            self.thread_pool.shutdown(wait=False)

    async def upload_file(self, file_obj: BinaryIO, key: str, 
                         content_type: str = None, 
                         metadata: Dict[str, str] = None) -> Dict[str, Any]:
        """上传文件到S3"""
        try:
            extra_args = {}
            if content_type:
                extra_args['ContentType'] = content_type
            if metadata:
                extra_args['Metadata'] = metadata
            
            # 使用线程池执行同步的上传操作
            def _upload_sync():
                self.client.upload_fileobj(
                    file_obj, 
                    self.bucket, 
                    key, 
                    ExtraArgs=extra_args,
                    Config=self.transfer_config
                )
            
            # 在线程池中执行同步操作
            await asyncio.get_event_loop().run_in_executor(
                self.thread_pool, _upload_sync
            )
            
            return {
                'key': key,
                'bucket': self.bucket,
                'url': f"{settings.s3_endpoint_url}/{self.bucket}/{key}"
            }
            
        except ClientError as e:
            logger.error(f"Failed to upload file {key}: {e}")
            raise S3OperationError(f"Upload failed: {e}")

    async def download_file(self, key: str) -> bytes:
        """从S3下载文件"""
        try:
            def _download_sync():
                response = self.client.get_object(Bucket=self.bucket, Key=key)
                return response['Body'].read()
            
            return await asyncio.get_event_loop().run_in_executor(
                self.thread_pool, _download_sync
            )
        except ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchKey':
                raise FileNotFoundError(f"File {key} not found")
            logger.error(f"Failed to download file {key}: {e}")
            raise S3OperationError(f"Download failed: {e}")

    async def get_file_info(self, key: str) -> Dict[str, Any]:
        """获取文件信息"""
        try:
            def _get_info_sync():
                response = self.client.head_object(Bucket=self.bucket, Key=key)
                return {
                    'key': key,
                    'size': response['ContentLength'],
                    'content_type': response.get('ContentType', ''),
                    'etag': response['ETag'].strip('"'),
                    'last_modified': response['LastModified'],
                    'metadata': response.get('Metadata', {})
                }
            
            return await asyncio.get_event_loop().run_in_executor(
                self.thread_pool, _get_info_sync
            )
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                raise FileNotFoundError(f"File {key} not found")
            logger.error(f"Failed to get file info {key}: {e}")
            raise S3OperationError(f"Get file info failed: {e}")

    async def list_files(self, prefix: str = "", max_keys: int = 1000, 
                        marker: str = None) -> Dict[str, Any]:
        """列出文件"""
        try:
            def _list_sync():
                kwargs = {
                    'Bucket': self.bucket,
                    'MaxKeys': max_keys
                }
                if prefix:
                    kwargs['Prefix'] = prefix
                if marker:
                    kwargs['Marker'] = marker
                    
                response = self.client.list_objects_v2(**kwargs)
                
                files = []
                if 'Contents' in response:
                    for obj in response['Contents']:
                        files.append({
                            'key': obj['Key'],
                            'size': obj['Size'],
                            'etag': obj['ETag'].strip('"'),
                            'last_modified': obj['LastModified'],
                            'storage_class': obj.get('StorageClass', 'STANDARD')
                        })
                
                return {
                    'files': files,
                    'is_truncated': response.get('IsTruncated', False),
                    'next_marker': response.get('NextContinuationToken'),
                    'total_count': len(files)
                }
            
            return await asyncio.get_event_loop().run_in_executor(
                self.thread_pool, _list_sync
            )
            
        except ClientError as e:
            logger.error(f"Failed to list files: {e}")
            raise S3OperationError(f"List files failed: {e}")

    async def delete_file(self, key: str) -> bool:
        """删除文件"""
        try:
            def _delete_sync():
                self.client.delete_object(Bucket=self.bucket, Key=key)
                return True
            
            return await asyncio.get_event_loop().run_in_executor(
                self.thread_pool, _delete_sync
            )
        except ClientError as e:
            logger.error(f"Failed to delete file {key}: {e}")
            raise S3OperationError(f"Delete failed: {e}")

    async def delete_files(self, keys: List[str]) -> Dict[str, Any]:
        """批量删除文件"""
        try:
            def _delete_batch_sync():
                objects = [{'Key': key} for key in keys]
                response = self.client.delete_objects(
                    Bucket=self.bucket,
                    Delete={'Objects': objects}
                )
                
                deleted = [obj['Key'] for obj in response.get('Deleted', [])]
                errors = response.get('Errors', [])
                
                return {
                    'deleted': deleted,
                    'errors': errors,
                    'success': len(errors) == 0
                }
            
            return await asyncio.get_event_loop().run_in_executor(
                self.thread_pool, _delete_batch_sync
            )
            
        except ClientError as e:
            logger.error(f"Failed to delete files: {e}")
            raise S3OperationError(f"Batch delete failed: {e}")

    async def copy_file(self, source_key: str, dest_key: str, 
                       metadata: Dict[str, str] = None) -> bool:
        """复制文件"""
        try:
            def _copy_sync():
                copy_source = {'Bucket': self.bucket, 'Key': source_key}
                extra_args = {}
                
                if metadata:
                    extra_args['Metadata'] = metadata
                    extra_args['MetadataDirective'] = 'REPLACE'
                
                self.client.copy_object(
                    CopySource=copy_source,
                    Bucket=self.bucket,
                    Key=dest_key,
                    **extra_args
                )
                return True
            
            return await asyncio.get_event_loop().run_in_executor(
                self.thread_pool, _copy_sync
            )
            
        except ClientError as e:
            logger.error(f"Failed to copy file {source_key} to {dest_key}: {e}")
            raise S3OperationError(f"Copy failed: {e}")

    async def generate_presigned_url(self, key: str, expiration: int = 3600) -> str:
        """生成预签名URL"""
        try:
            def _generate_url_sync():
                return self.client.generate_presigned_url(
                    'get_object',
                    Params={'Bucket': self.bucket, 'Key': key},
                    ExpiresIn=expiration
                )
            
            return await asyncio.get_event_loop().run_in_executor(
                self.thread_pool, _generate_url_sync
            )
        except ClientError as e:
            logger.error(f"Failed to generate presigned URL for {key}: {e}")
            raise S3OperationError(f"Generate URL failed: {e}")

    # 分块上传相关方法
    async def create_multipart_upload(self, key: str, 
                                    content_type: str = None) -> str:
        """创建分块上传"""
        try:
            def _create_multipart_sync():
                kwargs = {'Bucket': self.bucket, 'Key': key}
                if content_type:
                    kwargs['ContentType'] = content_type
                    
                response = self.client.create_multipart_upload(**kwargs)
                return response['UploadId']
            
            return await asyncio.get_event_loop().run_in_executor(
                self.thread_pool, _create_multipart_sync
            )
            
        except ClientError as e:
            logger.error(f"Failed to create multipart upload for {key}: {e}")
            raise S3OperationError(f"Create multipart upload failed: {e}")

    async def generate_presigned_upload_url(self, key: str, upload_id: str, 
                                          part_number: int, 
                                          expiration: int = 3600) -> str:
        """生成分块上传的预签名URL"""
        try:
            def _generate_upload_url_sync():
                return self.client.generate_presigned_url(
                    'upload_part',
                    Params={
                        'Bucket': self.bucket,
                        'Key': key,
                        'UploadId': upload_id,
                        'PartNumber': part_number
                    },
                    ExpiresIn=expiration
                )
            
            return await asyncio.get_event_loop().run_in_executor(
                self.thread_pool, _generate_upload_url_sync
            )
        except ClientError as e:
            logger.error(f"Failed to generate upload URL: {e}")
            raise S3OperationError(f"Generate upload URL failed: {e}")

    async def complete_multipart_upload(self, key: str, upload_id: str, 
                                      parts: List[Dict[str, Any]]) -> Dict[str, Any]:
        """完成分块上传"""
        try:
            def _complete_multipart_sync():
                response = self.client.complete_multipart_upload(
                    Bucket=self.bucket,
                    Key=key,
                    UploadId=upload_id,
                    MultipartUpload={'Parts': parts}
                )
                
                return {
                    'key': key,
                    'etag': response['ETag'],
                    'location': response['Location']
                }
            
            return await asyncio.get_event_loop().run_in_executor(
                self.thread_pool, _complete_multipart_sync
            )
            
        except ClientError as e:
            logger.error(f"Failed to complete multipart upload for {key}: {e}")
            raise S3OperationError(f"Complete multipart upload failed: {e}")

    async def upload_part(self, key: str, upload_id: str, part_number: int, 
                         data: bytes) -> str:
        """上传单个分块 - 真正的异步版本"""
        try:
            def _upload_part_sync():
                response = self.client.upload_part(
                    Bucket=self.bucket,
                    Key=key,
                    UploadId=upload_id,
                    PartNumber=part_number,
                    Body=data
                )
                return response['ETag'].strip('"')
            
            # 关键：使用线程池执行同步操作，实现真正的并发
            return await asyncio.get_event_loop().run_in_executor(
                self.thread_pool, _upload_part_sync
            )
        except ClientError as e:
            logger.error(f"Failed to upload part {part_number} for {key}: {e}")
            raise S3OperationError(f"Upload part failed: {e}")

    async def abort_multipart_upload(self, key: str, upload_id: str) -> bool:
        """中止分块上传"""
        try:
            def _abort_multipart_sync():
                self.client.abort_multipart_upload(
                    Bucket=self.bucket,
                    Key=key,
                    UploadId=upload_id
                )
                return True
            
            return await asyncio.get_event_loop().run_in_executor(
                self.thread_pool, _abort_multipart_sync
            )
        except ClientError as e:
            logger.error(f"Failed to abort multipart upload for {key}: {e}")
            return False