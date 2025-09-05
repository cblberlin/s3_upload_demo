from fastapi import APIRouter, Depends, HTTPException, Query, Response
from typing import Optional, List
from loguru import logger

from app.domains.file_service import FileService
from app.domains.download_service import DownloadService
from app.dependencies import get_file_service, get_download_service
from app.core.models import (
    FileInfo, FileListResponse, DeleteResponse, 
    FileUpdateRequest
)
from app.core.exceptions import FileNotFoundError, S3OperationError

router = APIRouter()


@router.get("/list", response_model=FileListResponse)
async def list_files(
    prefix: str = Query("", description="文件名前缀过滤"),
    max_keys: int = Query(100, ge=1, le=1000, description="返回的最大文件数"),
    marker: Optional[str] = Query(None, description="分页标记"),
    user_id: Optional[str] = Query(None, description="用户ID过滤"),
    file_service: FileService = Depends(get_file_service)
):
    """
    列出文件
    
    - **prefix**: 文件名前缀过滤
    - **max_keys**: 返回的最大文件数 (1-1000)
    - **marker**: 分页标记
    - **user_id**: 用户ID过滤
    """
    try:
        result = await file_service.list_files(prefix, max_keys, marker, user_id)
        return result
    except S3OperationError as e:
        raise HTTPException(status_code=500, detail=str(e))
    except Exception as e:
        logger.error(f"Unexpected error in list files: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/info/{file_key:path}", response_model=FileInfo)
async def get_file_info(
    file_key: str,
    file_service: FileService = Depends(get_file_service)
):
    """
    获取文件信息
    
    - **file_key**: 文件键
    """
    try:
        result = await file_service.get_file_info(file_key)
        return result
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except S3OperationError as e:
        raise HTTPException(status_code=500, detail=str(e))
    except Exception as e:
        logger.error(f"Unexpected error in get file info: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.delete("/delete/{file_key:path}", response_model=DeleteResponse)
async def delete_file(
    file_key: str,
    file_service: FileService = Depends(get_file_service)
):
    """
    删除单个文件
    
    - **file_key**: 文件键
    """
    try:
        result = await file_service.delete_file(file_key)
        return result
    except S3OperationError as e:
        raise HTTPException(status_code=500, detail=str(e))
    except Exception as e:
        logger.error(f"Unexpected error in delete file: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.delete("/delete/batch", response_model=DeleteResponse)
async def delete_files_batch(
    file_keys: List[str],
    file_service: FileService = Depends(get_file_service)
):
    """
    批量删除文件
    
    - **file_keys**: 文件键列表
    """
    try:
        if len(file_keys) > 1000:  # S3限制
            raise HTTPException(status_code=400, detail="Maximum 1000 files allowed per batch delete")
        
        result = await file_service.delete_files(file_keys)
        return result
    except S3OperationError as e:
        raise HTTPException(status_code=500, detail=str(e))
    except Exception as e:
        logger.error(f"Unexpected error in batch delete: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.post("/copy", response_model=FileInfo)
async def copy_file(
    source_key: str,
    dest_key: str,
    new_name: Optional[str] = None,
    file_service: FileService = Depends(get_file_service)
):
    """
    复制文件
    
    - **source_key**: 源文件键
    - **dest_key**: 目标文件键
    - **new_name**: 新文件名（可选）
    """
    try:
        result = await file_service.copy_file(source_key, dest_key, new_name)
        return result
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except S3OperationError as e:
        raise HTTPException(status_code=500, detail=str(e))
    except Exception as e:
        logger.error(f"Unexpected error in copy file: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.post("/move", response_model=FileInfo)
async def move_file(
    source_key: str,
    dest_key: str,
    new_name: Optional[str] = None,
    file_service: FileService = Depends(get_file_service)
):
    """
    移动文件
    
    - **source_key**: 源文件键
    - **dest_key**: 目标文件键
    - **new_name**: 新文件名（可选）
    """
    try:
        result = await file_service.move_file(source_key, dest_key, new_name)
        return result
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except S3OperationError as e:
        raise HTTPException(status_code=500, detail=str(e))
    except Exception as e:
        logger.error(f"Unexpected error in move file: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.put("/rename/{file_key:path}", response_model=FileInfo)
async def rename_file(
    file_key: str,
    new_name: str,
    file_service: FileService = Depends(get_file_service)
):
    """
    重命名文件
    
    - **file_key**: 文件键
    - **new_name**: 新文件名
    """
    try:
        if not new_name.strip():
            raise HTTPException(status_code=400, detail="New name cannot be empty")
        
        result = await file_service.rename_file(file_key, new_name.strip())
        return result
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except S3OperationError as e:
        raise HTTPException(status_code=500, detail=str(e))
    except Exception as e:
        logger.error(f"Unexpected error in rename file: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.put("/update/{file_key:path}", response_model=FileInfo)
async def update_file_metadata(
    file_key: str,
    update_data: FileUpdateRequest,
    file_service: FileService = Depends(get_file_service)
):
    """
    更新文件元数据
    
    - **file_key**: 文件键
    - **update_data**: 更新数据
    """
    try:
        result = await file_service.update_file_metadata(file_key, update_data)
        return result
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except S3OperationError as e:
        raise HTTPException(status_code=500, detail=str(e))
    except Exception as e:
        logger.error(f"Unexpected error in update file metadata: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/search", response_model=FileListResponse)
async def search_files(
    query: str = Query(..., min_length=1, description="搜索关键词"),
    user_id: Optional[str] = Query(None, description="用户ID过滤"),
    max_results: int = Query(50, ge=1, le=500, description="最大结果数"),
    file_service: FileService = Depends(get_file_service)
):
    """
    搜索文件
    
    - **query**: 搜索关键词
    - **user_id**: 用户ID过滤
    - **max_results**: 最大结果数 (1-500)
    """
    try:
        result = await file_service.search_files(query, user_id, max_results)
        return result
    except S3OperationError as e:
        raise HTTPException(status_code=500, detail=str(e))
    except Exception as e:
        logger.error(f"Unexpected error in search files: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/download/{file_key:path}")
async def download_file(
    file_key: str,
    stream: bool = Query(False, description="是否使用流式下载"),
    download_service: DownloadService = Depends(get_download_service)
):
    """
    下载文件
    
    - **file_key**: 文件键
    - **stream**: 是否使用流式下载
    """
    try:
        if stream:
            return await download_service.download_file_stream(file_key)
        else:
            content = await download_service.download_file_direct(file_key)
            # 获取文件信息用于设置响应头
            from app.dependencies import get_file_service
            file_service = get_file_service()
            file_info = await file_service.get_file_info(file_key)
            
            return Response(
                content=content,
                media_type=file_info.content_type or 'application/octet-stream',
                headers={
                    'Content-Disposition': f'attachment; filename="{file_info.name}"'
                }
            )
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except S3OperationError as e:
        raise HTTPException(status_code=500, detail=str(e))
    except Exception as e:
        logger.error(f"Unexpected error in download file: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/download/url/{file_key:path}")
async def get_download_url(
    file_key: str,
    expiration: int = Query(3600, ge=300, le=604800, description="URL有效期（秒）"),
    download_service: DownloadService = Depends(get_download_service)
):
    """
    获取下载URL
    
    - **file_key**: 文件键
    - **expiration**: URL有效期（秒），300-604800（7天）
    """
    try:
        url = await download_service.get_download_url(file_key, expiration)
        return {"download_url": url, "expiration": expiration}
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except S3OperationError as e:
        raise HTTPException(status_code=500, detail=str(e))
    except Exception as e:
        logger.error(f"Unexpected error in get download URL: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/download/range/{file_key:path}")
async def download_file_range(
    file_key: str,
    start: int = Query(..., ge=0, description="起始位置"),
    end: Optional[int] = Query(None, ge=0, description="结束位置（可选）"),
    download_service: DownloadService = Depends(get_download_service)
):
    """
    范围下载文件（支持断点续传）
    
    - **file_key**: 文件键
    - **start**: 起始位置
    - **end**: 结束位置（可选）
    """
    try:
        result = await download_service.download_file_range(file_key, start, end)
        
        return Response(
            content=result['content'],
            media_type=result['content_type'] or 'application/octet-stream',
            headers={
                'Content-Range': f'bytes {result["start"]}-{result["end"]}/{result["total_size"]}',
                'Accept-Ranges': 'bytes',
                'Content-Length': str(len(result['content']))
            },
            status_code=206  # Partial Content
        )
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except S3OperationError as e:
        raise HTTPException(status_code=500, detail=str(e))
    except Exception as e:
        logger.error(f"Unexpected error in range download: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/preview/{file_key:path}")
async def preview_file(
    file_key: str,
    max_size: int = Query(1048576, ge=1024, le=10485760, description="最大预览大小"),
    download_service: DownloadService = Depends(get_download_service)
):
    """
    预览文件内容
    
    - **file_key**: 文件键
    - **max_size**: 最大预览大小（字节），1KB-10MB
    """
    try:
        result = await download_service.preview_file(file_key, max_size)
        
        # 返回预览信息，不包含二进制内容
        return {
            "file_info": result['file_info'],
            "text_content": result['text_content'],
            "is_text": result['is_text'],
            "is_truncated": result['is_truncated'],
            "preview_size": result['preview_size']
        }
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except S3OperationError as e:
        raise HTTPException(status_code=500, detail=str(e))
    except Exception as e:
        logger.error(f"Unexpected error in file preview: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")