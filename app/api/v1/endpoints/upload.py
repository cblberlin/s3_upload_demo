from fastapi import APIRouter, Depends, UploadFile, File, Form, HTTPException
from typing import List, Optional
from loguru import logger

from app.domains.upload_service import UploadService
from app.dependencies import get_upload_service
from app.core.models import (
    UploadResponse, ChunkUploadInit, ChunkUploadInitResponse, 
    ChunkUploadComplete
)
from app.core.exceptions import (
    FileSizeExceedError, FileTypeNotAllowedError, 
    UploadFailedError, ChunkUploadError
)

router = APIRouter()


@router.post("/single", response_model=UploadResponse)
async def upload_single_file(
    file: UploadFile = File(...),
    user_id: Optional[str] = Form(None),
    upload_service: UploadService = Depends(get_upload_service)
):
    """
    上传单个文件
    
    - **file**: 要上传的文件
    - **user_id**: 可选的用户ID，用于文件路径分组
    """
    try:
        result = await upload_service.upload_single_file(file, user_id)
        return result
    except (FileSizeExceedError, FileTypeNotAllowedError) as e:
        raise HTTPException(status_code=400, detail=str(e))
    except UploadFailedError as e:
        raise HTTPException(status_code=500, detail=str(e))
    except Exception as e:
        logger.error(f"Unexpected error in single file upload: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.post("/multiple", response_model=List[UploadResponse])
async def upload_multiple_files(
    files: List[UploadFile] = File(...),
    user_id: Optional[str] = Form(None),
    upload_service: UploadService = Depends(get_upload_service)
):
    """
    上传多个文件
    
    - **files**: 要上传的文件列表
    - **user_id**: 可选的用户ID，用于文件路径分组
    """
    try:
        if len(files) > 20:  # 限制单次上传文件数量
            raise HTTPException(status_code=400, detail="Maximum 20 files allowed per upload")
        
        results = await upload_service.upload_multiple_files(files, user_id)
        return results
    except Exception as e:
        logger.error(f"Unexpected error in multiple file upload: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.post("/chunk/init", response_model=ChunkUploadInitResponse)
async def init_chunk_upload(
    init_data: ChunkUploadInit,
    user_id: Optional[str] = None,
    upload_service: UploadService = Depends(get_upload_service)
):
    """
    初始化分块上传
    
    - **init_data**: 分块上传初始化数据
    - **user_id**: 可选的用户ID
    """
    try:
        result = await upload_service.init_chunk_upload(init_data, user_id)
        return result
    except ChunkUploadError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Unexpected error in chunk upload init: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.post("/chunk/complete", response_model=UploadResponse)
async def complete_chunk_upload(
    complete_data: ChunkUploadComplete,
    user_id: Optional[str] = None,
    upload_service: UploadService = Depends(get_upload_service)
):
    """
    完成分块上传
    
    - **complete_data**: 分块上传完成数据
    - **user_id**: 可选的用户ID
    """
    try:
        result = await upload_service.complete_chunk_upload(complete_data, user_id)
        return result
    except ChunkUploadError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Unexpected error in chunk upload complete: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.delete("/chunk/abort")
async def abort_chunk_upload(
    file_key: str,
    upload_id: str,
    upload_service: UploadService = Depends(get_upload_service)
):
    """
    中止分块上传
    
    - **file_key**: 文件键
    - **upload_id**: 上传ID
    """
    try:
        success = await upload_service.abort_chunk_upload(file_key, upload_id)
        return {"success": success, "message": "Chunk upload aborted" if success else "Failed to abort"}
    except Exception as e:
        logger.error(f"Unexpected error in abort chunk upload: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/chunk/info")
async def get_chunk_info(
    file_size: int,
    chunk_size: Optional[int] = None,
    upload_service: UploadService = Depends(get_upload_service)
):
    """
    计算分块上传信息
    
    - **file_size**: 文件大小（字节）
    - **chunk_size**: 可选的分块大小，默认使用配置值
    """
    try:
        if file_size <= 0:
            raise HTTPException(status_code=400, detail="File size must be greater than 0")
        
        chunk_info = upload_service.calculate_chunks(file_size, chunk_size)
        return chunk_info
    except Exception as e:
        logger.error(f"Unexpected error in get chunk info: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")