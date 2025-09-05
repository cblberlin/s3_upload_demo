from pydantic import BaseModel
from typing import Optional, List, Dict, Any
from datetime import datetime
from enum import Enum


class UploadStatus(str, Enum):
    PENDING = "pending"
    UPLOADING = "uploading"
    COMPLETED = "completed"
    FAILED = "failed"


class FileInfo(BaseModel):
    key: str
    name: str
    size: int
    content_type: str
    etag: Optional[str] = None
    last_modified: Optional[datetime] = None
    url: Optional[str] = None


class FileListResponse(BaseModel):
    files: List[FileInfo]
    total_count: int
    has_more: bool
    next_marker: Optional[str] = None


class UploadResponse(BaseModel):
    success: bool
    message: str
    file_key: Optional[str] = None
    file_url: Optional[str] = None


class ChunkUploadInit(BaseModel):
    filename: str
    file_size: int
    content_type: str
    total_chunks: int


class ChunkUploadInitResponse(BaseModel):
    upload_id: str
    file_key: str
    chunk_urls: List[Dict[str, Any]]


class ChunkUploadComplete(BaseModel):
    upload_id: str
    file_key: str
    parts: List[Dict[str, Any]]


class DeleteResponse(BaseModel):
    success: bool
    message: str
    deleted_keys: List[str]


class FileUpdateRequest(BaseModel):
    new_name: Optional[str] = None
    new_content_type: Optional[str] = None


class ErrorResponse(BaseModel):
    error: str
    detail: Optional[str] = None