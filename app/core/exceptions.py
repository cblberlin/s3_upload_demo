class FileManagerException(Exception):
    """文件管理基础异常"""
    pass


class FileNotFoundError(FileManagerException):
    """文件未找到异常"""
    pass


class FileSizeExceedError(FileManagerException):
    """文件大小超限异常"""
    pass


class FileTypeNotAllowedError(FileManagerException):
    """文件类型不允许异常"""
    pass


class UploadFailedError(FileManagerException):
    """上传失败异常"""
    pass


class S3OperationError(FileManagerException):
    """S3操作异常"""
    pass


class ChunkUploadError(FileManagerException):
    """分块上传异常"""
    pass