from pydantic_settings import BaseSettings
from typing import List, Dict

class Settings(BaseSettings):
    # S3/R2 Configuration
    aws_access_key_id: str
    aws_secret_access_key: str
    aws_region: str = "auto"
    s3_bucket_name: str
    s3_endpoint_url: str
    
    # File Upload Configuration
    max_file_size: int = 1024 * 1024 * 1024  # 1024MB
    chunk_size: int = 25 * 1024 * 1024       # 25MB (保持兼容性，但会被动态优化覆盖)
    allowed_extensions: str = "jpg,jpeg,png,gif,pdf,txt,doc,docx,zip,mkv,mp4,mp3,xlsx,xls,csv,ppt,pptx"
    
    # 性能优化配置
    # 分块上传阈值
    multipart_threshold: int = 100 * 1024 * 1024  # 100MB，超过此大小使用分块上传
    
    # 并发上传配置
    max_concurrent_uploads: int = 5  # 最大并发上传数
    upload_timeout: int = 300        # 上传超时时间（秒）
    
    # 动态分块大小配置 (根据文件大小自动调整)
    chunk_size_small: int = 50 * 1024 * 1024    # 50MB - 适用于100MB-500MB文件
    chunk_size_medium: int = 100 * 1024 * 1024  # 100MB - 适用于500MB-1GB文件  
    chunk_size_large: int = 200 * 1024 * 1024   # 200MB - 适用于1GB+文件
    
    # 动态分块策略的文件大小阈值
    small_file_threshold: int = 500 * 1024 * 1024   # 500MB
    medium_file_threshold: int = 1024 * 1024 * 1024  # 1GB
    
    # 内存优化配置
    # 超过此大小使用流式上传而非并发上传（避免内存溢出）
    streaming_threshold: int = 2 * 1024 * 1024 * 1024  # 2GB
    
    # 并发策略配置
    min_chunks_for_concurrency: int = 2  # 至少2个分块才使用并发
    max_chunks_full_concurrency: int = 3  # 3个分块以下全并发
    max_chunks_limited_concurrency: int = 10  # 10个分块以下限制并发为3
    
    # API Configuration
    api_v1_prefix: str = "/api/v1"
    project_name: str = "S3 File Manager"
    
    @property
    def allowed_extensions_list(self) -> List[str]:
        return [ext.strip().lower() for ext in self.allowed_extensions.split(",")]
    
    @property
    def chunk_size_config(self) -> Dict[str, int]:
        """获取分块大小配置字典"""
        return {
            'small': self.chunk_size_small,
            'medium': self.chunk_size_medium, 
            'large': self.chunk_size_large,
            'default': self.chunk_size_medium
        }
    
    @property
    def file_size_thresholds(self) -> Dict[str, int]:
        """获取文件大小阈值配置"""
        return {
            'multipart': self.multipart_threshold,
            'small': self.small_file_threshold,
            'medium': self.medium_file_threshold,
            'streaming': self.streaming_threshold
        }
    
    def get_optimal_chunk_size(self, file_size: int) -> int:
        """根据文件大小获取最优分块大小"""
        if file_size <= self.small_file_threshold:
            return self.chunk_size_small
        elif file_size <= self.medium_file_threshold:
            return self.chunk_size_medium
        else:
            return self.chunk_size_large
    
    def get_optimal_concurrency(self, total_chunks: int) -> int:
        """根据分块数量计算最优并发数"""
        if total_chunks < self.min_chunks_for_concurrency:
            return 1
        elif total_chunks <= self.max_chunks_full_concurrency:
            return total_chunks
        elif total_chunks <= self.max_chunks_limited_concurrency:
            return min(3, total_chunks)
        else:
            return min(self.max_concurrent_uploads, total_chunks)
    
    def should_use_streaming(self, file_size: int) -> bool:
        """判断是否应该使用流式上传"""
        return file_size >= self.streaming_threshold
    
    def should_use_multipart(self, file_size: int) -> bool:
        """判断是否应该使用分块上传"""
        return file_size >= self.multipart_threshold
    
    class Config:
        env_file = ".env"

settings = Settings()