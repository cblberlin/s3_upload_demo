from pydantic_settings import BaseSettings
from typing import List


class Settings(BaseSettings):
    # S3/R2 Configuration
    aws_access_key_id: str
    aws_secret_access_key: str
    aws_region: str = "auto"
    s3_bucket_name: str
    s3_endpoint_url: str
    
    # App Configuration
    max_file_size: int = 1024 * 1024 * 1024  # 1024MB
    chunk_size: int = 8 * 1024 * 1024       # 8MB
    allowed_extensions: str = "jpg,jpeg,png,gif,pdf,txt,doc,docx,zip,mkv,mp4,mp3,xlsx,xls,csv,ppt,pptx" # all files
    
    # API Configuration
    api_v1_prefix: str = "/api/v1"
    project_name: str = "S3 File Manager"
    
    @property
    def allowed_extensions_list(self) -> List[str]:
        return [ext.strip().lower() for ext in self.allowed_extensions.split(",")]
    
    class Config:
        env_file = ".env"


settings = Settings()