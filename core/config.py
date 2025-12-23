"""
Application configuration using Pydantic Settings
"""

from pydantic_settings import BaseSettings
from typing import Optional


class Settings(BaseSettings):
    """Application settings with environment variable support"""
    
    # Database
    DATABASE_URL: str = "postgresql+asyncpg://etl_user:etl_password@postgres:5432/etl_db"
    
    # API
    API_KEY: Optional[str] = None
    API_HOST: str = "0.0.0.0"
    API_PORT: int = 8000
    
    # Environment
    ENVIRONMENT: str = "development"
    LOG_LEVEL: str = "INFO"
    
    # ETL Configuration
    ETL_BATCH_SIZE: int = 500
    MAX_RETRIES: int = 3
    
    class Config:
        env_file = ".env"
        case_sensitive = True
        extra = "ignore"


settings = Settings()