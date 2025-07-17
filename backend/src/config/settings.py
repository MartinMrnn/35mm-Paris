"""
Configuration settings for 35mm Paris backend.
Uses Pydantic for validation and environment variable management.
"""

from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings with validation."""

    # Project info
    app_name: str = "35mm-paris"
    version: str = "0.1.0"
    debug: bool = Field(default=False, description="Debug mode")

    # Database
    supabase_url: str = Field(..., description="Supabase project URL")
    supabase_key: str = Field(..., description="Supabase anon/service key")

    # API settings
    api_host: str = Field(default="0.0.0.0", description="API host")
    api_port: int = Field(default=8000, description="API port")

    # External APIs
    allocine_timeout: int = Field(
        default=30, description="Allocine API timeout in seconds"
    )

    # Logging
    log_level: str = Field(default="INFO", description="Logging level")
    log_format: str = Field(default="json", description="Log format (json or text)")

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        # This allows SUPABASE_URL to map to supabase_url
        env_prefix="",
        extra="ignore",
    )


@lru_cache
def get_settings() -> Settings:
    """
    Get cached settings instance.
    Use this function to get settings throughout the app.
    """
    return Settings()


# For backward compatibility
settings = get_settings()
SUPABASE_URL = settings.supabase_url
SUPABASE_KEY = settings.supabase_key
