from functools import lru_cache
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env", env_file_encoding="utf-8",
        case_sensitive=False, extra="ignore",
    )
    # Angel One SmartAPI
    angel_one_api_key: str = ""
    angel_one_client_id: str = ""
    angel_one_password: str = ""
    angel_one_totp_secret: str = ""
    # Dhan
    dhan_client_id: str = ""
    dhan_access_token: str = ""
    # Anthropic
    anthropic_api_key: str = ""
    # Telegram
    telegram_bot_token: str = ""
    telegram_chat_id: str = ""
    # TradingView
    tv_webhook_secret: str = ""
    # Infrastructure
    redis_url: str = "redis://localhost:6379"
    database_url: str = "postgresql://localhost/trading"
    # App
    environment: str = "production"
    log_level: str = "INFO"

    @property
    def is_production(self) -> bool:
        return self.environment == "production"


@lru_cache()
def get_settings() -> Settings:
    return Settings()


settings: Settings = get_settings()
