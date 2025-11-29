from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    DB_USER: str
    DB_PASSWORD: str
    DB_HOST: str
    DB_PORT: int
    DB_NAME: str

    model_config = SettingsConfigDict(
        env_file=Path(__file__).parent / ".env",
        env_file_encoding='utf-8'
    )

    def get_db_url(self):
        return (f"postgresql://{self.DB_USER}:{self.DB_PASSWORD}@"
                f"{self.DB_HOST}:{self.DB_PORT}/{self.DB_NAME}")


settings = Settings()

