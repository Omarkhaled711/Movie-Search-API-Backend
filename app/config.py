from pydantic_settings import BaseSettings
from pydantic import ConfigDict


class Settings(BaseSettings):
    TMDB_API_KEY: str
    OMDB_API_KEY: str
    REDIS_URL: str

    model_config = ConfigDict(
        env_file=".env"
    )


settings = Settings()
