import os
from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    MONGO_URL: str = os.getenv("MONGO_URL", "mongodb://mongodb:27017")
    EXTERNAL_API_URL: str = os.getenv("EXTERNAL_API_URL", "http://mock-external-api:9000")
    
    class Config:
        env_file = ".env"

settings = Settings()
