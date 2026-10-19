from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    app_name: str = "rag-sec-pipeline"
    environment: str = "dev"
    log_level: str = "INFO"

    redis_url: str = "redis://localhost:6380/0"

    artifact_root: str = Field(default="./artifacts")

    sec_base_url: str = "https://www.sec.gov"
    sec_user_agent: str = "RAG_SEC_Pipeline contact@example.com"

    vector_db_url: str = "http://localhost:6333"
    embedding_endpoint: str = "http://localhost:8001"
    llm_endpoint: str = "http://localhost:8002"
    policy_endpoint: str = "http://localhost:8010"

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )
    