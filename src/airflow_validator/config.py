"""설정 관리 모듈"""

from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

# 프로젝트 루트 디렉토리
PROJECT_ROOT = Path(__file__).parent.parent.parent


class MySQLSettings(BaseSettings):
    """MySQL 연결 설정"""

    model_config = SettingsConfigDict(
        env_prefix="MYSQL_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    host: str = Field(default="localhost")
    port: int = Field(default=3306)
    user: str = Field(default="root")
    password: str = Field(default="")
    database: str = Field(default="airflow_results")
    charset: str = Field(default="utf8mb4")


class AirflowAPISettings(BaseSettings):
    """Airflow REST API 설정"""

    model_config = SettingsConfigDict(
        env_prefix="AIRFLOW_API_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    url: str = Field(default="http://localhost:8080")
    user: str = Field(default="admin")
    password: str = Field(default="admin")


class PostgresSettings(BaseSettings):
    """PostgreSQL 연결 설정 (Airflow 메타데이터)"""

    model_config = SettingsConfigDict(
        env_prefix="POSTGRES_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    host: str = Field(default="localhost")
    port: int = Field(default=5432)
    user: str = Field(default="airflow")
    password: str = Field(default="")
    database: str = Field(default="airflow")


class AppSettings(BaseSettings):
    """애플리케이션 설정"""

    model_config = SettingsConfigDict(
        env_prefix="APP_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    log_level: str = Field(default="INFO")
    output_dir: str = Field(default="./output")
