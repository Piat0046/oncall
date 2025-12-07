"""
Migration 설정 모듈
환경변수에서 소스/타겟 DB 설정을 로드
"""

from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


# 프로젝트 루트 경로
PROJECT_ROOT = Path(__file__).parent.parent.parent


class SourceDBSettings(BaseSettings):
    """소스 DB 설정 (마이그레이션 원본)"""

    model_config = SettingsConfigDict(
        env_file=str(PROJECT_ROOT / ".env"),
        env_prefix="SOURCE_DB_",
        extra="ignore",
    )

    host: str = Field(default="localhost")
    port: int = Field(default=3306)
    user: str = Field(default="user")
    password: str = Field(default="password")
    database: str = Field(default="laplace")
    charset: str = Field(default="utf8mb4")

    def to_dict(self) -> dict:
        return {
            "host": self.host,
            "port": self.port,
            "user": self.user,
            "password": self.password,
            "database": self.database,
            "charset": self.charset,
        }


class TargetDBSettings(BaseSettings):
    """타겟 DB 설정 (마이그레이션 대상)"""

    model_config = SettingsConfigDict(
        env_file=str(PROJECT_ROOT / ".env"),
        env_prefix="TARGET_DB_",
        extra="ignore",
    )

    host: str = Field(default="localhost")
    port: int = Field(default=3307)
    user: str = Field(default="laplace")
    password: str = Field(default="laplace123")
    database: str = Field(default="laplace")
    charset: str = Field(default="utf8mb4")

    def to_dict(self) -> dict:
        return {
            "host": self.host,
            "port": self.port,
            "user": self.user,
            "password": self.password,
            "database": self.database,
            "charset": self.charset,
        }


class MigrationSettings(BaseSettings):
    """마이그레이션 전체 설정"""

    source: SourceDBSettings = Field(default_factory=SourceDBSettings)
    target: TargetDBSettings = Field(default_factory=TargetDBSettings)


def get_settings() -> MigrationSettings:
    """설정 로드"""
    return MigrationSettings()


# 싱글톤 인스턴스
settings = get_settings()
