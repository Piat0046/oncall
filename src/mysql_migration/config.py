"""
Migration 설정 모듈
환경변수에서 소스/타겟 DB 연결 설정을 로드
YAML 파일에서 마이그레이션 대상 DB 및 테이블 설정을 로드
"""

from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings, SettingsConfigDict


def _find_project_root() -> Path:
    """프로젝트 루트 경로 탐색 (pyproject.toml 기준)"""
    current = Path(__file__).parent
    for _ in range(5):
        if (current / "pyproject.toml").exists():
            return current
        current = current.parent
    return Path(__file__).parent.parent.parent


PROJECT_ROOT = _find_project_root()


class SourceDBSettings(BaseSettings):
    """소스 DB 연결 설정 (마이그레이션 원본)"""

    model_config = SettingsConfigDict(
        env_file=str(PROJECT_ROOT / ".env"),
        env_prefix="SOURCE_DB_",
        extra="ignore",
    )

    host: str = Field(default="localhost")
    port: int = Field(default=3306)
    user: str = Field(default="user")
    password: str = Field(default="password")
    charset: str = Field(default="utf8mb4")

    def to_dict(self, database: str | None = None) -> dict:
        return {
            "host": self.host,
            "port": self.port,
            "user": self.user,
            "password": self.password,
            "database": database,
            "charset": self.charset,
        }


class TargetDBSettings(BaseSettings):
    """타겟 DB 연결 설정 (마이그레이션 대상)"""

    model_config = SettingsConfigDict(
        env_file=str(PROJECT_ROOT / ".env"),
        env_prefix="TARGET_DB_",
        extra="ignore",
    )

    host: str = Field(default="localhost")
    port: int = Field(default=3307)
    user: str = Field(default="laplace")
    password: str = Field(default="laplace123")
    charset: str = Field(default="utf8mb4")

    def to_dict(self, database: str | None = None) -> dict:
        return {
            "host": self.host,
            "port": self.port,
            "user": self.user,
            "password": self.password,
            "database": database,
            "charset": self.charset,
        }


class MigrationSettings(BaseSettings):
    """마이그레이션 연결 설정"""

    source: SourceDBSettings = Field(default_factory=SourceDBSettings)
    target: TargetDBSettings = Field(default_factory=TargetDBSettings)


# ============================================================
# YAML 설정 모델
# ============================================================

class TableConfig(BaseModel):
    """테이블 마이그레이션 설정"""
    name: str
    where: str = ""
    limit: int | None = None


class DatabaseConfig(BaseModel):
    """데이터베이스 마이그레이션 설정"""
    name: str
    target_name: str | None = None  # 타겟 DB명 (없으면 소스와 동일)
    mode: str = "all"  # all, tables, config
    exclude: list[str] | None = None  # None이면 빈 리스트로 처리
    tables: list[TableConfig] | None = None
    where: str = ""  # 전체 테이블에 적용할 기본 WHERE 조건
    limit: int | None = None  # 전체 테이블에 적용할 기본 LIMIT
    exclude_date_tables: bool = True  # 날짜 suffix 테이블 제외 (기본: True)
    # laplace 모드: user_id 컬럼 유무에 따라 자동 필터링
    laplace_mode: bool = False
    user_ids: list[int] | None = None  # laplace 모드에서 필터링할 user_id 목록

    def model_post_init(self, __context) -> None:
        """None 값을 빈 리스트로 변환"""
        if self.exclude is None:
            object.__setattr__(self, "exclude", [])
        if self.tables is None:
            object.__setattr__(self, "tables", [])


class LookupQueryConfig(BaseModel):
    """동적 DB 조회 쿼리 설정"""
    database: str  # 쿼리를 실행할 데이터베이스
    sql: str  # user_id 등을 조회하는 SQL


class DynamicDatabaseConfig(BaseModel):
    """동적 데이터베이스 패턴 설정

    예시:
        pattern: "laplacian_{user_id}"
        lookup_query:
          database: laplace
          sql: "SELECT user_id FROM user_subscription WHERE ..."
    """
    pattern: str  # DB명 패턴 (예: "laplacian_{user_id}")
    lookup_query: LookupQueryConfig  # ID 조회 쿼리
    target_pattern: str | None = None  # 타겟 DB명 패턴 (없으면 소스와 동일)
    mode: str = "all"
    exclude: list[str] | None = None
    tables: list[TableConfig] | None = None
    where: str = ""
    limit: int | None = None
    exclude_date_tables: bool = True

    def model_post_init(self, __context) -> None:
        """None 값을 빈 리스트로 변환"""
        if self.exclude is None:
            object.__setattr__(self, "exclude", [])
        if self.tables is None:
            object.__setattr__(self, "tables", [])


class MigrationYAMLConfig(BaseModel):
    """YAML 마이그레이션 설정"""
    databases: list[DatabaseConfig] = Field(default_factory=list)
    dynamic_databases: list[DynamicDatabaseConfig] = Field(default_factory=list)
    auto_order: bool = True
    truncate: bool = False
    create_tables: bool = True
    exclude_date_tables: bool = True  # 전역 설정 (기본: True)
    # 병렬 처리 설정
    parallel: bool = True  # 병렬 처리 활성화
    max_workers: int = 3  # 동시 DB 처리 수
    max_table_workers: int = 5  # DB당 동시 테이블 처리 수


def _parse_tables(tables_data: list) -> list[TableConfig]:
    """tables 필드 파싱"""
    tables = []
    for t in tables_data:
        if isinstance(t, str):
            tables.append(TableConfig(name=t))
        elif isinstance(t, dict):
            tables.append(TableConfig(**t))
    return tables


def load_yaml_config(yaml_path: str | Path) -> MigrationYAMLConfig:
    """YAML 설정 파일 로드"""
    path = Path(yaml_path)
    if not path.exists():
        raise FileNotFoundError(f"설정 파일을 찾을 수 없습니다: {yaml_path}")

    with open(path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f)

    # databases 배열 파싱 (None 처리)
    databases = []
    for db_data in data.get("databases") or []:
        db_data["tables"] = _parse_tables(db_data.get("tables") or [])
        databases.append(DatabaseConfig(**db_data))

    # dynamic_databases 배열 파싱 (None 처리)
    dynamic_databases = []
    for ddb_data in data.get("dynamic_databases") or []:
        ddb_data["tables"] = _parse_tables(ddb_data.get("tables") or [])
        # lookup_query 파싱
        if "lookup_query" in ddb_data:
            ddb_data["lookup_query"] = LookupQueryConfig(**ddb_data["lookup_query"])
        dynamic_databases.append(DynamicDatabaseConfig(**ddb_data))

    return MigrationYAMLConfig(
        databases=databases,
        dynamic_databases=dynamic_databases,
        auto_order=data.get("auto_order", True),
        truncate=data.get("truncate", False),
        create_tables=data.get("create_tables", True),
        exclude_date_tables=data.get("exclude_date_tables", True),
        parallel=data.get("parallel", True),
        max_workers=data.get("max_workers", 3),
        max_table_workers=data.get("max_table_workers", 5),
    )


def get_settings() -> MigrationSettings:
    """설정 로드"""
    return MigrationSettings()


# 싱글톤 인스턴스
settings = get_settings()
