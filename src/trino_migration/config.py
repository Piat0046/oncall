"""
Trino 마이그레이션 설정 모듈
- 소스/타겟 Trino 연결 설정
- S3 설정
- YAML 마이그레이션 설정 로드
"""

from pathlib import Path
from typing import Literal

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


# ============================================================
# 환경변수 기반 설정
# ============================================================

class SourceTrinoSettings(BaseSettings):
    """소스 Trino 연결 설정"""

    model_config = SettingsConfigDict(
        env_file=str(PROJECT_ROOT / ".env"),
        env_prefix="SOURCE_TRINO_",
        extra="ignore",
    )

    host: str = Field(default="localhost")
    port: int = Field(default=8080)
    user: str = Field(default="trino")
    catalog: str = Field(default="hive")
    schema_: str = Field(default="default", alias="schema")

    def get_connection_string(self) -> str:
        return f"{self.host}:{self.port}"


class TargetTrinoSettings(BaseSettings):
    """타겟 Trino 연결 설정"""

    model_config = SettingsConfigDict(
        env_file=str(PROJECT_ROOT / ".env"),
        env_prefix="TARGET_TRINO_",
        extra="ignore",
    )

    host: str = Field(default="localhost")
    port: int = Field(default=8080)
    user: str = Field(default="trino")
    catalog: str = Field(default="hive")
    schema_: str = Field(default="default", alias="schema")

    def get_connection_string(self) -> str:
        return f"{self.host}:{self.port}"


class S3Settings(BaseSettings):
    """S3 설정"""

    model_config = SettingsConfigDict(
        env_file=str(PROJECT_ROOT / ".env"),
        env_prefix="S3_",
        extra="ignore",
    )

    # 소스 S3
    source_bucket: str = Field(default="")
    source_prefix: str = Field(default="")
    source_endpoint_url: str | None = Field(default=None)

    # 타겟 S3
    target_bucket: str = Field(default="warehouse")
    target_prefix: str = Field(default="")
    target_endpoint_url: str | None = Field(default=None)

    # AWS 설정
    aws_profile: str = Field(default="default")
    aws_region: str = Field(default="ap-northeast-2")


class TrinoMigrationSettings(BaseSettings):
    """마이그레이션 전체 설정"""

    source: SourceTrinoSettings = Field(default_factory=SourceTrinoSettings)
    target: TargetTrinoSettings = Field(default_factory=TargetTrinoSettings)
    s3: S3Settings = Field(default_factory=S3Settings)


# ============================================================
# YAML 설정 모델
# ============================================================

class TableMigrationConfig(BaseModel):
    """테이블 마이그레이션 설정"""
    catalog: str  # 소스 카탈로그 (필수)
    schema_name: str = Field(alias="schema")
    table: str
    method: Literal["s3_copy", "insert_select"] = "s3_copy"

    # S3 복사용 파티션 필터 (예: ["dt >= '2024-01-01'", "dt <= '2024-12-31'"])
    partition_filter: list[str] | None = None

    # INSERT SELECT용 WHERE 조건
    where: str | None = None

    # 타겟 설정 (기본: 소스와 동일)
    target_catalog: str | None = None  # 없으면 소스 catalog 사용
    target_schema: str | None = None
    target_table: str | None = None

    # 병렬 처리
    parallel_partitions: int = 5

    class Config:
        populate_by_name = True


class SchemaMigrationConfig(BaseModel):
    """스키마 단위 마이그레이션 설정"""
    catalog: str  # 소스 카탈로그 (필수)
    schema_name: str = Field(alias="schema")
    method: Literal["s3_copy", "insert_select"] = "s3_copy"

    # 제외할 테이블
    exclude: list[str] | None = None

    # 파티션 필터 (전체 테이블에 적용)
    partition_filter: list[str] | None = None

    # 타겟 설정 (기본: 소스와 동일)
    target_catalog: str | None = None  # 없으면 소스 catalog 사용
    target_schema: str | None = None

    @property
    def exclude_tables(self) -> list[str]:
        """제외 테이블 목록 (None이면 빈 리스트)"""
        return self.exclude or []

    class Config:
        populate_by_name = True


class MigrationYAMLConfig(BaseModel):
    """YAML 마이그레이션 설정"""
    # 개별 테이블 설정
    tables: list[TableMigrationConfig] = Field(default_factory=list)

    # 스키마 단위 설정
    schemas: list[SchemaMigrationConfig] = Field(default_factory=list)

    # 전역 설정
    parallel_tables: int = 3
    parallel_partitions: int = 5
    parallel_inserts: int = 4  # INSERT 병렬 수
    batch_size: int = 1000  # INSERT 배치 크기
    dry_run: bool = False

    # S3 설정 오버라이드
    source_bucket: str | None = None
    target_bucket: str | None = None


def load_yaml_config(yaml_path: str | Path) -> MigrationYAMLConfig:
    """YAML 설정 파일 로드"""
    path = Path(yaml_path)
    if not path.exists():
        raise FileNotFoundError(f"설정 파일을 찾을 수 없습니다: {yaml_path}")

    with open(path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f)

    # tables 파싱
    tables = []
    for t in data.get("tables") or []:
        tables.append(TableMigrationConfig(**t))

    # schemas 파싱
    schemas = []
    for s in data.get("schemas") or []:
        schemas.append(SchemaMigrationConfig(**s))

    return MigrationYAMLConfig(
        tables=tables,
        schemas=schemas,
        parallel_tables=data.get("parallel_tables", 3),
        parallel_partitions=data.get("parallel_partitions", 5),
        parallel_inserts=data.get("parallel_inserts", 4),
        batch_size=data.get("batch_size", 1000),
        dry_run=data.get("dry_run", False),
        source_bucket=data.get("source_bucket"),
        target_bucket=data.get("target_bucket"),
    )


def get_settings() -> TrinoMigrationSettings:
    """설정 로드"""
    return TrinoMigrationSettings()


# 싱글톤 인스턴스
settings = get_settings()
