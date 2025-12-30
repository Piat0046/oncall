"""
Trino 마이그레이션 설정 모듈
- 소스/타겟 Trino 연결 설정
- S3 설정
- YAML 마이그레이션 설정 로드
"""

import re
from pathlib import Path
from typing import Literal

import yaml
from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings, SettingsConfigDict


def matches_any_regex(name: str, patterns: list[str]) -> bool:
    """이름이 정규식 패턴 중 하나와 매칭되는지 확인

    Args:
        name: 확인할 이름 (테이블명 등)
        patterns: 정규식 패턴 목록

    Returns:
        하나라도 매칭되면 True

    Examples:
        >>> matches_any_regex("mview_commerce", ["^mview_"])
        True
        >>> matches_any_regex("commerce_log", ["_log$", "^test_"])
        True
        >>> matches_any_regex("users", ["^mview_", "_log$"])
        False
    """
    for pattern in patterns:
        if re.search(pattern, name):
            return True
    return False


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


def _clean_endpoint_url(url: str | None) -> str | None:
    """endpoint URL 정제 (주석, 공백 제거)"""
    if not url:
        return None
    # 주석 제거 (# 이후 무시)
    if "#" in url:
        url = url.split("#")[0]
    url = url.strip()
    # 빈 문자열이면 None 반환
    return url if url else None


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

    def model_post_init(self, __context) -> None:
        """초기화 후 endpoint URL 정제"""
        # pydantic v2에서 값 수정
        object.__setattr__(self, "source_endpoint_url", _clean_endpoint_url(self.source_endpoint_url))
        object.__setattr__(self, "target_endpoint_url", _clean_endpoint_url(self.target_endpoint_url))


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
    schema_name: str = Field(alias="schema")  # 단일 스키마 (내부용)
    method: Literal["s3_copy", "insert_select"] = "s3_copy"

    # 포함할 테이블 (정확히 매칭)
    # 예: ["users", "orders", "commerce_ad"]
    include: list[str] | None = None

    # 포함할 테이블 (정규식 패턴)
    # 예: ["^mview_", "_log$", "commerce"]
    include_regex: list[str] | None = None

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

    def matches_include(self, table_name: str) -> bool:
        """테이블이 include 조건에 맞는지 확인

        - include와 include_regex 모두 None이면 모든 테이블 포함
        - include: 정확한 문자열 매칭
        - include_regex: 정규식 매칭 (re.search)
        """
        # 둘 다 없으면 모든 테이블 포함
        if not self.include and not self.include_regex:
            return True

        # 정확한 매칭 체크
        if self.include and table_name in self.include:
            return True

        # 정규식 매칭 체크
        if self.include_regex and matches_any_regex(table_name, self.include_regex):
            return True

        return False

    def filter_tables(self, tables: list[str]) -> list[str]:
        """include/exclude 조건으로 테이블 필터링"""
        result = []
        exclude_set = set(self.exclude or [])

        for table in tables:
            # exclude 체크
            if table in exclude_set:
                continue
            # include 체크
            if not self.matches_include(table):
                continue
            result.append(table)

        return result

    class Config:
        populate_by_name = True


class SchemaMigrationConfigInput(BaseModel):
    """스키마 마이그레이션 YAML 입력용 (schema가 리스트 가능)"""
    catalog: str
    schema_names: str | list[str] = Field(alias="schema")  # 단일 또는 리스트
    method: Literal["s3_copy", "insert_select"] = "s3_copy"
    include: list[str] | None = None
    include_regex: list[str] | None = None
    exclude: list[str] | None = None
    partition_filter: list[str] | None = None
    target_catalog: str | None = None
    target_schema: str | None = None

    class Config:
        populate_by_name = True

    def expand(self) -> list[SchemaMigrationConfig]:
        """스키마 리스트를 개별 SchemaMigrationConfig로 펼침"""
        schemas = self.schema_names if isinstance(self.schema_names, list) else [self.schema_names]
        return [
            SchemaMigrationConfig(
                catalog=self.catalog,
                schema=schema,
                method=self.method,
                include=self.include,
                include_regex=self.include_regex,
                exclude=self.exclude,
                partition_filter=self.partition_filter,
                target_catalog=self.target_catalog,
                target_schema=self.target_schema,
            )
            for schema in schemas
        ]


class MigrationYAMLConfig(BaseModel):
    """YAML 마이그레이션 설정"""
    # 개별 테이블 설정
    tables: list[TableMigrationConfig] = Field(default_factory=list)

    # 스키마 단위 설정
    schemas: list[SchemaMigrationConfig] = Field(default_factory=list)

    # 전역 설정
    parallel_tables: int = 3  # 테이블 단위 병렬 처리 (스키마 마이그레이션 시)
    parallel_partitions: int = 5  # 파티션 단위 병렬 복사 (S3 복사 시)
    parallel_inserts: int = 4  # 배치 INSERT 병렬 수 (INSERT SELECT 시)
    batch_size: int = 1000  # INSERT 배치 크기
    dry_run: bool = False
    stop_on_error: bool = False  # 에러 발생 시 즉시 중단

    # S3 설정 오버라이드 (주의: s3_copy 방식은 소스 버킷 유지)
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

    # schemas 파싱 (리스트 지원)
    schemas = []
    for s in data.get("schemas") or []:
        # SchemaMigrationConfigInput으로 파싱 후 expand
        input_config = SchemaMigrationConfigInput(**s)
        schemas.extend(input_config.expand())

    return MigrationYAMLConfig(
        tables=tables,
        schemas=schemas,
        parallel_tables=data.get("parallel_tables", 3),
        parallel_partitions=data.get("parallel_partitions", 5),
        parallel_inserts=data.get("parallel_inserts", 4),
        batch_size=data.get("batch_size", 1000),
        dry_run=data.get("dry_run", False),
        stop_on_error=data.get("stop_on_error", False),
        source_bucket=data.get("source_bucket"),
        target_bucket=data.get("target_bucket"),
    )


def get_settings() -> TrinoMigrationSettings:
    """설정 로드"""
    return TrinoMigrationSettings()


# 싱글톤 인스턴스
settings = get_settings()
