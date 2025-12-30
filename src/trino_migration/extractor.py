"""
메타데이터 추출기
소스 Trino에서 DDL, 파티션, 테이블 정보 추출
"""

import re
from dataclasses import dataclass, field
from typing import Any

from rich.console import Console

from trino_migration.client import TrinoClient

console = Console()


@dataclass
class TableMetadata:
    """테이블 메타데이터"""
    catalog: str
    schema_name: str
    table_name: str
    ddl: str
    location: str | None
    file_format: str | None
    partition_columns: list[str]
    partitions: list[dict[str, Any]]
    columns: list[dict[str, Any]]
    row_count: int | None = None

    @property
    def is_partitioned(self) -> bool:
        return len(self.partition_columns) > 0

    @property
    def s3_bucket(self) -> str | None:
        """S3 버킷 추출"""
        if self.location:
            match = re.match(r"s3[a]?://([^/]+)", self.location)
            if match:
                return match.group(1)
        return None

    @property
    def s3_prefix(self) -> str | None:
        """S3 prefix 추출"""
        if self.location:
            match = re.match(r"s3[a]?://[^/]+/(.+)", self.location)
            if match:
                return match.group(1).rstrip("/")
        return None


@dataclass
class SchemaMetadata:
    """스키마 메타데이터"""
    catalog: str
    schema_name: str
    tables: list[TableMetadata] = field(default_factory=list)

    @property
    def table_count(self) -> int:
        return len(self.tables)


class MetadataExtractor:
    """메타데이터 추출기"""

    def __init__(self, client: TrinoClient):
        self.client = client

    def extract_table_metadata(
        self,
        catalog: str,
        schema: str,
        table: str,
        include_row_count: bool = False,
    ) -> TableMetadata | None:
        """테이블 메타데이터 추출 (VIEW/MATERIALIZED VIEW는 None 반환)"""
        # 테이블 타입 확인
        table_type = self.client.get_table_type(schema, table, catalog)
        if table_type != "BASE TABLE":
            console.print(f"  [yellow]스킵: {catalog}.{schema}.{table} ({table_type})[/yellow]")
            return None

        console.print(f"  [dim]메타데이터 추출: {catalog}.{schema}.{table}[/dim]")

        try:
            ddl = self.client.get_table_ddl(schema, table, catalog)
        except Exception as e:
            # MATERIALIZED VIEW 등 DDL 조회 불가한 경우 스킵
            if "materialized view" in str(e).lower():
                console.print(f"  [yellow]스킵: {catalog}.{schema}.{table} (MATERIALIZED VIEW)[/yellow]")
                return None
            raise
        location = self.client.get_table_location(schema, table, catalog)
        file_format = self.client.get_table_format(schema, table, catalog)
        partition_columns = self.client.get_partition_columns(schema, table, catalog)
        columns = self.client.get_table_columns(schema, table, catalog)

        # 파티션 조회 (파티션 테이블인 경우)
        partitions = []
        if partition_columns:
            try:
                partitions = self.client.get_partitions(schema, table, catalog)
            except Exception as e:
                console.print(f"    [yellow]파티션 조회 실패: {e}[/yellow]")

        row_count = None
        if include_row_count:
            try:
                row_count = self.client.get_row_count(schema, table, catalog=catalog)
            except Exception as e:
                console.print(f"    [yellow]row count 조회 실패: {e}[/yellow]")

        return TableMetadata(
            catalog=catalog,
            schema_name=schema,
            table_name=table,
            ddl=ddl,
            location=location,
            file_format=file_format,
            partition_columns=partition_columns,
            partitions=partitions,
            columns=columns,
            row_count=row_count,
        )

    def extract_schema_metadata(
        self,
        catalog: str,
        schema: str,
        exclude_tables: list[str] | None = None,
        include_row_count: bool = False,
    ) -> SchemaMetadata:
        """스키마 전체 메타데이터 추출"""
        console.print(f"[cyan]스키마 메타데이터 추출: {catalog}.{schema}[/cyan]")

        tables = self.client.get_tables(schema, catalog)
        exclude_set = set(exclude_tables or [])

        metadata = SchemaMetadata(catalog=catalog, schema_name=schema)

        for table in tables:
            if table in exclude_set:
                console.print(f"  [dim]제외: {table}[/dim]")
                continue

            try:
                table_meta = self.extract_table_metadata(
                    catalog, schema, table, include_row_count
                )
                if table_meta:  # None이면 스킵 (VIEW 등)
                    metadata.tables.append(table_meta)
            except Exception as e:
                console.print(f"  [red]오류 ({table}): {e}[/red]")

        console.print(f"  총 {len(metadata.tables)}개 테이블 추출 완료")
        return metadata

    # 버전별로 호환되지 않을 수 있는 Iceberg 테이블 속성들
    INCOMPATIBLE_PROPERTIES = [
        "max_commit_retry",
        "commit_retry_min_wait_ms",
        "commit_retry_max_wait_ms",
        "commit_num_retries",
        "commit_total_retry_time_ms",
        "write_parallelism",
        "target_max_file_size_bytes",
    ]

    def generate_target_ddl(
        self,
        metadata: TableMetadata,
        target_catalog: str,
        target_schema: str | None = None,
        target_table: str | None = None,
        target_location: str | None = None,
    ) -> str:
        """타겟용 DDL 생성 (location 변경, 호환되지 않는 속성 제거)"""
        ddl = metadata.ddl

        # 카탈로그/스키마/테이블명 변경
        new_schema = target_schema or metadata.schema_name
        new_table = target_table or metadata.table_name
        new_full_name = f"{target_catalog}.{new_schema}.{new_table}"

        # 테이블명 치환 (CREATE TABLE 문에서)
        ddl = re.sub(
            rf"CREATE\s+TABLE\s+\S*\.?{re.escape(metadata.table_name)}",
            f"CREATE TABLE {new_full_name}",
            ddl,
            flags=re.IGNORECASE,
        )

        # 호환되지 않는 속성 제거
        for prop in self.INCOMPATIBLE_PROPERTIES:
            # 속성 = 값 형태 제거 (숫자, 문자열 모두 지원)
            ddl = re.sub(
                rf",?\s*{prop}\s*=\s*(?:'[^']*'|\d+)",
                "",
                ddl,
                flags=re.IGNORECASE,
            )

        # 빈 WITH () 정리 - WITH ( ) 또는 WITH (  ,  ) 형태 제거
        ddl = re.sub(r"WITH\s*\(\s*,", "WITH (", ddl)  # WITH ( , ... → WITH ( ...
        ddl = re.sub(r",\s*\)", ")", ddl)  # ... , ) → ... )
        ddl = re.sub(r"WITH\s*\(\s*\)", "", ddl)  # 빈 WITH () 제거

        # location 변경
        if target_location and metadata.location:
            # 모든 location 패턴 변경 (location = '...', external_location = '...', LOCATION '...')
            # s3a:// 도 s3:// 로 변환
            ddl = re.sub(
                r"((?:external_)?location\s*=\s*')[^']+(')",
                rf"\g<1>{target_location}\g<2>",
                ddl,
                flags=re.IGNORECASE,
            )
            # LOCATION '...' 형태 (Hive external table)
            ddl = re.sub(
                r"(LOCATION\s+')[^']+(')",
                rf"\g<1>{target_location}\g<2>",
                ddl,
                flags=re.IGNORECASE,
            )

        return ddl

    def filter_partitions(
        self,
        metadata: TableMetadata,
        partition_filter: list[str] | None = None,
    ) -> list[dict[str, Any]]:
        """파티션 필터링

        Args:
            metadata: 테이블 메타데이터
            partition_filter: 필터 조건 (예: ["dt >= '2024-01-01'"])

        Returns:
            필터링된 파티션 목록
        """
        if not partition_filter:
            return metadata.partitions

        # 간단한 필터 파싱 및 적용
        filtered = []
        for partition in metadata.partitions:
            match = True
            for f in partition_filter:
                # 간단한 비교 연산 파싱 (예: "dt >= '2024-01-01'")
                m = re.match(r"(\w+)\s*(>=|<=|>|<|=)\s*'([^']+)'", f)
                if m:
                    col, op, val = m.groups()
                    part_val = str(partition.get(col, ""))

                    if op == ">=" and not (part_val >= val):
                        match = False
                    elif op == "<=" and not (part_val <= val):
                        match = False
                    elif op == ">" and not (part_val > val):
                        match = False
                    elif op == "<" and not (part_val < val):
                        match = False
                    elif op == "=" and not (part_val == val):
                        match = False

            if match:
                filtered.append(partition)

        return filtered
