"""
Trino 마이그레이션 오케스트레이터
S3 복사 + 메타데이터 등록 또는 INSERT SELECT 방식 지원
"""

import asyncio
import re
from dataclasses import dataclass, field
from typing import Literal

from rich.console import Console
from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    SpinnerColumn,
    TaskProgressColumn,
    TextColumn,
    TimeElapsedColumn,
)

from trino_migration.client import TrinoClient
from trino_migration.config import TableMigrationConfig, settings
from trino_migration.extractor import MetadataExtractor, TableMetadata
from trino_migration.s3_copier import S3Copier

console = Console()


@dataclass
class MigrationResult:
    """마이그레이션 결과"""
    catalog: str
    schema_name: str
    table_name: str
    method: str
    status: Literal["success", "error", "skipped", "dry_run"]
    files_copied: int = 0
    bytes_copied: int = 0
    rows_inserted: int = 0
    partitions_migrated: int = 0
    error: str | None = None


@dataclass
class MigrationSummary:
    """마이그레이션 요약"""
    results: list[MigrationResult] = field(default_factory=list)

    @property
    def total_tables(self) -> int:
        return len(self.results)

    @property
    def success_count(self) -> int:
        return len([r for r in self.results if r.status == "success"])

    @property
    def error_count(self) -> int:
        return len([r for r in self.results if r.status == "error"])

    @property
    def skipped_count(self) -> int:
        return len([r for r in self.results if r.status == "skipped"])

    @property
    def total_files(self) -> int:
        return sum(r.files_copied for r in self.results)

    @property
    def total_bytes(self) -> int:
        return sum(r.bytes_copied for r in self.results)


class TrinoMigrator:
    """Trino 마이그레이션 오케스트레이터"""

    def __init__(
        self,
        source_client: TrinoClient,
        target_client: TrinoClient,
        s3_copier: S3Copier,
        target_bucket: str,
        target_prefix: str = "",
    ):
        self.source_client = source_client
        self.target_client = target_client
        self.s3_copier = s3_copier
        self.target_bucket = target_bucket
        self.target_prefix = target_prefix
        self.extractor = MetadataExtractor(source_client)

    @classmethod
    def from_settings(cls) -> "TrinoMigrator":
        """설정에서 인스턴스 생성"""
        source_client = TrinoClient(
            host=settings.source.host,
            port=settings.source.port,
            user=settings.source.user,
            catalog=settings.source.catalog,
            schema=settings.source.schema_,
        )

        target_client = TrinoClient(
            host=settings.target.host,
            port=settings.target.port,
            user=settings.target.user,
            catalog=settings.target.catalog,
            schema=settings.target.schema_,
        )

        s3_copier = S3Copier(
            aws_profile=settings.s3.aws_profile,
            aws_region=settings.s3.aws_region,
            source_endpoint_url=settings.s3.source_endpoint_url,
            target_endpoint_url=settings.s3.target_endpoint_url,
        )

        return cls(
            source_client=source_client,
            target_client=target_client,
            s3_copier=s3_copier,
            target_bucket=settings.s3.target_bucket,
            target_prefix=settings.s3.target_prefix,
        )

    def migrate_table_s3_copy(
        self,
        metadata: TableMetadata,
        target_catalog: str,
        target_schema: str | None = None,
        target_table: str | None = None,
        partition_filter: list[str] | None = None,
        dry_run: bool = False,
    ) -> MigrationResult:
        """S3 복사 방식으로 테이블 마이그레이션"""
        schema = target_schema or metadata.schema_name
        table = target_table or metadata.table_name

        console.print(f"\n[bold cyan]S3 복사: {metadata.catalog}.{metadata.schema_name}.{metadata.table_name} → {target_catalog}.{schema}.{table}[/bold cyan]")

        if not metadata.location:
            return MigrationResult(
                catalog=target_catalog,
                schema_name=schema,
                table_name=table,
                method="s3_copy",
                status="error",
                error="소스 테이블에 S3 location이 없습니다",
            )

        source_bucket = metadata.s3_bucket
        source_prefix = metadata.s3_prefix

        if not source_bucket or not source_prefix:
            return MigrationResult(
                catalog=target_catalog,
                schema_name=schema,
                table_name=table,
                method="s3_copy",
                status="error",
                error=f"S3 경로 파싱 실패: {metadata.location}",
            )

        # 타겟 경로 생성
        target_prefix = f"{self.target_prefix}/{schema}/{table}".lstrip("/")
        target_location = f"s3://{self.target_bucket}/{target_prefix}"

        console.print(f"  소스: s3://{source_bucket}/{source_prefix}")
        console.print(f"  타겟: {target_location}")

        total_files = 0
        total_bytes = 0
        partitions_migrated = 0

        if metadata.is_partitioned:
            # 파티션 필터링
            partitions = self.extractor.filter_partitions(metadata, partition_filter)
            console.print(f"  파티션: {len(partitions)}개")

            if partitions:
                results = self.s3_copier.copy_partitions(
                    source_bucket=source_bucket,
                    source_base_prefix=source_prefix,
                    target_bucket=self.target_bucket,
                    target_base_prefix=target_prefix,
                    partitions=partitions,
                    partition_columns=metadata.partition_columns,
                    dry_run=dry_run,
                )

                for r in results:
                    total_files += r.files_copied
                    total_bytes += r.bytes_copied
                    if r.status in ("success", "dry_run"):
                        partitions_migrated += 1
        else:
            # 비파티션 테이블
            result = self.s3_copier.copy_prefix(
                source_bucket=source_bucket,
                source_prefix=source_prefix,
                target_bucket=self.target_bucket,
                target_prefix=target_prefix,
                dry_run=dry_run,
            )
            total_files = result.files_copied
            total_bytes = result.bytes_copied

        # 타겟에 테이블 생성
        if not dry_run:
            try:
                # 스키마 생성
                if not self.target_client.schema_exists(schema, target_catalog):
                    schema_location = f"s3://{self.target_bucket}/{self.target_prefix}/{schema}".lstrip("/")
                    self.target_client.create_schema(schema, schema_location, target_catalog)
                    console.print(f"  [green]스키마 생성: {target_catalog}.{schema}[/green]")

                # DDL 생성 및 실행
                target_ddl = self.extractor.generate_target_ddl(
                    metadata,
                    target_catalog=target_catalog,
                    target_schema=schema,
                    target_table=table,
                    target_location=target_location,
                )

                # 기존 테이블 존재 시 DROP
                if self.target_client.table_exists(schema, table, target_catalog):
                    self.target_client.execute(
                        f"DROP TABLE {target_catalog}.{schema}.{table}",
                        fetch=False,
                    )

                self.target_client.execute(target_ddl, fetch=False)
                console.print(f"  [green]테이블 생성 완료[/green]")

                # 파티션 복구 (MSCK REPAIR TABLE)
                if metadata.is_partitioned:
                    try:
                        self.target_client.execute(
                            f"CALL {target_catalog}.system.sync_partition_metadata('{schema}', '{table}', 'FULL')",
                            fetch=False,
                        )
                        console.print(f"  [green]파티션 동기화 완료[/green]")
                    except Exception as e:
                        console.print(f"  [yellow]파티션 동기화 경고: {e}[/yellow]")

            except Exception as e:
                return MigrationResult(
                    catalog=target_catalog,
                    schema_name=schema,
                    table_name=table,
                    method="s3_copy",
                    status="error",
                    files_copied=total_files,
                    bytes_copied=total_bytes,
                    partitions_migrated=partitions_migrated,
                    error=str(e),
                )

        status = "dry_run" if dry_run else "success"
        console.print(f"  [green]완료: {total_files}개 파일, {total_bytes / 1024 / 1024:.2f} MB[/green]")

        return MigrationResult(
            catalog=target_catalog,
            schema_name=schema,
            table_name=table,
            method="s3_copy",
            status=status,
            files_copied=total_files,
            bytes_copied=total_bytes,
            partitions_migrated=partitions_migrated,
        )

    def migrate_table_insert_select(
        self,
        metadata: TableMetadata,
        target_catalog: str,
        target_schema: str | None = None,
        target_table: str | None = None,
        where: str | None = None,
        dry_run: bool = False,
    ) -> MigrationResult:
        """INSERT SELECT 방식으로 테이블 마이그레이션"""
        schema = target_schema or metadata.schema_name
        table = target_table or metadata.table_name

        console.print(f"\n[bold cyan]INSERT SELECT: {metadata.catalog}.{metadata.schema_name}.{metadata.table_name} → {target_catalog}.{schema}.{table}[/bold cyan]")

        if where:
            console.print(f"  WHERE: {where}")

        # row count 조회
        try:
            source_count = self.source_client.get_row_count(
                metadata.schema_name, metadata.table_name, where, catalog=metadata.catalog
            )
            console.print(f"  소스 row count: {source_count:,}")
        except Exception as e:
            console.print(f"  [yellow]row count 조회 실패: {e}[/yellow]")
            source_count = 0

        if dry_run:
            console.print(f"  [yellow][DRY-RUN] {source_count:,}건 마이그레이션 예정[/yellow]")
            return MigrationResult(
                catalog=target_catalog,
                schema_name=schema,
                table_name=table,
                method="insert_select",
                status="dry_run",
                rows_inserted=source_count,
            )

        try:
            # 타겟 스키마 생성
            if not self.target_client.schema_exists(schema, target_catalog):
                schema_location = f"s3://{self.target_bucket}/{self.target_prefix}/{schema}".lstrip("/")
                self.target_client.create_schema(schema, schema_location, target_catalog)
                console.print(f"  [green]스키마 생성: {target_catalog}.{schema}[/green]")

            # 타겟 테이블 생성 (CTAS 또는 CREATE + INSERT)
            target_location = f"s3://{self.target_bucket}/{self.target_prefix}/{schema}/{table}"

            source_full = f"{metadata.catalog}.{metadata.schema_name}.{metadata.table_name}"
            target_full = f"{target_catalog}.{schema}.{table}"

            # 기존 테이블 DROP
            if self.target_client.table_exists(schema, table, target_catalog):
                self.target_client.execute(f"DROP TABLE {target_full}", fetch=False)

            # CTAS로 생성
            ctas_query = f"""
                CREATE TABLE {target_full}
                WITH (
                    external_location = '{target_location}',
                    format = '{metadata.file_format or 'PARQUET'}'
                )
                AS SELECT * FROM {source_full}
            """
            if where:
                ctas_query += f" WHERE {where}"

            console.print(f"  [dim]CTAS 실행 중...[/dim]")
            self.target_client.execute(ctas_query, fetch=False)

            # 결과 row count 확인
            target_count = self.target_client.get_row_count(schema, table, catalog=target_catalog)
            console.print(f"  [green]완료: {target_count:,}건 삽입[/green]")

            return MigrationResult(
                catalog=target_catalog,
                schema_name=schema,
                table_name=table,
                method="insert_select",
                status="success",
                rows_inserted=target_count,
            )

        except Exception as e:
            return MigrationResult(
                catalog=target_catalog,
                schema_name=schema,
                table_name=table,
                method="insert_select",
                status="error",
                error=str(e),
            )

    def migrate_table(
        self,
        config: TableMigrationConfig,
        dry_run: bool = False,
    ) -> MigrationResult | None:
        """테이블 마이그레이션 (설정 기반)"""
        # 타겟 카탈로그 결정 (없으면 소스와 동일)
        target_catalog = config.target_catalog or config.catalog

        # 메타데이터 추출
        metadata = self.extractor.extract_table_metadata(
            config.catalog, config.schema_name, config.table
        )

        # VIEW/MATERIALIZED VIEW는 스킵
        if metadata is None:
            return MigrationResult(
                catalog=target_catalog,
                schema_name=config.schema_name,
                table_name=config.table,
                method=config.method,
                status="skipped",
                error="VIEW 또는 MATERIALIZED VIEW는 마이그레이션 불가",
            )

        if config.method == "s3_copy":
            return self.migrate_table_s3_copy(
                metadata=metadata,
                target_catalog=target_catalog,
                target_schema=config.target_schema,
                target_table=config.target_table,
                partition_filter=config.partition_filter,
                dry_run=dry_run,
            )
        else:
            return self.migrate_table_insert_select(
                metadata=metadata,
                target_catalog=target_catalog,
                target_schema=config.target_schema,
                target_table=config.target_table,
                where=config.where,
                dry_run=dry_run,
            )

    def migrate_schema(
        self,
        catalog: str,
        schema: str,
        target_catalog: str | None = None,
        method: Literal["s3_copy", "insert_select"] = "s3_copy",
        exclude_tables: list[str] | None = None,
        partition_filter: list[str] | None = None,
        target_schema: str | None = None,
        parallel_tables: int = 3,
        dry_run: bool = False,
    ) -> MigrationSummary:
        """스키마 전체 마이그레이션"""
        target_cat = target_catalog or catalog
        console.print(f"\n[bold]스키마 마이그레이션: {catalog}.{schema} → {target_cat}.{target_schema or schema}[/bold]")
        console.print(f"  방식: {method}")
        console.print(f"  병렬 처리: {parallel_tables}개")

        # 스키마 메타데이터 추출
        schema_meta = self.extractor.extract_schema_metadata(
            catalog, schema, exclude_tables
        )

        summary = MigrationSummary()

        progress = Progress(
            SpinnerColumn(),
            TextColumn("[bold]{task.description}"),
            BarColumn(),
            MofNCompleteColumn(),
            TimeElapsedColumn(),
            console=console,
        )

        with progress:
            task = progress.add_task("테이블 마이그레이션", total=len(schema_meta.tables))

            for table_meta in schema_meta.tables:
                if method == "s3_copy":
                    result = self.migrate_table_s3_copy(
                        metadata=table_meta,
                        target_catalog=target_cat,
                        target_schema=target_schema,
                        partition_filter=partition_filter,
                        dry_run=dry_run,
                    )
                else:
                    result = self.migrate_table_insert_select(
                        metadata=table_meta,
                        target_catalog=target_cat,
                        target_schema=target_schema,
                        dry_run=dry_run,
                    )

                summary.results.append(result)
                progress.advance(task)

        return summary

    def close(self):
        """리소스 정리"""
        self.source_client.close()
        self.target_client.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


def print_summary(summary: MigrationSummary):
    """마이그레이션 요약 출력"""
    console.print("\n" + "=" * 60)
    console.print("[bold]마이그레이션 요약[/bold]")
    console.print("=" * 60)

    console.print(f"총 테이블: {summary.total_tables}")
    console.print(f"성공: [green]{summary.success_count}[/green]")
    console.print(f"실패: [red]{summary.error_count}[/red]")
    if summary.skipped_count > 0:
        console.print(f"스킵: [yellow]{summary.skipped_count}[/yellow]")

    if summary.total_files > 0:
        console.print(f"총 파일: {summary.total_files:,}")
        console.print(f"총 크기: {summary.total_bytes / 1024 / 1024:.2f} MB")

    total_rows = sum(r.rows_inserted for r in summary.results)
    if total_rows > 0:
        console.print(f"총 row: {total_rows:,}")

    # 실패 목록
    errors = [r for r in summary.results if r.status == "error"]
    if errors:
        console.print("\n[red]실패 목록:[/red]")
        for r in errors:
            console.print(f"  - {r.catalog}.{r.schema_name}.{r.table_name}: {r.error}")
