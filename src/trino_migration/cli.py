#!/usr/bin/env python3
"""
Trino 마이그레이션 CLI
- analyze: 소스 환경 분석
- migrate: 테이블/스키마 마이그레이션
- run: YAML 설정 기반 마이그레이션
"""

from pathlib import Path

import click
from rich.console import Console
from rich.table import Table

from trino_migration.client import TrinoClient
from trino_migration.config import (
    TableMigrationConfig,
    load_yaml_config,
    settings,
)
from trino_migration.extractor import MetadataExtractor
from trino_migration.migrator import TrinoMigrator, print_summary
from trino_migration.s3_copier import S3Copier

console = Console()


@click.group()
def main():
    """Trino 데이터 마이그레이션 도구"""
    pass


@main.command()
def show_config():
    """현재 연결 설정 출력"""
    console.print("[bold]Trino 연결 설정[/bold]")
    console.print(f"\n[소스 Trino]")
    console.print(f"  Host: {settings.source.host}:{settings.source.port}")
    console.print(f"  User: {settings.source.user}")
    console.print(f"  Catalog: {settings.source.catalog}")

    console.print(f"\n[타겟 Trino]")
    console.print(f"  Host: {settings.target.host}:{settings.target.port}")
    console.print(f"  User: {settings.target.user}")
    console.print(f"  Catalog: {settings.target.catalog}")

    console.print(f"\n[S3 설정]")
    console.print(f"  AWS Profile: {settings.s3.aws_profile}")
    console.print(f"  Target Bucket: {settings.s3.target_bucket}")


@main.command()
@click.option("--host", default=None, help="소스 Trino 호스트")
@click.option("--port", type=int, default=None, help="소스 Trino 포트")
@click.option("--user", default=None, help="소스 Trino 사용자")
@click.option("--catalog", "-c", required=True, help="카탈로그 (필수)")
@click.option("--schema", "-s", "schema_name", required=True, help="분석할 스키마")
@click.option("--table", "-t", "table_name", default=None, help="특정 테이블만 분석")
@click.option("--show-ddl", is_flag=True, help="DDL 출력")
@click.option("--show-partitions", is_flag=True, help="파티션 정보 출력")
def analyze(host, port, user, catalog, schema_name, table_name, show_ddl, show_partitions):
    """소스 Trino 환경 분석

    예시:
      trino-migrate analyze -c hive -s my_schema
      trino-migrate analyze -c hive -s my_schema -t my_table --show-ddl
    """
    client = TrinoClient(
        host=host or settings.source.host,
        port=port or settings.source.port,
        user=user or settings.source.user,
        catalog=catalog,
        schema=schema_name,
    )

    extractor = MetadataExtractor(client)

    try:
        if table_name:
            # 단일 테이블 분석
            metadata = extractor.extract_table_metadata(catalog, schema_name, table_name)

            console.print(f"\n[bold]테이블: {catalog}.{schema_name}.{table_name}[/bold]")
            console.print(f"  Location: {metadata.location or 'N/A'}")
            console.print(f"  Format: {metadata.file_format or 'N/A'}")
            console.print(f"  파티션 컬럼: {', '.join(metadata.partition_columns) or 'None'}")
            console.print(f"  파티션 수: {len(metadata.partitions)}")

            if show_ddl:
                console.print(f"\n[bold]DDL:[/bold]")
                console.print(metadata.ddl)

            if show_partitions and metadata.partitions:
                console.print(f"\n[bold]파티션 목록 (최대 20개):[/bold]")
                for p in metadata.partitions[:20]:
                    console.print(f"  {p}")

        else:
            # 스키마 전체 분석
            schema_meta = extractor.extract_schema_metadata(catalog, schema_name)

            console.print(f"\n[bold]스키마: {catalog}.{schema_name}[/bold]")
            console.print(f"테이블 수: {schema_meta.table_count}")

            # 테이블 목록 테이블로 출력
            table = Table(title="테이블 목록")
            table.add_column("테이블", style="cyan")
            table.add_column("Location", style="dim")
            table.add_column("Format")
            table.add_column("파티션")

            for t in schema_meta.tables:
                partition_info = ", ".join(t.partition_columns) if t.partition_columns else "-"
                table.add_row(
                    t.table_name,
                    t.location[:50] + "..." if t.location and len(t.location) > 50 else (t.location or "N/A"),
                    t.file_format or "N/A",
                    partition_info,
                )

            console.print(table)

    finally:
        client.close()


@main.command()
@click.option("--source-host", default=None, help="소스 Trino 호스트")
@click.option("--source-port", type=int, default=None, help="소스 Trino 포트")
@click.option("--source-user", default=None, help="소스 Trino 사용자")
@click.option("--catalog", "-c", required=True, help="소스 카탈로그 (필수)")
@click.option("--target-host", default=None, help="타겟 Trino 호스트")
@click.option("--target-port", type=int, default=None, help="타겟 Trino 포트")
@click.option("--target-user", default=None, help="타겟 Trino 사용자")
@click.option("--target-catalog", default=None, help="타겟 카탈로그 (기본: 소스와 동일)")
@click.option("--schema", "-s", "schema_name", required=True, help="마이그레이션할 스키마")
@click.option("--table", "-t", "table_names", multiple=True, help="특정 테이블만 마이그레이션")
@click.option("--exclude", "-e", "exclude_tables", multiple=True, help="제외할 테이블")
@click.option("--method", type=click.Choice(["s3_copy", "insert_select"]), default="s3_copy", help="마이그레이션 방식")
@click.option("--where", "where_clause", default=None, help="WHERE 조건 (insert_select용)")
@click.option("--partition-filter", multiple=True, help="파티션 필터 (s3_copy용)")
@click.option("--target-schema", default=None, help="타겟 스키마명")
@click.option("--target-bucket", default=None, help="타겟 S3 버킷")
@click.option("--aws-profile", default=None, help="AWS 프로필")
@click.option("--parallel", type=int, default=3, help="병렬 테이블 수")
@click.option("--dry-run", is_flag=True, help="실제 마이그레이션 없이 확인만")
def migrate(
    source_host,
    source_port,
    source_user,
    catalog,
    target_host,
    target_port,
    target_user,
    target_catalog,
    schema_name,
    table_names,
    exclude_tables,
    method,
    where_clause,
    partition_filter,
    target_schema,
    target_bucket,
    aws_profile,
    parallel,
    dry_run,
):
    """스키마/테이블 마이그레이션

    예시:
      # 스키마 전체 (S3 복사)
      trino-migrate migrate -c hive -s my_schema

      # 특정 테이블만
      trino-migrate migrate -c hive -s my_schema -t users -t orders

      # INSERT SELECT 방식 + WHERE 조건
      trino-migrate migrate -c hive -s my_schema -t events --method insert_select --where "dt >= '2024-01-01'"

      # 파티션 필터
      trino-migrate migrate -c hive -s my_schema --partition-filter "dt >= '2024-01-01'"
    """
    # 타겟 카탈로그 결정 (없으면 소스와 동일)
    target_cat = target_catalog or catalog

    source_client = TrinoClient(
        host=source_host or settings.source.host,
        port=source_port or settings.source.port,
        user=source_user or settings.source.user,
        catalog=catalog,
        schema=schema_name,
    )

    target_client = TrinoClient(
        host=target_host or settings.target.host,
        port=target_port or settings.target.port,
        user=target_user or settings.target.user,
        catalog=target_cat,
        schema=target_schema or schema_name,
    )

    s3_copier = S3Copier(
        aws_profile=aws_profile or settings.s3.aws_profile,
        aws_region=settings.s3.aws_region,
        source_endpoint_url=settings.s3.source_endpoint_url,
        target_endpoint_url=settings.s3.target_endpoint_url,
    )

    migrator = TrinoMigrator(
        source_client=source_client,
        target_client=target_client,
        s3_copier=s3_copier,
        target_bucket=target_bucket or settings.s3.target_bucket,
        target_prefix=settings.s3.target_prefix,
    )

    try:
        if table_names:
            # 특정 테이블만
            from trino_migration.migrator import MigrationSummary
            summary = MigrationSummary()

            for table in table_names:
                config = TableMigrationConfig(
                    catalog=catalog,
                    schema=schema_name,
                    table=table,
                    method=method,
                    partition_filter=list(partition_filter) if partition_filter else None,
                    where=where_clause,
                    target_catalog=target_catalog,
                    target_schema=target_schema,
                )
                result = migrator.migrate_table(config, dry_run=dry_run)
                summary.results.append(result)

            print_summary(summary)

        else:
            # 스키마 전체
            summary = migrator.migrate_schema(
                catalog=catalog,
                schema=schema_name,
                target_catalog=target_catalog,
                method=method,
                exclude_tables=list(exclude_tables),
                partition_filter=list(partition_filter) if partition_filter else None,
                target_schema=target_schema,
                parallel_tables=parallel,
                dry_run=dry_run,
            )
            print_summary(summary)

    finally:
        migrator.close()


@main.command()
@click.argument("yaml_file", type=click.Path(exists=True))
@click.option("--dry-run", is_flag=True, help="실제 마이그레이션 없이 확인만")
def run(yaml_file, dry_run):
    """YAML 설정 파일로 마이그레이션 실행

    예시:
      trino-migrate run migration.yaml
      trino-migrate run migration.yaml --dry-run
    """
    # YAML 설정 로드
    yaml_config = load_yaml_config(yaml_file)

    console.print(f"YAML 설정 로드: [cyan]{yaml_file}[/cyan]")
    console.print(f"테이블 수: {len(yaml_config.tables)}")
    console.print(f"스키마 수: {len(yaml_config.schemas)}")

    if yaml_config.dry_run or dry_run:
        console.print("[yellow][DRY-RUN 모드][/yellow]")

    # 마이그레이터 생성
    migrator = TrinoMigrator.from_settings()

    # S3 버킷 오버라이드
    if yaml_config.target_bucket:
        migrator.target_bucket = yaml_config.target_bucket

    try:
        from trino_migration.migrator import MigrationSummary
        summary = MigrationSummary()

        # 개별 테이블 마이그레이션
        for table_config in yaml_config.tables:
            console.print(f"\n{'=' * 60}")
            console.print(f"[bold]{table_config.catalog}.{table_config.schema_name}.{table_config.table}[/bold]")
            console.print(f"{'=' * 60}")

            result = migrator.migrate_table(
                table_config,
                dry_run=yaml_config.dry_run or dry_run,
            )
            summary.results.append(result)

        # 스키마 단위 마이그레이션
        for schema_config in yaml_config.schemas:
            console.print(f"\n{'=' * 60}")
            console.print(f"[bold]스키마: {schema_config.catalog}.{schema_config.schema_name}[/bold]")
            console.print(f"{'=' * 60}")

            schema_summary = migrator.migrate_schema(
                catalog=schema_config.catalog,
                schema=schema_config.schema_name,
                target_catalog=schema_config.target_catalog,
                method=schema_config.method,
                exclude_tables=schema_config.exclude_tables,
                partition_filter=schema_config.partition_filter,
                target_schema=schema_config.target_schema,
                parallel_tables=yaml_config.parallel_tables,
                dry_run=yaml_config.dry_run or dry_run,
            )
            summary.results.extend(schema_summary.results)

        print_summary(summary)

    finally:
        migrator.close()


@main.command()
def init():
    """예시 YAML 설정 파일 생성"""
    example_yaml = """# Trino 마이그레이션 설정
# 사용법: trino-migrate run trino-migration.yaml

# 전역 설정
parallel_tables: 3        # 동시 테이블 처리 수
parallel_partitions: 5    # 동시 파티션 처리 수
dry_run: false

# S3 설정 (환경변수 오버라이드)
# source_bucket: source-data-lake
# target_bucket: target-data-lake

# 개별 테이블 마이그레이션
tables:
  # S3 복사 방식 (전체 데이터)
  - catalog: hive           # 카탈로그 (필수)
    schema: analytics
    table: events
    method: s3_copy

  # S3 복사 + 파티션 필터
  - catalog: hive
    schema: analytics
    table: daily_metrics
    method: s3_copy
    partition_filter:
      - "dt >= '2024-01-01'"
      - "dt <= '2024-12-31'"

  # INSERT SELECT 방식 (WHERE 조건)
  - catalog: hive
    schema: analytics
    table: users
    method: insert_select
    where: "created_at >= '2024-01-01'"

  # 타겟 카탈로그/스키마/테이블명 변경
  - catalog: hive
    schema: prod
    table: orders
    method: s3_copy
    target_catalog: iceberg  # 다른 카탈로그로 마이그레이션
    target_schema: prod_backup
    target_table: orders_2024

# 스키마 단위 마이그레이션
schemas:
  # 전체 테이블 S3 복사
  - catalog: hive           # 카탈로그 (필수)
    schema: raw_data
    method: s3_copy
    exclude:
      - temp_table
      - staging_table

  # 다른 카탈로그로 마이그레이션 + 파티션 필터
  - catalog: hive
    schema: logs
    method: s3_copy
    partition_filter:
      - "dt >= '2024-06-01'"
    target_catalog: iceberg
    target_schema: logs_archive
"""

    output_path = Path("trino-migration.yaml")
    if output_path.exists():
        if not click.confirm(f"'{output_path}'가 이미 존재합니다. 덮어쓰시겠습니까?"):
            click.echo("취소되었습니다.")
            return

    output_path.write_text(example_yaml, encoding="utf-8")
    console.print(f"예시 설정 파일 생성: [cyan]{output_path}[/cyan]")
    console.print("\n파일을 편집한 후 다음 명령어로 실행하세요:")
    console.print("  [green]trino-migrate run trino-migration.yaml[/green]")
    console.print("  [green]trino-migrate run trino-migration.yaml --dry-run[/green]")


if __name__ == "__main__":
    main()
