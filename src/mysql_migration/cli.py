#!/usr/bin/env python3
"""
MySQL 데이터 마이그레이션 CLI (비동기 버전)
- YAML 설정 파일로 여러 DB를 병렬 마이그레이션
- CLI 인자로 단일 DB 마이그레이션
"""

import asyncio
import json
import re
import sys
from functools import wraps
from pathlib import Path

import click
from rich.console import Console

from mysql_migration.config import (
    settings,
    load_yaml_config,
    DatabaseConfig,
    DynamicDatabaseConfig,
)
from mysql_migration.migrator import (
    AsyncMySQLMigrator,
    normalize_where,
    is_date_suffixed_table,
    execute_lookup_query,
    build_user_id_where,
)

console = Console()


def async_command(f):
    """Click 명령어를 async로 실행하는 데코레이터"""
    @wraps(f)
    def wrapper(*args, **kwargs):
        return asyncio.run(f(*args, **kwargs))
    return wrapper


@click.group()
def main():
    """MySQL 데이터 마이그레이션 도구"""
    pass


@main.command()
def show_config():
    """현재 DB 연결 설정 출력"""
    click.echo("현재 DB 연결 설정 (환경변수 기반):")
    click.echo(f"\n[소스 DB]")
    click.echo(f"  Host: {settings.source.host}")
    click.echo(f"  Port: {settings.source.port}")
    click.echo(f"  User: {settings.source.user}")
    click.echo(f"\n[타겟 DB]")
    click.echo(f"  Host: {settings.target.host}")
    click.echo(f"  Port: {settings.target.port}")
    click.echo(f"  User: {settings.target.user}")


async def migrate_single_database(
    source_config: dict,
    target_config: dict,
    db_config: DatabaseConfig,
    auto_order: bool,
    truncate: bool,
    create_tables: bool,
    dry_run: bool,
    exclude_date_tables: bool = True,
    max_table_workers: int = 5,
) -> list[dict]:
    """단일 데이터베이스 마이그레이션 (비동기)"""
    source_db = db_config.name
    target_db = db_config.target_name or db_config.name

    # DB별 설정 적용
    source_config = {**source_config, "database": source_db}
    target_config = {**target_config, "database": target_db}

    migrator = AsyncMySQLMigrator(source_config, target_config)

    try:
        # 타겟 DB 존재 여부 확인
        if not await migrator.check_database_exists(target_config):
            console.print(f"\n타겟 데이터베이스 '[cyan]{target_db}[/cyan]'가 존재하지 않습니다.")
            # 비동기 환경에서는 자동 생성 (click.confirm 대신)
            console.print(f"데이터베이스 '[cyan]{target_db}[/cyan]' 생성 중...")
            try:
                await migrator.create_database(target_config)
                console.print(f"데이터베이스 '[green]{target_db}[/green]' 생성 완료")
            except Exception as e:
                console.print(f"[red]데이터베이스 생성 실패: {e}[/red]")
                return []

        # 테이블 설정 생성
        if db_config.mode == "tables" and db_config.tables:
            # 지정된 테이블만
            tables_config = [
                {
                    "name": t.name,
                    "where": normalize_where(t.where or db_config.where),
                    "limit": t.limit or db_config.limit,
                }
                for t in db_config.tables
            ]
        else:
            # 전체 테이블 (all 모드)
            console.print(f"소스 DB '[cyan]{source_db}[/cyan]'에서 테이블 목록 조회 중...")
            all_tables = await migrator.get_all_tables()
            total_count = len(all_tables)

            # 날짜 suffix 테이블 제외 (기본: True)
            if exclude_date_tables:
                date_tables = [t for t in all_tables if is_date_suffixed_table(t)]
                if date_tables:
                    all_tables = [t for t in all_tables if not is_date_suffixed_table(t)]
                    console.print(f"  날짜 테이블 제외: {len(date_tables)}개")

            # 명시적 제외 테이블 필터링
            if db_config.exclude:
                exclude_set = set(db_config.exclude)
                before_exclude = len(all_tables)
                all_tables = [t for t in all_tables if t not in exclude_set]
                console.print(f"  명시적 제외: {before_exclude - len(all_tables)}개")

            console.print(f"  마이그레이션 대상: {len(all_tables)}/{total_count}개 테이블")

            tables_config = [
                {
                    "name": table,
                    "where": normalize_where(db_config.where),
                    "limit": db_config.limit,
                }
                for table in all_tables
            ]

        # laplace 모드: user_id 컬럼 유무에 따라 WHERE 조건 동적 생성
        if db_config.laplace_mode and db_config.user_ids:
            console.print(f"\n[bold magenta][Laplace 모드][/bold magenta]")
            console.print(f"  필터링 user_id: {db_config.user_ids}")

            # 각 테이블의 user_id 컬럼 유무 확인
            table_names = [cfg["name"] for cfg in tables_config]
            user_id_info = await migrator.get_tables_with_user_id_info(table_names)

            tables_with_user_id = [t for t, has in user_id_info.items() if has]
            tables_without_user_id = [t for t, has in user_id_info.items() if not has]

            console.print(f"  user_id 컬럼 있음: {len(tables_with_user_id)}개 테이블 → 필터링 적용")
            console.print(f"  user_id 컬럼 없음: {len(tables_without_user_id)}개 테이블 → 전체 마이그레이션")

            # WHERE 조건 업데이트
            for cfg in tables_config:
                table_name = cfg["name"]
                if user_id_info.get(table_name, False):
                    # user_id가 있는 테이블: user_id 필터링 추가
                    cfg["where"] = build_user_id_where(db_config.user_ids, cfg["where"])
                # user_id가 없는 테이블은 기존 where 조건 유지

        if not tables_config:
            console.print("[yellow]마이그레이션할 테이블이 없습니다.[/yellow]")
            return []

        # 설정 출력
        console.print("=" * 60)
        console.print(f"[bold][{source_db}] -> [{target_db}][/bold]")
        console.print("=" * 60)
        console.print(f"소스: {source_config['host']}:{source_config['port']}/{source_db}")
        console.print(f"타겟: {target_config['host']}:{target_config['port']}/{target_db}")
        console.print(f"테이블 수: {len(tables_config)}")
        console.print(f"자동 순서 정렬: {'활성' if auto_order else '비활성'}")
        console.print(f"테이블 병렬 처리: {max_table_workers}개")
        console.print("=" * 60)

        # 외래키 기반 자동 순서 정렬
        if auto_order and len(tables_config) > 1:
            tables_config = await migrator.sort_tables_by_fk(tables_config)

        if dry_run:
            console.print("\n[yellow][DRY-RUN] 마이그레이션 설정:[/yellow]")
            for i, cfg in enumerate(tables_config, 1):
                where_display = "전체 데이터" if cfg.get("where") == "1=1" else cfg.get("where")
                console.print(f"  {i}. {cfg['name']}")
                console.print(f"     WHERE: {where_display}")
                if cfg.get("limit"):
                    console.print(f"     LIMIT: {cfg.get('limit')}")
            return []

        # 마이그레이션 실행
        results = await migrator.migrate_all(
            tables_config,
            create_tables=create_tables,
            truncate=truncate,
            max_table_workers=max_table_workers,
        )

        return results

    finally:
        await migrator.close()


def print_results(results: list[dict]):
    """결과 출력"""
    if not results:
        return

    console.print("\n" + "=" * 60)
    console.print("[bold]마이그레이션 결과[/bold]")
    console.print("=" * 60)

    total_fetched = 0
    total_inserted = 0
    total_skipped = 0
    errors = []

    for r in results:
        if r["status"] == "success":
            status_icon = "[green]OK[/green]"
        elif r["status"] == "warning":
            status_icon = "[yellow]WN[/yellow]"
        else:
            status_icon = "[red]NG[/red]"

        skip_info = f" (스킵: {r['skipped']})" if r.get("skipped") else ""
        console.print(f"[{status_icon}] {r['table']}: {r['inserted']}/{r['fetched']}건{skip_info}")

        total_fetched += r["fetched"]
        total_inserted += r["inserted"]
        total_skipped += r.get("skipped", 0)

        if r.get("errors"):
            for err in r["errors"][:2]:
                errors.append(f"  - {r['table']}: {err}")

    console.print("-" * 60)
    console.print(f"총계: {total_inserted}/{total_fetched}건 마이그레이션")
    if total_skipped:
        console.print(f"스킵: {total_skipped}건")

    if errors:
        console.print("\n[red]오류 목록:[/red]")
        for e in errors:
            console.print(e)


async def expand_dynamic_databases(
    dynamic_config: DynamicDatabaseConfig,
    source_base: dict,
) -> list[DatabaseConfig]:
    """동적 DB 설정을 실제 DatabaseConfig 목록으로 확장 (비동기)

    Args:
        dynamic_config: 패턴 기반 동적 DB 설정
        source_base: 소스 DB 연결 설정

    Returns:
        확장된 DatabaseConfig 목록
    """
    # lookup 쿼리 실행 (비동기)
    lookup = dynamic_config.lookup_query
    ids = await execute_lookup_query(source_base, lookup.database, lookup.sql)

    if not ids:
        console.print(f"  [yellow]경고: lookup 쿼리 결과가 없습니다.[/yellow]")
        return []

    console.print(f"  조회된 ID: {len(ids)}개")

    # 패턴에서 placeholder 추출 (예: "laplacian_{user_id}" -> "user_id")
    match = re.search(r"\{(\w+)\}", dynamic_config.pattern)
    if not match:
        console.print(f"  [red]오류: 패턴에 {{placeholder}}가 없습니다: {dynamic_config.pattern}[/red]")
        return []

    placeholder = match.group(1)

    # 각 ID에 대해 DatabaseConfig 생성
    configs = []
    for id_value in ids:
        db_name = dynamic_config.pattern.replace(f"{{{placeholder}}}", str(id_value))
        target_name = None
        if dynamic_config.target_pattern:
            target_name = dynamic_config.target_pattern.replace(f"{{{placeholder}}}", str(id_value))

        configs.append(DatabaseConfig(
            name=db_name,
            target_name=target_name,
            mode=dynamic_config.mode,
            exclude=dynamic_config.exclude,
            tables=dynamic_config.tables,
            where=dynamic_config.where,
            limit=dynamic_config.limit,
            exclude_date_tables=dynamic_config.exclude_date_tables,
        ))

    return configs


@main.command()
@click.argument("yaml_file", type=click.Path(exists=True))
@click.option("--dry-run", is_flag=True, help="실제 마이그레이션 없이 설정만 확인")
@click.option("--parallel/--no-parallel", default=True, help="DB 병렬 처리 (기본: 활성)")
@click.option("--max-workers", type=int, default=None, help="동시 DB 처리 수 (기본: YAML 설정 또는 3)")
@click.option("--max-table-workers", type=int, default=None, help="DB당 동시 테이블 처리 수 (기본: YAML 설정 또는 5)")
@async_command
async def run(yaml_file, dry_run, parallel, max_workers, max_table_workers):
    """YAML 설정 파일로 마이그레이션 실행 (병렬 처리 지원)

    예시:
      data-migrate run migration.yaml
      data-migrate run migration.yaml --max-workers 5
      data-migrate run migration.yaml --no-parallel
    """
    # YAML 설정 로드
    yaml_config = load_yaml_config(yaml_file)

    if not yaml_config.databases and not yaml_config.dynamic_databases:
        console.print("[red]오류: YAML 파일에 databases 또는 dynamic_databases가 정의되지 않았습니다.[/red]")
        sys.exit(1)

    # CLI 옵션으로 YAML 설정 오버라이드
    use_parallel = parallel and yaml_config.parallel
    workers = max_workers or yaml_config.max_workers
    table_workers = max_table_workers or yaml_config.max_table_workers

    console.print(f"YAML 설정 로드: [cyan]{yaml_file}[/cyan]")
    console.print(f"병렬 처리: {'[green]활성[/green]' if use_parallel else '[yellow]비활성[/yellow]'}")
    if use_parallel:
        console.print(f"동시 DB 처리: {workers}개")
        console.print(f"DB당 테이블 병렬: {table_workers}개")

    # 기본 연결 설정
    source_base = settings.source.to_dict()
    target_base = settings.target.to_dict()

    # 정적 DB 설정과 동적 DB 설정을 합침
    all_db_configs: list[DatabaseConfig] = list(yaml_config.databases)

    # 동적 DB 설정 확장
    if yaml_config.dynamic_databases:
        console.print(f"\n[bold cyan][동적 데이터베이스 확장][/bold cyan]")
        for ddb in yaml_config.dynamic_databases:
            console.print(f"  패턴: {ddb.pattern}")
            console.print(f"  쿼리: {ddb.lookup_query.sql[:50]}...")
            expanded = await expand_dynamic_databases(ddb, source_base)
            all_db_configs.extend(expanded)

    console.print(f"\n대상 데이터베이스: [bold]{len(all_db_configs)}개[/bold]")

    all_results = []

    if use_parallel and len(all_db_configs) > 1:
        # 병렬 처리
        semaphore = asyncio.Semaphore(workers)

        async def migrate_with_semaphore(db_config: DatabaseConfig) -> tuple[str, list[dict]]:
            async with semaphore:
                console.print(f"\n{'#' * 60}")
                console.print(f"# 데이터베이스: [bold cyan]{db_config.name}[/bold cyan]")
                console.print(f"{'#' * 60}")

                exclude_date = db_config.exclude_date_tables if db_config.exclude_date_tables is not None else yaml_config.exclude_date_tables

                try:
                    results = await migrate_single_database(
                        source_config=source_base,
                        target_config=target_base,
                        db_config=db_config,
                        auto_order=yaml_config.auto_order,
                        truncate=yaml_config.truncate,
                        create_tables=yaml_config.create_tables,
                        dry_run=dry_run,
                        exclude_date_tables=exclude_date,
                        max_table_workers=table_workers,
                    )
                    return db_config.name, results
                except Exception as e:
                    console.print(f"[red]오류 ({db_config.name}): {e}[/red]")
                    return db_config.name, []

        # 모든 DB 병렬 실행
        db_results = await asyncio.gather(
            *[migrate_with_semaphore(db) for db in all_db_configs],
            return_exceptions=True
        )

        for res in db_results:
            if isinstance(res, Exception):
                console.print(f"[red]예외 발생: {res}[/red]")
            else:
                db_name, results = res
                all_results.extend(results)

    else:
        # 순차 처리
        for db_config in all_db_configs:
            console.print(f"\n{'#' * 60}")
            console.print(f"# 데이터베이스: [bold cyan]{db_config.name}[/bold cyan]")
            console.print(f"{'#' * 60}")

            exclude_date = db_config.exclude_date_tables if db_config.exclude_date_tables is not None else yaml_config.exclude_date_tables

            results = await migrate_single_database(
                source_config=source_base,
                target_config=target_base,
                db_config=db_config,
                auto_order=yaml_config.auto_order,
                truncate=yaml_config.truncate,
                create_tables=yaml_config.create_tables,
                dry_run=dry_run,
                exclude_date_tables=exclude_date,
                max_table_workers=table_workers,
            )
            all_results.extend(results)

    # 전체 결과 출력
    if all_results:
        print_results(all_results)


@main.command()
@click.option("--source-host", default=None, help="소스 DB 호스트")
@click.option("--source-port", type=int, default=None, help="소스 DB 포트")
@click.option("--source-user", default=None, help="소스 DB 사용자")
@click.option("--source-password", default=None, help="소스 DB 비밀번호")
@click.option("--target-host", default=None, help="타겟 DB 호스트")
@click.option("--target-port", type=int, default=None, help="타겟 DB 포트")
@click.option("--target-user", default=None, help="타겟 DB 사용자")
@click.option("--target-password", default=None, help="타겟 DB 비밀번호")
@click.option("--database", "-d", required=True, help="마이그레이션할 데이터베이스명")
@click.option("--target-database", default=None, help="타겟 데이터베이스명 (기본: 소스와 동일)")
@click.option("--config", "config_file", type=click.Path(exists=True), help="테이블 설정 JSON 파일")
@click.option("--all", "migrate_all_tables", is_flag=True, help="DB의 모든 테이블 마이그레이션")
@click.option("--table", "tables", multiple=True, help="마이그레이션할 테이블 (여러 개 지정 가능)")
@click.option("--exclude", "exclude_tables", multiple=True, help="제외할 테이블 (--all 사용 시)")
@click.option("--where", "where_clause", help="WHERE 조건")
@click.option("--limit", type=int, help="조회 제한 건수")
@click.option("--truncate", is_flag=True, help="삽입 전 테이블 초기화")
@click.option("--no-create-table", is_flag=True, help="테이블 생성 건너뛰기")
@click.option("--auto-order/--no-auto-order", default=True, help="외래키 기반 자동 순서 정렬 (기본: 활성)")
@click.option("--exclude-date-tables/--include-date-tables", default=True, help="날짜 suffix 테이블 제외 (기본: 제외)")
@click.option("--max-table-workers", type=int, default=5, help="테이블 병렬 처리 수 (기본: 5)")
@click.option("--laplace-mode", is_flag=True, help="Laplace 모드: user_id 컬럼 유무에 따라 자동 필터링")
@click.option("--user-ids", "user_ids_str", default=None, help="Laplace 모드에서 필터링할 user_id 목록 (콤마 구분)")
@click.option("--dry-run", is_flag=True, help="실제 마이그레이션 없이 설정만 확인")
@async_command
async def migrate(
    source_host,
    source_port,
    source_user,
    source_password,
    target_host,
    target_port,
    target_user,
    target_password,
    database,
    target_database,
    config_file,
    migrate_all_tables,
    tables,
    exclude_tables,
    where_clause,
    limit,
    truncate,
    no_create_table,
    auto_order,
    exclude_date_tables,
    max_table_workers,
    laplace_mode,
    user_ids_str,
    dry_run,
):
    """단일 데이터베이스 마이그레이션 (CLI 인자 사용)

    예시:
      data-migrate migrate -d laplace --all
      data-migrate migrate -d laplace --table users --table orders
      data-migrate migrate -d laplace --all --exclude large_logs
      data-migrate migrate -d laplace --all --max-table-workers 10
      data-migrate migrate -d laplace --all --laplace-mode --user-ids 1,2,3
    """
    # user_ids 파싱
    user_ids = None
    if user_ids_str:
        try:
            user_ids = [int(uid.strip()) for uid in user_ids_str.split(",")]
        except ValueError:
            console.print("[red]오류: --user-ids는 콤마로 구분된 정수 목록이어야 합니다.[/red]")
            sys.exit(1)

    # laplace 모드 검증
    if laplace_mode and not user_ids:
        console.print("[red]오류: --laplace-mode 사용 시 --user-ids가 필요합니다.[/red]")
        sys.exit(1)

    # 테이블 지정 방식 확인
    if not config_file and not migrate_all_tables and not tables:
        console.print("[red]오류: 마이그레이션할 테이블을 지정해야 합니다.[/red]")
        console.print("  --all                              # DB 전체 테이블")
        console.print("  --all --exclude <테이블>           # 특정 테이블 제외")
        console.print("  --config <config.json>             # JSON 설정 파일")
        console.print("  --table <테이블명>                 # 특정 테이블 지정")
        sys.exit(1)

    # DatabaseConfig 생성
    if config_file:
        with open(config_file) as f:
            config = json.load(f)
        from mysql_migration.config import TableConfig
        db_config = DatabaseConfig(
            name=database,
            target_name=target_database,
            mode="tables",
            tables=[TableConfig(**t) if isinstance(t, dict) else TableConfig(name=t)
                    for t in config.get("tables", [])],
            laplace_mode=laplace_mode,
            user_ids=user_ids,
        )
    elif tables:
        from mysql_migration.config import TableConfig
        db_config = DatabaseConfig(
            name=database,
            target_name=target_database,
            mode="tables",
            tables=[TableConfig(name=t, where=where_clause or "", limit=limit) for t in tables],
            laplace_mode=laplace_mode,
            user_ids=user_ids,
        )
    else:
        db_config = DatabaseConfig(
            name=database,
            target_name=target_database,
            mode="all",
            exclude=list(exclude_tables),
            where=where_clause or "",
            limit=limit,
            laplace_mode=laplace_mode,
            user_ids=user_ids,
        )

    # 연결 설정
    source_config = {
        "host": source_host or settings.source.host,
        "port": source_port or settings.source.port,
        "user": source_user or settings.source.user,
        "password": source_password or settings.source.password,
    }

    target_config = {
        "host": target_host or settings.target.host,
        "port": target_port or settings.target.port,
        "user": target_user or settings.target.user,
        "password": target_password or settings.target.password,
    }

    results = await migrate_single_database(
        source_config=source_config,
        target_config=target_config,
        db_config=db_config,
        auto_order=auto_order,
        truncate=truncate,
        create_tables=not no_create_table,
        dry_run=dry_run,
        exclude_date_tables=exclude_date_tables,
        max_table_workers=max_table_workers,
    )

    print_results(results)


@main.command()
def init():
    """예시 YAML 설정 파일 생성"""
    example_yaml = """# 마이그레이션 설정 파일
# 사용법: data-migrate run migration.yaml

# 전역 설정
auto_order: true           # 외래키 기반 자동 순서 정렬
truncate: false            # 삽입 전 테이블 초기화
create_tables: true        # 테이블 자동 생성
exclude_date_tables: true  # 날짜 suffix 테이블 제외 (예: table_20240101)

# 병렬 처리 설정
parallel: true             # DB 병렬 처리 활성화
max_workers: 3             # 동시 DB 처리 수
max_table_workers: 5       # DB당 동시 테이블 처리 수

# 마이그레이션 대상 데이터베이스 목록
databases:
  # 예시 1: 전체 테이블 마이그레이션
  - name: laplace
    mode: all
    exclude:
      - large_log_table
      - temp_table

  # 예시 2: 특정 테이블만 마이그레이션
  - name: analytics
    target_name: analytics_backup  # 다른 이름의 타겟 DB로 마이그레이션
    mode: tables
    tables:
      - users
      - orders
      - name: events
        where: "created_at >= '2024-01-01'"
        limit: 10000

  # 예시 3: 전체 테이블에 조건 적용
  - name: archive
    mode: all
    where: "updated_at >= '2024-01-01'"
    limit: 50000
    exclude:
      - deleted_records

  # 예시 4: 날짜 테이블 포함 마이그레이션
  - name: logs
    mode: all
    exclude_date_tables: false  # DB별로 설정 오버라이드 가능

  # 예시 5: Laplace 모드 - user_id 기반 자동 필터링
  # user_id 컬럼이 있는 테이블은 지정된 user_id만 필터링
  # user_id 컬럼이 없는 테이블은 전체 데이터 마이그레이션
  - name: laplace
    mode: all
    laplace_mode: true
    user_ids:
      - 1
      - 2
      - 3
    exclude:
      - large_log_table

# 동적 데이터베이스 (쿼리 결과로 DB명 생성)
dynamic_databases:
  # 예시: user_subscription 테이블에서 조건에 맞는 user_id 조회 후
  # laplacian_{user_id} 데이터베이스들을 마이그레이션
  - pattern: "laplacian_{user_id}"
    lookup_query:
      database: laplace
      sql: |
        SELECT user_id
        FROM user_subscription
        WHERE plan_end_datetime_id > '2025-12-05 00:00:00'
    mode: all
    exclude:
      - large_log_table
"""

    output_path = Path("migration.yaml")
    if output_path.exists():
        if not click.confirm(f"'{output_path}'가 이미 존재합니다. 덮어쓰시겠습니까?"):
            click.echo("취소되었습니다.")
            return

    output_path.write_text(example_yaml, encoding="utf-8")
    console.print(f"예시 설정 파일 생성: [cyan]{output_path}[/cyan]")
    console.print("\n파일을 편집한 후 다음 명령어로 실행하세요:")
    console.print("  [green]data-migrate run migration.yaml[/green]")
    console.print("  [green]data-migrate run migration.yaml --max-workers 5[/green]")
    console.print("  [green]data-migrate run migration.yaml --no-parallel[/green]")


if __name__ == "__main__":
    main()
