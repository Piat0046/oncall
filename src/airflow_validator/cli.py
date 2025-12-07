"""CLI 명령어 정의"""

from datetime import date, timedelta

import click
from dotenv import load_dotenv
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich.table import Table

from .config import AppSettings, AirflowAPISettings, MySQLSettings, PostgresSettings
from .db.mysql_client import MySQLClient
from .db.postgres_client import PostgresClient
from .api.airflow_client import AirflowAPIClient
from .services.dag_query import DagQueryService, PROVIDER_DAG_MAPPING
from .services.failure_checker import create_failure_checker
from .services.config_extractor import ConfigExtractorService
from .exporters.xlsx_exporter import XLSXExporter
from .utils.logger import setup_logger

# .env 파일 로드
load_dotenv()

console = Console()


@click.group()
@click.option("--verbose", "-v", is_flag=True, help="상세 로그 출력")
@click.pass_context
def main(ctx, verbose):
    """Airflow 작업 결과 검증 도구"""
    ctx.ensure_object(dict)
    ctx.obj["verbose"] = verbose
    ctx.obj["logger"] = setup_logger(verbose)


@main.command()
@click.option(
    "--date",
    "-d",
    "target_date",
    type=click.DateTime(formats=["%Y-%m-%d"]),
    default=str(date.today() - timedelta(days=1)),
    help="검증할 날짜 (기본값: 어제)",
)
@click.option(
    "--output",
    "-o",
    type=click.Path(),
    default="./output",
    help="출력 디렉토리 (기본값: ./output)",
)
@click.option(
    "--dag-id",
    "-dag",
    multiple=True,
    help="특정 DAG만 검사 (여러 개 지정 가능)",
)
@click.option(
    "--use-db",
    is_flag=True,
    help="PostgreSQL 직접 조회 사용 (기본: REST API)",
)
@click.pass_context
def validate(ctx, target_date, output, dag_id, use_db):
    """
    Airflow DAG 실행 결과를 검증하고 실패 보고서를 생성합니다.

    예시:
        airflow-validator validate -d 2024-01-15
        airflow-validator validate --dag-id my_dag_1 --dag-id my_dag_2
        airflow-validator validate --use-db
    """
    logger = ctx.obj["logger"]
    target = target_date.date() if hasattr(target_date, "date") else target_date

    console.print(f"\n[bold blue]Airflow 검증 시작[/bold blue]")
    console.print(f"대상 날짜: {target}")
    console.print(f"조회 방식: {'PostgreSQL 직접' if use_db else 'REST API'}")

    try:
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console,
        ) as progress:

            # Step 1: MySQL에서 동기화 실패 데이터 조회
            task1 = progress.add_task(
                "[cyan]MySQL에서 동기화 실패 데이터 조회 중...", total=None
            )
            mysql_client = MySQLClient()
            dag_service = DagQueryService(mysql_client)
            sync_failures = dag_service.get_sync_failures()
            progress.update(task1, completed=True)
            console.print(f"  동기화 실패 데이터: {len(sync_failures)}건")

            # Step 2: Provider에서 DAG ID 추출
            if dag_id:
                dag_ids = list(dag_id)
                console.print(f"  지정된 DAG: {dag_ids}")
            else:
                dag_ids = dag_service.get_dag_ids_from_failures(sync_failures)
                console.print(f"  추출된 DAG 수: {len(dag_ids)}")

            # Step 3: Airflow에서 실패 정보 조회
            airflow_failures = []
            if dag_ids:
                task2 = progress.add_task(
                    "[cyan]Airflow에서 실패 정보 조회 중...", total=None
                )
                failure_checker = create_failure_checker(use_db=use_db)
                airflow_failures = failure_checker.get_failures(dag_ids, target)
                progress.update(task2, completed=True)
                console.print(f"  실패한 DAG Run 수: {len(airflow_failures)}")

            # Step 4: Config 추출
            task3 = progress.add_task("[cyan]Config 정보 추출 중...", total=None)
            config_service = ConfigExtractorService()
            failure_data = config_service.extract_configs(airflow_failures)
            progress.update(task3, completed=True)

            # Step 5: XLSX 생성
            task4 = progress.add_task("[cyan]XLSX 파일 생성 중...", total=None)
            exporter = XLSXExporter(output)
            filepath = exporter.export(
                failure_data=failure_data,
                sync_failures=sync_failures,
                target_date=str(target),
            )
            progress.update(task4, completed=True)

        # 결과 출력
        console.print(f"\n[bold green]검증 완료![/bold green]")
        console.print(f"보고서 저장 위치: {filepath}")

        # 동기화 실패 요약 테이블
        if sync_failures:
            _print_sync_failure_table(sync_failures)

        # Airflow 실패 요약 테이블
        if airflow_failures:
            _print_airflow_failure_table(airflow_failures)
        else:
            console.print("\n[green]Airflow에서 실패한 DAG가 없습니다.[/green]")

    except Exception as e:
        logger.error(f"검증 실패: {e}")
        console.print(f"\n[bold red]오류 발생:[/bold red] {e}")
        raise click.Abort()


@main.command()
@click.option(
    "--date",
    "-d",
    "target_date",
    type=click.DateTime(formats=["%Y-%m-%d"]),
    default=str(date.today() - timedelta(days=1)),
    help="검증할 날짜 (기본값: 어제)",
)
@click.option(
    "--dag-id",
    "-dag",
    multiple=True,
    help="특정 DAG만 검사",
)
@click.option(
    "--use-db",
    is_flag=True,
    help="PostgreSQL 직접 조회 사용",
)
@click.pass_context
def summary(ctx, target_date, dag_id, use_db):
    """실패 요약 정보만 출력합니다 (파일 생성 없음)."""
    target = target_date.date() if hasattr(target_date, "date") else target_date

    console.print(f"\n[bold]날짜: {target}[/bold]")
    console.print(f"조회 방식: {'PostgreSQL 직접' if use_db else 'REST API'}")

    try:
        # MySQL에서 동기화 실패 조회
        mysql_client = MySQLClient()
        dag_service = DagQueryService(mysql_client)
        sync_failures = dag_service.get_sync_failures()

        console.print(f"\n동기화 실패 데이터: {len(sync_failures)}건")

        # DAG ID 추출
        if dag_id:
            dag_ids = list(dag_id)
        else:
            dag_ids = dag_service.get_dag_ids_from_failures(sync_failures)

        if not dag_ids:
            console.print("[yellow]조회된 DAG가 없습니다.[/yellow]")
            return

        # Airflow 실패 정보 조회
        failure_checker = create_failure_checker(use_db=use_db)
        airflow_failures = failure_checker.get_failures(dag_ids, target)

        # 요약 출력
        config_service = ConfigExtractorService()
        summary_info = config_service.get_summary(airflow_failures)

        console.print(f"실패한 DAG Run: {summary_info['total_failed_dag_runs']}")
        console.print(f"실패한 Task: {summary_info['total_failed_tasks']}")

        if sync_failures:
            _print_sync_failure_table(sync_failures)

        if airflow_failures:
            _print_airflow_failure_table(airflow_failures)

    except Exception as e:
        console.print(f"\n[bold red]오류 발생:[/bold red] {e}")
        raise click.Abort()


@main.command("check-connection")
def check_connection():
    """DB 및 API 연결 상태를 확인합니다."""
    console.print("[bold]연결 확인 중...[/bold]\n")

    # MySQL 연결 확인
    try:
        mysql_client = MySQLClient()
        if mysql_client.test_connection():
            console.print("[green]MySQL 연결 성공[/green]")
        else:
            console.print("[red]MySQL 연결 실패[/red]")
    except Exception as e:
        console.print(f"[red]MySQL 연결 실패:[/red] {e}")

    # Airflow REST API 연결 확인
    try:
        with AirflowAPIClient() as api_client:
            if api_client.test_connection():
                console.print("[green]Airflow REST API 연결 성공[/green]")
            else:
                console.print("[yellow]Airflow REST API 연결 실패 (서버 응답 없음)[/yellow]")
    except Exception as e:
        console.print(f"[yellow]Airflow REST API 연결 실패:[/yellow] {e}")

    # PostgreSQL 연결 확인
    try:
        postgres_client = PostgresClient()
        if postgres_client.test_connection():
            console.print("[green]PostgreSQL 연결 성공[/green]")
        else:
            console.print("[red]PostgreSQL 연결 실패[/red]")
    except Exception as e:
        console.print(f"[red]PostgreSQL 연결 실패:[/red] {e}")


@main.command("show-mapping")
def show_mapping():
    """Provider → DAG ID 매핑을 출력합니다."""
    console.print("[bold]Provider → DAG ID 매핑[/bold]\n")

    table = Table()
    table.add_column("Provider", style="cyan")
    table.add_column("DAG ID", style="green")

    for provider, dag_id in PROVIDER_DAG_MAPPING.items():
        table.add_row(provider, dag_id)

    console.print(table)


def _print_sync_failure_table(sync_failures: list):
    """동기화 실패 요약 테이블 출력"""
    # Provider별 그룹핑
    by_provider: dict[str, int] = {}
    for row in sync_failures:
        provider = row.get("provider", "UNKNOWN")
        by_provider[provider] = by_provider.get(provider, 0) + 1

    table = Table(title="동기화 실패 요약 (Provider별)")
    table.add_column("Provider", style="cyan")
    table.add_column("실패 건수", justify="right", style="red")

    for provider, count in sorted(by_provider.items()):
        table.add_row(provider, str(count))

    console.print(table)


def _print_airflow_failure_table(failures):
    """Airflow 실패 요약 테이블 출력"""
    table = Table(title="Airflow 실패한 DAG 요약")
    table.add_column("DAG ID", style="cyan")
    table.add_column("Run ID", style="magenta")
    table.add_column("Execution Date", style="white")
    table.add_column("실패 Task 수", justify="right", style="red")

    for f in failures:
        exec_date = f.execution_date
        if exec_date:
            exec_date_str = exec_date.strftime("%Y-%m-%d %H:%M")
        else:
            exec_date_str = "-"

        table.add_row(
            f.dag_id,
            f.run_id,
            exec_date_str,
            str(len(f.failed_tasks)),
        )

    console.print(table)


if __name__ == "__main__":
    main()
