"""
MySQL 데이터 마이그레이터 (비동기 버전)
소스 DB에서 타겟 DB로 데이터 마이그레이션
aiomysql 기반 비동기 처리 + 병렬 마이그레이션 지원
"""

import asyncio
import re
import warnings
from collections import defaultdict
from typing import Any

import aiomysql

# INSERT IGNORE 중복 키 경고 억제
warnings.filterwarnings("ignore", message=".*Duplicate entry.*")
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

console = Console()

# 날짜 suffix 패턴 (테이블명 끝에 날짜가 붙은 경우)
DATE_SUFFIX_PATTERN = re.compile(
    r"_(\d{8}|\d{6}|\d{4}[-_]\d{2}[-_]\d{2}|\d{2}[-_]\d{2}[-_]\d{2})(_\w+)?$"
)


def is_date_suffixed_table(table_name: str) -> bool:
    """테이블 이름 끝에 날짜 패턴이 있는지 확인"""
    return bool(DATE_SUFFIX_PATTERN.search(table_name))


def normalize_where(where: str | None) -> str:
    """WHERE 조건 정규화 - 빈 값이면 1=1 반환"""
    if not where or where.strip() == "":
        return "1=1"
    return where.strip()


def build_user_id_where(user_ids: list[int], existing_where: str | None = None) -> str:
    """user_id 필터링 WHERE 조건 생성

    Args:
        user_ids: 필터링할 user_id 목록
        existing_where: 기존 WHERE 조건 (있으면 AND로 결합)

    Returns:
        완성된 WHERE 조건 문자열
    """
    if not user_ids:
        return normalize_where(existing_where)

    # user_id IN (...) 조건 생성
    user_id_condition = f"user_id IN ({', '.join(map(str, user_ids))})"

    existing = normalize_where(existing_where)
    if existing == "1=1":
        return user_id_condition
    else:
        return f"({existing}) AND {user_id_condition}"


def topological_sort(tables: list[str], dependencies: dict[str, set[str]]) -> list[str]:
    """외래키 의존성을 기반으로 테이블을 토폴로지컬 정렬"""
    table_set = set(tables)
    in_degree = defaultdict(int)
    graph = defaultdict(list)

    for table in tables:
        in_degree[table] = 0

    for table, parents in dependencies.items():
        if table not in table_set:
            continue
        for parent in parents:
            if parent in table_set and parent != table:
                graph[parent].append(table)
                in_degree[table] += 1

    queue = [t for t in tables if in_degree[t] == 0]
    result = []

    while queue:
        queue.sort()
        current = queue.pop(0)
        result.append(current)

        for child in graph[current]:
            in_degree[child] -= 1
            if in_degree[child] == 0:
                queue.append(child)

    if len(result) != len(tables):
        remaining = [t for t in tables if t not in result]
        console.print(f"  [yellow]경고: 순환 참조 발견, 나머지 테이블 추가: {remaining}[/yellow]")
        result.extend(remaining)

    return result


def group_tables_by_dependency_level(
    tables: list[str],
    dependencies: dict[str, set[str]]
) -> list[list[str]]:
    """FK 의존성 기반으로 테이블을 레벨별로 그룹화

    같은 레벨의 테이블들은 병렬 실행 가능
    Returns: [[level0_tables], [level1_tables], ...]
    """
    table_set = set(tables)
    remaining = set(tables)
    levels = []
    processed = set()

    while remaining:
        # 현재 레벨: 의존성이 모두 처리된 테이블들
        current_level = []
        for table in remaining:
            deps = dependencies.get(table, set())
            # 의존하는 테이블이 모두 처리되었거나, 대상 테이블 목록에 없는 경우
            unmet_deps = deps & table_set - processed
            if not unmet_deps:
                current_level.append(table)

        if not current_level:
            # 순환 참조 - 나머지 모두 추가
            current_level = list(remaining)

        levels.append(sorted(current_level))
        processed.update(current_level)
        remaining -= set(current_level)

    return levels


async def execute_lookup_query(config: dict[str, Any], database: str, sql: str) -> list[Any]:
    """lookup 쿼리 실행하여 ID 목록 조회 (비동기)"""
    conn = await aiomysql.connect(
        host=config["host"],
        port=config.get("port", 3306),
        user=config["user"],
        password=config["password"],
        db=database,
        charset=config.get("charset", "utf8mb4"),
    )
    try:
        async with conn.cursor(aiomysql.DictCursor) as cursor:
            await cursor.execute(sql)
            rows = await cursor.fetchall()
            if not rows:
                return []
            first_col = list(rows[0].keys())[0]
            return [row[first_col] for row in rows]
    finally:
        conn.close()


class AsyncMySQLMigrator:
    """비동기 MySQL 데이터 마이그레이터"""

    def __init__(
        self,
        source_config: dict[str, Any],
        target_config: dict[str, Any],
    ):
        self.source_config = source_config
        self.target_config = target_config
        self._source_pool: aiomysql.Pool | None = None
        self._target_pool: aiomysql.Pool | None = None

    async def _get_source_pool(self) -> aiomysql.Pool:
        """소스 DB 커넥션 풀 가져오기"""
        if self._source_pool is None:
            self._source_pool = await aiomysql.create_pool(
                host=self.source_config["host"],
                port=self.source_config.get("port", 3306),
                user=self.source_config["user"],
                password=self.source_config["password"],
                db=self.source_config.get("database"),
                charset=self.source_config.get("charset", "utf8mb4"),
                minsize=1,
                maxsize=10,
            )
        return self._source_pool

    async def _get_target_pool(self) -> aiomysql.Pool:
        """타겟 DB 커넥션 풀 가져오기"""
        if self._target_pool is None:
            self._target_pool = await aiomysql.create_pool(
                host=self.target_config["host"],
                port=self.target_config.get("port", 3306),
                user=self.target_config["user"],
                password=self.target_config["password"],
                db=self.target_config.get("database"),
                charset=self.target_config.get("charset", "utf8mb4"),
                minsize=1,
                maxsize=10,
            )
        return self._target_pool

    async def close(self):
        """커넥션 풀 정리"""
        if self._source_pool:
            self._source_pool.close()
            await self._source_pool.wait_closed()
        if self._target_pool:
            self._target_pool.close()
            await self._target_pool.wait_closed()

    async def check_database_exists(self, config: dict[str, Any]) -> bool:
        """데이터베이스 존재 여부 확인"""
        db_name = config.get("database")
        conn = await aiomysql.connect(
            host=config["host"],
            port=config.get("port", 3306),
            user=config["user"],
            password=config["password"],
            charset=config.get("charset", "utf8mb4"),
        )
        try:
            async with conn.cursor() as cursor:
                await cursor.execute(
                    "SELECT SCHEMA_NAME FROM information_schema.SCHEMATA WHERE SCHEMA_NAME = %s",
                    (db_name,)
                )
                result = await cursor.fetchone()
                return result is not None
        finally:
            conn.close()

    async def create_database(self, config: dict[str, Any]):
        """데이터베이스 생성"""
        db_name = config.get("database")
        conn = await aiomysql.connect(
            host=config["host"],
            port=config.get("port", 3306),
            user=config["user"],
            password=config["password"],
            charset=config.get("charset", "utf8mb4"),
        )
        try:
            async with conn.cursor() as cursor:
                await cursor.execute(
                    f"CREATE DATABASE `{db_name}` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"
                )
                await conn.commit()
        finally:
            conn.close()

    async def get_all_tables(self) -> list[str]:
        """소스 DB의 모든 테이블 목록 조회"""
        pool = await self._get_source_pool()
        async with pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cursor:
                await cursor.execute("SHOW TABLES")
                rows = await cursor.fetchall()
                return [list(row.values())[0] for row in rows]

    async def get_table_schema(self, table_name: str) -> str:
        """테이블 스키마(CREATE TABLE) 조회"""
        pool = await self._get_source_pool()
        async with pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cursor:
                await cursor.execute(f"SHOW CREATE TABLE `{table_name}`")
                result = await cursor.fetchone()
                return result["Create Table"]

    async def get_table_columns(self, table_name: str) -> list[str]:
        """테이블 컬럼 목록 조회"""
        pool = await self._get_source_pool()
        async with pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cursor:
                await cursor.execute(f"DESCRIBE `{table_name}`")
                rows = await cursor.fetchall()
                return [row["Field"] for row in rows]

    async def has_user_id_column(self, table_name: str) -> bool:
        """테이블에 user_id 컬럼이 있는지 확인"""
        columns = await self.get_table_columns(table_name)
        return "user_id" in columns

    async def get_tables_with_user_id_info(self, table_names: list[str]) -> dict[str, bool]:
        """여러 테이블의 user_id 컬럼 유무를 한 번에 조회

        Returns:
            {table_name: has_user_id} 딕셔너리
        """
        result = {}
        for table_name in table_names:
            result[table_name] = await self.has_user_id_column(table_name)
        return result

    async def get_foreign_key_dependencies(self, table_names: list[str]) -> dict[str, set[str]]:
        """테이블들의 외래키 의존성 조회"""
        if not table_names:
            return {}

        dependencies: dict[str, set[str]] = {t: set() for t in table_names}
        table_set = set(table_names)

        placeholders = ", ".join(["%s"] * len(table_names))
        query = f"""
            SELECT TABLE_NAME, REFERENCED_TABLE_NAME
            FROM information_schema.KEY_COLUMN_USAGE
            WHERE TABLE_SCHEMA = %s
                AND TABLE_NAME IN ({placeholders})
                AND REFERENCED_TABLE_NAME IS NOT NULL
        """

        pool = await self._get_source_pool()
        async with pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cursor:
                await cursor.execute(query, [self.source_config["database"]] + table_names)
                rows = await cursor.fetchall()
                for row in rows:
                    table = row["TABLE_NAME"]
                    parent = row["REFERENCED_TABLE_NAME"]
                    if parent in table_set:
                        dependencies[table].add(parent)

        return dependencies

    async def sort_tables_by_fk(self, tables_config: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """외래키 의존성을 기반으로 테이블 설정을 정렬"""
        table_names = [cfg["name"] for cfg in tables_config]
        config_map = {cfg["name"]: cfg for cfg in tables_config}

        console.print("\n[cyan][외래키 의존성 분석][/cyan]")
        dependencies = await self.get_foreign_key_dependencies(table_names)

        fk_count = sum(len(deps) for deps in dependencies.values())
        console.print(f"  대상 테이블: {len(table_names)}개")
        console.print(f"  외래키 관계: {fk_count}개")

        if fk_count > 0:
            for table, parents in dependencies.items():
                if parents:
                    console.print(f"  [dim]{table} → {', '.join(sorted(parents))}[/dim]")

        sorted_names = topological_sort(table_names, dependencies)
        return [config_map[name] for name in sorted_names]

    async def get_row_count(self, table_name: str, where: str, limit: int | None = None) -> int:
        """테이블의 row 수 조회"""
        query = f"SELECT COUNT(*) as cnt FROM `{table_name}` WHERE {where}"
        pool = await self._get_source_pool()
        async with pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cursor:
                await cursor.execute(query)
                result = await cursor.fetchone()
                count = result["cnt"]
                if limit:
                    return min(count, limit)
                return count

    async def create_table_if_not_exists(self, table_name: str, schema: str):
        """타겟 DB에 테이블 생성"""
        pool = await self._get_target_pool()
        async with pool.acquire() as conn:
            async with conn.cursor() as cursor:
                schema_modified = schema.replace("CREATE TABLE", "CREATE TABLE IF NOT EXISTS")
                try:
                    await cursor.execute(schema_modified)
                    await conn.commit()
                except Exception as e:
                    if "already exists" not in str(e):
                        raise

    async def migrate_table_with_progress(
        self,
        table_name: str,
        where: str,
        progress: Progress,
        overall_task_id: int,
        limit: int | None = None,
        create_table: bool = True,
        truncate: bool = False,
        batch_size: int = 1000,
    ) -> dict[str, Any]:
        """단일 테이블 마이그레이션 (스트리밍 + 진행률 표시)"""
        result = {
            "table": table_name,
            "status": "success",
            "fetched": 0,
            "inserted": 0,
            "skipped": 0,
            "errors": [],
        }

        try:
            # 스키마 조회 및 테이블 생성
            if create_table:
                try:
                    schema = await self.get_table_schema(table_name)
                except Exception as e:
                    result["status"] = "error"
                    result["errors"] = [f"[소스] 스키마 조회 실패: {e}"]
                    return result

                try:
                    await self.create_table_if_not_exists(table_name, schema)
                except Exception as e:
                    result["status"] = "error"
                    result["errors"] = [f"[타겟] 테이블 생성 실패: {e}"]
                    return result

            # row count 조회
            try:
                total_rows = await self.get_row_count(table_name, where, limit)
            except Exception as e:
                result["status"] = "error"
                result["errors"] = [f"[소스] row count 조회 실패: {e}"]
                return result

            if total_rows == 0:
                return result

            # 데이터 태스크
            data_task = progress.add_task(
                f"  [dim]└ {table_name}[/dim]",
                total=total_rows,
            )

            # 스트리밍 조회 + 배치 삽입
            query = f"SELECT * FROM `{table_name}` WHERE {where}"
            if limit:
                query += f" LIMIT {limit}"

            # 소스에서 스트리밍 조회
            source_conn = await aiomysql.connect(
                host=self.source_config["host"],
                port=self.source_config.get("port", 3306),
                user=self.source_config["user"],
                password=self.source_config["password"],
                db=self.source_config.get("database"),
                charset=self.source_config.get("charset", "utf8mb4"),
            )

            try:
                # SSCursor로 스트리밍
                async with source_conn.cursor(aiomysql.SSDictCursor) as source_cursor:
                    await source_cursor.execute(query)

                    # 타겟 커넥션
                    target_pool = await self._get_target_pool()
                    async with target_pool.acquire() as target_conn:
                        async with target_conn.cursor() as target_cursor:
                            if truncate:
                                await target_cursor.execute(f"TRUNCATE TABLE `{table_name}`")

                            columns = None
                            insert_query = None
                            batch = []

                            async for row in source_cursor:
                                if columns is None:
                                    columns = list(row.keys())
                                    placeholders = ", ".join(["%s"] * len(columns))
                                    columns_str = ", ".join([f"`{col}`" for col in columns])
                                    insert_query = f"INSERT IGNORE INTO `{table_name}` ({columns_str}) VALUES ({placeholders})"

                                batch.append(tuple(row[col] for col in columns))
                                result["fetched"] += 1

                                if len(batch) >= batch_size:
                                    inserted = await self._insert_batch(target_cursor, insert_query, batch, result)
                                    result["inserted"] += inserted
                                    result["skipped"] += len(batch) - inserted
                                    progress.update(data_task, completed=result["fetched"])
                                    batch = []

                            # 남은 배치
                            if batch:
                                inserted = await self._insert_batch(target_cursor, insert_query, batch, result)
                                result["inserted"] += inserted
                                result["skipped"] += len(batch) - inserted

                            await target_conn.commit()

            finally:
                source_conn.close()

            progress.update(data_task, completed=total_rows, visible=False)

            if result["errors"]:
                result["status"] = "warning"

        except Exception as e:
            result["status"] = "error"
            result["errors"] = [f"알 수 없는 오류: {e}"]

        return result

    async def _insert_batch(
        self,
        cursor,
        insert_query: str,
        batch: list[tuple],
        result: dict[str, Any],
    ) -> int:
        """배치 삽입 실행"""
        try:
            await cursor.executemany(insert_query, batch)
            return cursor.rowcount
        except Exception as e:
            if len(result["errors"]) < 5:
                result["errors"].append(f"[타겟] Batch: {e}")

            inserted = 0
            for row in batch:
                try:
                    await cursor.execute(insert_query, row)
                    inserted += cursor.rowcount
                except Exception as e2:
                    if len(result["errors"]) < 5:
                        result["errors"].append(f"[타겟] {e2}")
            return inserted

    async def migrate_all(
        self,
        tables_config: list[dict[str, Any]],
        create_tables: bool = True,
        truncate: bool = False,
        max_table_workers: int = 5,
    ) -> list[dict[str, Any]]:
        """여러 테이블 마이그레이션 (FK 레벨별 병렬 처리)"""
        results = []
        total_tables = len(tables_config)

        # FK 의존성 조회
        table_names = [cfg["name"] for cfg in tables_config]
        dependencies = await self.get_foreign_key_dependencies(table_names)

        # FK 레벨별 그룹화
        levels = group_tables_by_dependency_level(table_names, dependencies)
        config_map = {cfg["name"]: cfg for cfg in tables_config}

        progress = Progress(
            SpinnerColumn(),
            TextColumn("[bold]{task.description}"),
            BarColumn(),
            MofNCompleteColumn(),
            TaskProgressColumn(),
            TimeElapsedColumn(),
            console=console,
            transient=False,
            expand=False,
        )

        progress.start()
        try:
            overall_task = progress.add_task("[cyan]전체 진행률", total=total_tables)
            completed = 0

            for level_idx, level_tables in enumerate(levels):
                # 같은 레벨 내에서 병렬 실행
                semaphore = asyncio.Semaphore(max_table_workers)

                async def migrate_with_semaphore(table_name: str) -> dict[str, Any]:
                    async with semaphore:
                        cfg = config_map[table_name]
                        return await self.migrate_table_with_progress(
                            table_name=table_name,
                            where=cfg["where"],
                            progress=progress,
                            overall_task_id=overall_task,
                            limit=cfg.get("limit"),
                            create_table=create_tables,
                            truncate=truncate,
                        )

                level_results = await asyncio.gather(
                    *[migrate_with_semaphore(t) for t in level_tables],
                    return_exceptions=True
                )

                for table_name, res in zip(level_tables, level_results):
                    completed += 1
                    progress.update(overall_task, completed=completed)

                    if isinstance(res, Exception):
                        result = {
                            "table": table_name,
                            "status": "error",
                            "fetched": 0,
                            "inserted": 0,
                            "skipped": 0,
                            "errors": [str(res)],
                        }
                    else:
                        result = res

                    results.append(result)

                    # 상태 출력
                    if result["status"] == "success":
                        console.print(f"[green]✓[/green] {table_name}: {result['inserted']}/{result['fetched']}건")
                    elif result["status"] == "warning":
                        skip_info = f", 스킵: {result['skipped']}" if result["skipped"] else ""
                        console.print(f"[yellow]⚠[/yellow] {table_name}: {result['inserted']}/{result['fetched']}건{skip_info}")
                        for err in result["errors"][:3]:
                            console.print(f"  [dim red]└ {err}[/dim red]")
                    else:
                        console.print(f"[red]✗[/red] {table_name}: 오류 발생")
                        for err in result["errors"][:3]:
                            console.print(f"  [dim red]└ {err}[/dim red]")

        finally:
            progress.stop()

        return results
