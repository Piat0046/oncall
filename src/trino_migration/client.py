"""
Trino 클라이언트 래퍼
trino-python-client 기반 연결 및 쿼리 실행
"""

import re
from typing import Any, Iterator

import trino
from rich.console import Console

console = Console()


class TrinoClient:
    """Trino 클라이언트 래퍼"""

    def __init__(
        self,
        host: str,
        port: int = 8080,
        user: str = "trino",
        catalog: str = "hive",
        schema: str = "default",
    ):
        self.host = host
        self.port = port
        self.user = user
        self.catalog = catalog  # 기본 카탈로그
        self.schema = schema
        self._conn: trino.dbapi.Connection | None = None

    def _get_connection(self) -> trino.dbapi.Connection:
        """연결 가져오기 (lazy initialization)"""
        if self._conn is None:
            self._conn = trino.dbapi.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                catalog=self.catalog,
                schema=self.schema,
            )
        return self._conn

    def _resolve_catalog(self, catalog: str | None) -> str:
        """카탈로그 결정 (파라미터 > 기본값)"""
        return catalog or self.catalog

    def execute(self, query: str, fetch: bool = True) -> list[dict[str, Any]]:
        """쿼리 실행 및 결과 반환"""
        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(query)
            # Trino는 lazy execution이므로 결과를 반드시 소비해야 쿼리가 완료됨
            if cursor.description:
                columns = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
                if fetch:
                    return [dict(zip(columns, row)) for row in rows]
            else:
                # INSERT/UPDATE/DELETE 등 결과가 없는 쿼리도 완료 대기
                cursor.fetchall()
            return []
        finally:
            cursor.close()

    def execute_iter(self, query: str) -> Iterator[dict[str, Any]]:
        """쿼리 실행 및 결과 스트리밍 반환"""
        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(query)
            if cursor.description:
                columns = [desc[0] for desc in cursor.description]
                for row in cursor:
                    yield dict(zip(columns, row))
        finally:
            cursor.close()

    def get_schemas(self, catalog: str | None = None) -> list[str]:
        """카탈로그의 모든 스키마 조회"""
        cat = self._resolve_catalog(catalog)
        query = f"SHOW SCHEMAS FROM {cat}"
        results = self.execute(query)
        return [r["Schema"] for r in results]

    def get_tables(self, schema: str, catalog: str | None = None) -> list[str]:
        """스키마의 모든 테이블 조회 (VIEW, MATERIALIZED VIEW 제외)"""
        cat = self._resolve_catalog(catalog)
        query = f"""
            SELECT table_name
            FROM {cat}.information_schema.tables
            WHERE table_schema = '{schema}'
              AND table_type = 'BASE TABLE'
        """
        results = self.execute(query)
        return [r["table_name"] for r in results]

    def get_table_ddl(self, schema: str, table: str, catalog: str | None = None) -> str:
        """테이블 DDL 조회"""
        cat = self._resolve_catalog(catalog)
        query = f"SHOW CREATE TABLE {cat}.{schema}.{table}"
        results = self.execute(query)
        if results:
            return results[0].get("Create Table", "")
        return ""

    def get_table_columns(self, schema: str, table: str, catalog: str | None = None) -> list[dict[str, Any]]:
        """테이블 컬럼 정보 조회"""
        cat = self._resolve_catalog(catalog)
        query = f"DESCRIBE {cat}.{schema}.{table}"
        return self.execute(query)

    def get_table_properties(self, schema: str, table: str, catalog: str | None = None) -> dict[str, Any]:
        """테이블 속성 조회 (location, format 등)"""
        cat = self._resolve_catalog(catalog)
        query = f"""
            SELECT *
            FROM {cat}.information_schema.tables
            WHERE table_schema = '{schema}'
              AND table_name = '{table}'
        """
        results = self.execute(query)
        return results[0] if results else {}

    def get_partitions(self, schema: str, table: str, catalog: str | None = None) -> list[dict[str, Any]]:
        """테이블 파티션 정보 조회"""
        cat = self._resolve_catalog(catalog)
        query = f'SHOW PARTITIONS FROM {cat}.{schema}."{table}"'
        try:
            return self.execute(query)
        except Exception:
            # 파티션이 없는 테이블
            return []

    def get_partition_columns(self, schema: str, table: str, catalog: str | None = None) -> list[str]:
        """파티션 컬럼 조회"""
        columns = self.get_table_columns(schema, table, catalog)
        partition_cols = []
        in_partition = False
        for col in columns:
            col_name = col.get("Column", col.get("column_name", ""))
            extra = col.get("Extra", col.get("extra", ""))
            if "partition key" in extra.lower():
                partition_cols.append(col_name)
            # DESCRIBE 결과에서 파티션 구분
            if col_name == "" and "Partition" in str(col):
                in_partition = True
            elif in_partition and col_name:
                partition_cols.append(col_name)
        return partition_cols

    def get_table_location(self, schema: str, table: str, catalog: str | None = None) -> str | None:
        """테이블의 S3 location 조회"""
        ddl = self.get_table_ddl(schema, table, catalog)
        # DDL에서 LOCATION 추출
        match = re.search(r"LOCATION\s+'([^']+)'", ddl, re.IGNORECASE)
        if match:
            return match.group(1)

        # external_location 속성에서 추출 (Hive)
        match = re.search(r"external_location\s*=\s*'([^']+)'", ddl, re.IGNORECASE)
        if match:
            return match.group(1)

        # location 속성에서 추출 (Iceberg)
        match = re.search(r"(?<![_\w])location\s*=\s*'([^']+)'", ddl, re.IGNORECASE)
        if match:
            return match.group(1)

        return None

    def get_table_format(self, schema: str, table: str, catalog: str | None = None) -> str | None:
        """테이블 파일 포맷 조회 (PARQUET, ORC 등)"""
        ddl = self.get_table_ddl(schema, table, catalog)
        match = re.search(r"format\s*=\s*'(\w+)'", ddl, re.IGNORECASE)
        if match:
            return match.group(1).upper()
        return None

    def get_row_count(self, schema: str, table: str, where: str | None = None, catalog: str | None = None) -> int:
        """테이블 row count 조회"""
        cat = self._resolve_catalog(catalog)
        query = f"SELECT COUNT(*) as cnt FROM {cat}.{schema}.{table}"
        if where:
            query += f" WHERE {where}"
        results = self.execute(query)
        return results[0]["cnt"] if results else 0

    def table_exists(self, schema: str, table: str, catalog: str | None = None) -> bool:
        """테이블 존재 여부 확인"""
        cat = self._resolve_catalog(catalog)
        query = f"""
            SELECT COUNT(*) as cnt
            FROM {cat}.information_schema.tables
            WHERE table_schema = '{schema}'
              AND table_name = '{table}'
        """
        results = self.execute(query)
        return results[0]["cnt"] > 0 if results else False

    def get_table_type(self, schema: str, table: str, catalog: str | None = None) -> str | None:
        """테이블 타입 조회 (BASE TABLE, VIEW, etc.)"""
        cat = self._resolve_catalog(catalog)
        query = f"""
            SELECT table_type
            FROM {cat}.information_schema.tables
            WHERE table_schema = '{schema}'
              AND table_name = '{table}'
        """
        results = self.execute(query)
        return results[0]["table_type"] if results else None

    def is_base_table(self, schema: str, table: str, catalog: str | None = None) -> bool:
        """일반 테이블인지 확인 (VIEW, MATERIALIZED VIEW 제외)"""
        return self.get_table_type(schema, table, catalog) == "BASE TABLE"

    def schema_exists(self, schema: str, catalog: str | None = None) -> bool:
        """스키마 존재 여부 확인"""
        schemas = self.get_schemas(catalog)
        return schema in schemas

    def create_schema(self, schema: str, location: str | None = None, catalog: str | None = None):
        """스키마 생성"""
        cat = self._resolve_catalog(catalog)
        query = f"CREATE SCHEMA IF NOT EXISTS {cat}.{schema}"
        if location:
            query += f" WITH (location = '{location}')"
        self.execute(query, fetch=False)

    def drop_table(self, schema: str, table: str, catalog: str | None = None):
        """테이블 삭제"""
        cat = self._resolve_catalog(catalog)
        self.execute(f"DROP TABLE IF EXISTS {cat}.{schema}.{table}", fetch=False)

    def get_full_table_name(self, schema: str, table: str, catalog: str | None = None) -> str:
        """전체 테이블명 반환 (catalog.schema.table)"""
        cat = self._resolve_catalog(catalog)
        return f"{cat}.{schema}.{table}"

    def close(self):
        """연결 종료"""
        if self._conn:
            self._conn.close()
            self._conn = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
