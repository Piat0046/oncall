"""PostgreSQL 연결 및 Airflow 메타데이터 조회"""

import pickle
from contextlib import contextmanager
from datetime import date, datetime, timedelta
from typing import Any

import psycopg2
import psycopg2.extras

from ..config import PostgresSettings


class PostgresClient:
    """PostgreSQL 클라이언트 (Airflow 메타데이터 DB)"""

    def __init__(self, config: PostgresSettings | None = None):
        self.config = config or PostgresSettings()

    @contextmanager
    def get_connection(self):
        """컨텍스트 매니저로 연결 관리"""
        conn = psycopg2.connect(
            host=self.config.host,
            port=self.config.port,
            user=self.config.user,
            password=self.config.password,
            dbname=self.config.database,
        )
        try:
            yield conn
        finally:
            conn.close()

    def execute_query(
        self, query: str, params: tuple | None = None
    ) -> list[dict[str, Any]]:
        """쿼리 실행 및 결과 반환"""
        with self.get_connection() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                cursor.execute(query, params)
                return [dict(row) for row in cursor.fetchall()]

    def test_connection(self) -> bool:
        """연결 테스트"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT 1")
                    return True
        except Exception:
            return False

    def get_failed_dag_runs(
        self, dag_ids: list[str], target_date: date
    ) -> list[dict[str, Any]]:
        """특정 DAG들의 실패한 DAG Run 조회"""
        start_dt = datetime.combine(target_date, datetime.min.time())
        end_dt = start_dt + timedelta(days=1)

        query = """
            SELECT
                dr.dag_id,
                dr.run_id,
                dr.execution_date,
                dr.state,
                dr.start_date,
                dr.end_date,
                dr.conf,
                dr.run_type,
                dr.external_trigger
            FROM dag_run dr
            WHERE dr.dag_id = ANY(%s)
              AND dr.state = 'failed'
              AND dr.execution_date >= %s
              AND dr.execution_date < %s
            ORDER BY dr.execution_date DESC
        """
        results = self.execute_query(query, (dag_ids, start_dt, end_dt))

        # conf 디코딩
        for row in results:
            row["conf"] = self._decode_conf(row.get("conf"))

        return results

    def get_failed_task_instances(
        self, dag_ids: list[str], target_date: date
    ) -> list[dict[str, Any]]:
        """실패한 Task Instance 조회"""
        start_dt = datetime.combine(target_date, datetime.min.time())
        end_dt = start_dt + timedelta(days=1)

        query = """
            SELECT
                ti.dag_id,
                ti.task_id,
                ti.run_id,
                ti.execution_date,
                ti.state,
                ti.start_date,
                ti.end_date,
                ti.duration,
                ti.try_number,
                ti.max_tries,
                ti.operator,
                ti.pool,
                ti.queue,
                ti.hostname
            FROM task_instance ti
            WHERE ti.dag_id = ANY(%s)
              AND ti.state = 'failed'
              AND ti.execution_date >= %s
              AND ti.execution_date < %s
            ORDER BY ti.dag_id, ti.execution_date, ti.task_id
        """
        return self.execute_query(query, (dag_ids, start_dt, end_dt))

    def get_dag_run_by_run_id(
        self, dag_id: str, run_id: str
    ) -> dict[str, Any] | None:
        """run_id로 DAG Run 조회"""
        query = """
            SELECT
                dr.dag_id,
                dr.run_id,
                dr.execution_date,
                dr.state,
                dr.start_date,
                dr.end_date,
                dr.conf,
                dr.run_type
            FROM dag_run dr
            WHERE dr.dag_id = %s AND dr.run_id = %s
        """
        results = self.execute_query(query, (dag_id, run_id))
        if results:
            results[0]["conf"] = self._decode_conf(results[0].get("conf"))
            return results[0]
        return None

    @staticmethod
    def _decode_conf(conf_bytes: bytes | None) -> dict[str, Any]:
        """dag_run.conf 컬럼 디코딩 (Pickle 형식)"""
        if conf_bytes is None:
            return {}
        try:
            return pickle.loads(conf_bytes)
        except Exception:
            return {"_decode_error": "Failed to decode conf"}
