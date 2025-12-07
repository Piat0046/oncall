"""MySQL 연결 및 쿼리 실행"""

from contextlib import contextmanager
from typing import Any

import pymysql
import pymysql.cursors

from ..config import MySQLSettings


class MySQLClient:
    """MySQL 클라이언트"""

    def __init__(self, config: MySQLSettings | None = None):
        self.config = config or MySQLSettings()

    @contextmanager
    def get_connection(self):
        """컨텍스트 매니저로 연결 관리"""
        conn = pymysql.connect(
            host=self.config.host,
            port=self.config.port,
            user=self.config.user,
            password=self.config.password,
            database=self.config.database,
            charset=self.config.charset,
            cursorclass=pymysql.cursors.DictCursor,
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
            with conn.cursor() as cursor:
                cursor.execute(query, params)
                return cursor.fetchall()

    def test_connection(self) -> bool:
        """연결 테스트"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT 1")
                    return True
        except Exception:
            return False
