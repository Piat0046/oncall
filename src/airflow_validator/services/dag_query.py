"""MySQL에서 동기화 실패 데이터 조회"""

from typing import Any

from ..db.mysql_client import MySQLClient


# Provider → DAG ID 매핑 (테스트용 샘플)
# TODO: 실제 매핑값으로 교체 필요
PROVIDER_DAG_MAPPING: dict[str, str] = {
    "NAVER": "naver_sync_dag",
    "KAKAO": "kakao_sync_dag",
    "GOOGLE": "google_sync_dag",
    "META": "meta_sync_dag",
    "TIKTOK": "tiktok_sync_dag",
    "CAFE24": "cafe24_sync_dag",
    "COUPANG": "coupang_sync_dag",
}


# 동기화 실패 데이터 조회 쿼리
SYNC_FAILURE_QUERY = """
WITH st0 AS (
  -- Connector V2 반영 Provider 제외
  SELECT dm.data_id
       , udl.user_id
       , dm.provider
       , dm.data_category
       , dm.mall_id
       , dm.mall_name
       , dm.data_set_name
       , dm.updated_at
       , us_cons.plan_end_datetime_id AS plan_end_consumer
    FROM laplace.data_meta AS dm
   INNER JOIN laplace.user_data_log AS udl
      ON dm.data_id = udl.data_id
    LEFT OUTER JOIN laplace.cafe_24_app_install AS cafe24
      ON dm.mall_id = cafe24.mall_id
    JOIN laplace.user_subscription us_cons
      ON us_cons.user_id = udl.user_id
   WHERE 1 = 1
     AND udl.use_yn = 'Y'
     AND dm.daily_yn = 'Y'
     AND (cafe24.install_yn = 'Y' OR cafe24.install_yn IS NULL)
     AND NOW() <= us_cons.plan_end_datetime_id
     AND DATE(CONVERT_TZ(dm.updated_at, '+00:00', '+09:00'))
      != DATE(CONVERT_TZ(NOW(), '+00:00', '+09:00'))
     AND (dm.data_category IN ('ad', 'order', 'analytics'))
     AND dm.provider NOT IN ('TIKTOK')
)
,st1 AS (
    -- Connector V2 반영 Provider 포함
  SELECT dm.data_id
       , dss.user_id
       , dm.provider
       , dm.data_category
       , dm.mall_id
       , dm.mall_name
       , dm.data_set_name
       , dm.updated_at
       , us_cons.plan_end_datetime_id AS plan_end_consumer
    FROM laplace.data_source_status AS dss
   INNER JOIN laplace.data_meta AS dm
      ON dss.data_source_id = dm.data_source_id
    JOIN laplace.user_subscription us_cons
      ON us_cons.user_id = dss.user_id
   WHERE 1 = 1
     AND dss.use_yn = 'Y'
     AND dss.status = 'SYNC_ENABLED'
     AND NOW() <= us_cons.plan_end_datetime_id
     AND DATE(CONVERT_TZ(dss.synced_at, '+00:00', '+09:00'))
      != DATE(CONVERT_TZ(NOW(), '+00:00', '+09:00'))
     AND dm.data_category IN ('ad', 'order')
     AND dm.provider IN ('TIKTOK')
)
, st2 AS (
  SELECT * FROM st0
   UNION ALL
  SELECT * FROM st1
)
SELECT *
  FROM st2
 ORDER BY provider, user_id, data_id
"""


class DagQueryService:
    """MySQL에서 동기화 실패 데이터 조회 서비스"""

    def __init__(
        self,
        mysql_client: MySQLClient,
        provider_dag_mapping: dict[str, str] | None = None,
    ):
        self.mysql = mysql_client
        self.provider_dag_mapping = provider_dag_mapping or PROVIDER_DAG_MAPPING

    def get_sync_failures(self) -> list[dict[str, Any]]:
        """
        동기화 실패 데이터 조회

        Returns:
            동기화 실패 데이터 목록
        """
        return self.mysql.execute_query(SYNC_FAILURE_QUERY)

    def get_dag_ids_from_failures(
        self, failures: list[dict[str, Any]] | None = None
    ) -> list[str]:
        """
        동기화 실패 데이터에서 DAG ID 목록 추출

        Args:
            failures: 동기화 실패 데이터 (None이면 새로 조회)

        Returns:
            DAG ID 목록 (중복 제거)
        """
        if failures is None:
            failures = self.get_sync_failures()

        dag_ids = set()
        for row in failures:
            provider = row.get("provider", "")
            dag_id = self.provider_dag_mapping.get(provider)
            if dag_id:
                dag_ids.add(dag_id)

        return list(dag_ids)

    def get_failures_by_provider(
        self, failures: list[dict[str, Any]] | None = None
    ) -> dict[str, list[dict[str, Any]]]:
        """
        Provider별로 실패 데이터 그룹핑

        Returns:
            {provider: [failures...]}
        """
        if failures is None:
            failures = self.get_sync_failures()

        grouped: dict[str, list[dict[str, Any]]] = {}
        for row in failures:
            provider = row.get("provider", "UNKNOWN")
            if provider not in grouped:
                grouped[provider] = []
            grouped[provider].append(row)

        return grouped

    def execute_custom_query(
        self, query: str, params: tuple | None = None
    ) -> list[dict[str, Any]]:
        """커스텀 쿼리 실행"""
        return self.mysql.execute_query(query, params)
