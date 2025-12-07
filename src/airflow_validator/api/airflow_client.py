"""Airflow REST API 클라이언트"""

from datetime import date, datetime, timedelta
from typing import Any

import httpx

from ..config import AirflowAPISettings


class AirflowAPIClient:
    """Airflow REST API 클라이언트 (Airflow 2.6.1)"""

    def __init__(self, config: AirflowAPISettings | None = None):
        self.config = config or AirflowAPISettings()
        self.base_url = self.config.url.rstrip("/")
        self._client: httpx.Client | None = None

    @property
    def client(self) -> httpx.Client:
        """HTTP 클라이언트 (lazy initialization)"""
        if self._client is None:
            self._client = httpx.Client(
                base_url=f"{self.base_url}/api/v1",
                auth=(self.config.user, self.config.password),
                timeout=30.0,
            )
        return self._client

    def close(self):
        """클라이언트 종료"""
        if self._client:
            self._client.close()
            self._client = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def test_connection(self) -> bool:
        """연결 테스트"""
        try:
            response = self.client.get("/health")
            return response.status_code == 200
        except Exception:
            return False

    def get_dag_runs(
        self,
        dag_id: str,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
        state: str | None = None,
    ) -> list[dict[str, Any]]:
        """DAG Run 목록 조회"""
        params = {"limit": 100, "order_by": "-execution_date"}

        if start_date:
            params["execution_date_gte"] = start_date.isoformat()
        if end_date:
            params["execution_date_lte"] = end_date.isoformat()
        if state:
            params["state"] = state

        response = self.client.get(f"/dags/{dag_id}/dagRuns", params=params)
        response.raise_for_status()
        return response.json().get("dag_runs", [])

    def get_failed_dag_runs(
        self, dag_id: str, target_date: date
    ) -> list[dict[str, Any]]:
        """특정 날짜의 실패한 DAG Run 조회"""
        start_dt = datetime.combine(target_date, datetime.min.time())
        end_dt = start_dt + timedelta(days=1)

        return self.get_dag_runs(
            dag_id=dag_id,
            start_date=start_dt,
            end_date=end_dt,
            state="failed",
        )

    def get_dag_run_detail(self, dag_id: str, dag_run_id: str) -> dict[str, Any]:
        """DAG Run 상세 정보 조회"""
        response = self.client.get(f"/dags/{dag_id}/dagRuns/{dag_run_id}")
        response.raise_for_status()
        return response.json()

    def get_task_instances(
        self,
        dag_id: str,
        dag_run_id: str,
        state: str | None = None,
    ) -> list[dict[str, Any]]:
        """Task Instance 목록 조회"""
        response = self.client.get(f"/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances")
        response.raise_for_status()

        tasks = response.json().get("task_instances", [])

        if state:
            tasks = [t for t in tasks if t.get("state") == state]

        return tasks

    def get_failed_task_instances(
        self, dag_id: str, dag_run_id: str
    ) -> list[dict[str, Any]]:
        """실패한 Task Instance 조회"""
        return self.get_task_instances(dag_id, dag_run_id, state="failed")

    def get_dags(self, only_active: bool = True) -> list[dict[str, Any]]:
        """DAG 목록 조회"""
        params = {"limit": 100}
        if only_active:
            params["only_active"] = "true"

        response = self.client.get("/dags", params=params)
        response.raise_for_status()
        return response.json().get("dags", [])
