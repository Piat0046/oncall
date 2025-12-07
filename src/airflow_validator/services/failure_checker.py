"""Airflow에서 실패 정보 조회 (API/DB 추상화)"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any

from ..api.airflow_client import AirflowAPIClient
from ..db.postgres_client import PostgresClient


@dataclass
class FailedTaskInstance:
    """실패한 Task Instance"""

    dag_id: str
    task_id: str
    run_id: str
    execution_date: datetime
    state: str
    duration: float | None = None
    try_number: int | None = None
    operator: str | None = None
    hostname: str | None = None


@dataclass
class FailedDagRun:
    """실패한 DAG Run"""

    dag_id: str
    run_id: str
    execution_date: datetime
    state: str
    start_date: datetime | None = None
    end_date: datetime | None = None
    conf: dict[str, Any] = field(default_factory=dict)
    run_type: str | None = None
    failed_tasks: list[FailedTaskInstance] = field(default_factory=list)


class FailureCheckerBase(ABC):
    """실패 검사 서비스 추상 클래스"""

    @abstractmethod
    def get_failures(
        self, dag_ids: list[str], target_date: date
    ) -> list[FailedDagRun]:
        """실패한 DAG Run과 Task Instance 조회"""
        pass

    @abstractmethod
    def test_connection(self) -> bool:
        """연결 테스트"""
        pass


class APIFailureChecker(FailureCheckerBase):
    """Airflow REST API를 통한 실패 검사"""

    def __init__(self, client: AirflowAPIClient):
        self.client = client

    def test_connection(self) -> bool:
        return self.client.test_connection()

    def get_failures(
        self, dag_ids: list[str], target_date: date
    ) -> list[FailedDagRun]:
        """REST API로 실패 정보 조회"""
        results = []

        for dag_id in dag_ids:
            # 실패한 DAG Run 조회
            failed_runs = self.client.get_failed_dag_runs(dag_id, target_date)

            for run in failed_runs:
                # 실패한 Task 조회
                run_id = run.get("dag_run_id", "")
                failed_tasks_data = self.client.get_failed_task_instances(dag_id, run_id)

                failed_tasks = [
                    FailedTaskInstance(
                        dag_id=dag_id,
                        task_id=t.get("task_id", ""),
                        run_id=run_id,
                        execution_date=self._parse_datetime(t.get("execution_date")),
                        state=t.get("state", ""),
                        duration=t.get("duration"),
                        try_number=t.get("try_number"),
                        operator=t.get("operator"),
                        hostname=t.get("hostname"),
                    )
                    for t in failed_tasks_data
                ]

                results.append(
                    FailedDagRun(
                        dag_id=dag_id,
                        run_id=run_id,
                        execution_date=self._parse_datetime(run.get("execution_date")),
                        state=run.get("state", ""),
                        start_date=self._parse_datetime(run.get("start_date")),
                        end_date=self._parse_datetime(run.get("end_date")),
                        conf=run.get("conf") or {},
                        run_type=run.get("run_type"),
                        failed_tasks=failed_tasks,
                    )
                )

        return results

    @staticmethod
    def _parse_datetime(value: str | None) -> datetime | None:
        """ISO 형식 문자열을 datetime으로 변환"""
        if not value:
            return None
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00"))
        except Exception:
            return None


class DBFailureChecker(FailureCheckerBase):
    """PostgreSQL 직접 조회를 통한 실패 검사"""

    def __init__(self, client: PostgresClient):
        self.client = client

    def test_connection(self) -> bool:
        return self.client.test_connection()

    def get_failures(
        self, dag_ids: list[str], target_date: date
    ) -> list[FailedDagRun]:
        """PostgreSQL에서 직접 실패 정보 조회"""
        # 실패한 DAG Run 조회
        failed_runs = self.client.get_failed_dag_runs(dag_ids, target_date)

        # 실패한 Task Instance 조회
        failed_tasks = self.client.get_failed_task_instances(dag_ids, target_date)

        # Task를 DAG/run_id별로 그룹핑
        tasks_by_run: dict[tuple[str, str], list[FailedTaskInstance]] = {}
        for task in failed_tasks:
            key = (task["dag_id"], task.get("run_id", ""))
            if key not in tasks_by_run:
                tasks_by_run[key] = []
            tasks_by_run[key].append(
                FailedTaskInstance(
                    dag_id=task["dag_id"],
                    task_id=task["task_id"],
                    run_id=task.get("run_id", ""),
                    execution_date=task["execution_date"],
                    state=task["state"],
                    duration=task.get("duration"),
                    try_number=task.get("try_number"),
                    operator=task.get("operator"),
                    hostname=task.get("hostname"),
                )
            )

        # FailedDagRun 객체 생성
        results = []
        for run in failed_runs:
            key = (run["dag_id"], run.get("run_id", ""))
            results.append(
                FailedDagRun(
                    dag_id=run["dag_id"],
                    run_id=run.get("run_id", ""),
                    execution_date=run["execution_date"],
                    state=run["state"],
                    start_date=run.get("start_date"),
                    end_date=run.get("end_date"),
                    conf=run.get("conf") or {},
                    run_type=run.get("run_type"),
                    failed_tasks=tasks_by_run.get(key, []),
                )
            )

        return results


def create_failure_checker(
    use_db: bool = False,
    api_client: AirflowAPIClient | None = None,
    db_client: PostgresClient | None = None,
) -> FailureCheckerBase:
    """Factory: 실패 검사 서비스 생성"""
    if use_db:
        client = db_client or PostgresClient()
        return DBFailureChecker(client)
    else:
        client = api_client or AirflowAPIClient()
        return APIFailureChecker(client)
