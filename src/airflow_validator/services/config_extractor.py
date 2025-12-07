"""실패한 DAG의 config 값 추출"""

from typing import Any

from .failure_checker import FailedDagRun


class ConfigExtractorService:
    """Config 추출 서비스"""

    def extract_configs(
        self, failed_runs: list[FailedDagRun]
    ) -> list[dict[str, Any]]:
        """
        실패한 DAG들의 config 및 상세 정보 추출

        Args:
            failed_runs: 실패한 DAG Run 목록

        Returns:
            추출된 정보 목록 (XLSX 내보내기용)
        """
        results = []

        for run in failed_runs:
            results.append(
                {
                    "dag_id": run.dag_id,
                    "run_id": run.run_id,
                    "execution_date": run.execution_date,
                    "state": run.state,
                    "start_date": run.start_date,
                    "end_date": run.end_date,
                    "run_type": run.run_type,
                    "conf": run.conf,
                    "failed_task_count": len(run.failed_tasks),
                    "failed_tasks": [
                        {
                            "task_id": t.task_id,
                            "operator": t.operator,
                            "duration": t.duration,
                            "try_number": t.try_number,
                            "hostname": t.hostname,
                        }
                        for t in run.failed_tasks
                    ],
                }
            )

        return results

    def get_summary(self, failed_runs: list[FailedDagRun]) -> dict[str, Any]:
        """실패 요약 정보"""
        total_failed_tasks = sum(len(run.failed_tasks) for run in failed_runs)

        return {
            "total_failed_dag_runs": len(failed_runs),
            "total_failed_tasks": total_failed_tasks,
            "dag_ids_with_failures": list(set(run.dag_id for run in failed_runs)),
        }
