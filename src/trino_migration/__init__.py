"""
Trino 마이그레이션 도구
소스 Trino 클러스터에서 타겟 Trino/Hive로 데이터 마이그레이션
- S3 복사 방식: 파티션 단위 필터링
- INSERT SELECT 방식: WHERE 조건 필터링
"""

from trino_migration.migrator import TrinoMigrator

__all__ = ["TrinoMigrator"]
