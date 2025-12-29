"""
데이터 마이그레이션 도구 (비동기 버전)
MySQL 소스 DB에서 타겟 DB로 데이터를 마이그레이션
aiomysql 기반 비동기 처리 + 병렬 마이그레이션 지원
"""

from mysql_migration.migrator import AsyncMySQLMigrator

__all__ = ["AsyncMySQLMigrator"]
