#!/usr/bin/env python3
"""
MySQL 데이터 마이그레이션 스크립트
- 소스 DB에서 타겟 DB로 데이터 마이그레이션
- --config 사용 시 JSON 파일의 테이블 목록 사용
- --table 사용 시 --where 또는 --full 옵션 필요
- 환경변수 또는 CLI 인자로 DB 설정 가능
"""

import argparse
import json
import sys
from pathlib import Path
from typing import Any

import pymysql
import pymysql.cursors

from config import settings


def normalize_where(where: str | None) -> str:
    """WHERE 조건 정규화 - 빈 값이면 1=1 반환"""
    if not where or where.strip() == "":
        return "1=1"
    return where.strip()


class MySQLMigrator:
    """MySQL 데이터 마이그레이터"""

    def __init__(
        self,
        source_config: dict[str, Any],
        target_config: dict[str, Any],
    ):
        self.source_config = source_config
        self.target_config = target_config

    def get_connection(self, config: dict[str, Any]):
        """DB 연결 생성"""
        return pymysql.connect(
            host=config["host"],
            port=config.get("port", 3306),
            user=config["user"],
            password=config["password"],
            database=config.get("database"),
            charset=config.get("charset", "utf8mb4"),
            cursorclass=pymysql.cursors.DictCursor,
        )

    def get_table_schema(self, table_name: str) -> str:
        """테이블 스키마(CREATE TABLE) 조회"""
        with self.get_connection(self.source_config) as conn:
            with conn.cursor() as cursor:
                cursor.execute(f"SHOW CREATE TABLE {table_name}")
                result = cursor.fetchone()
                return result["Create Table"]

    def get_table_columns(self, table_name: str) -> list[str]:
        """테이블 컬럼 목록 조회"""
        with self.get_connection(self.source_config) as conn:
            with conn.cursor() as cursor:
                cursor.execute(f"DESCRIBE {table_name}")
                return [row["Field"] for row in cursor.fetchall()]

    def fetch_data(
        self,
        table_name: str,
        where: str,
        limit: int | None = None,
    ) -> list[dict[str, Any]]:
        """소스 DB에서 데이터 조회"""
        query = f"SELECT * FROM {table_name} WHERE {where}"
        if limit:
            query += f" LIMIT {limit}"

        with self.get_connection(self.source_config) as conn:
            with conn.cursor() as cursor:
                cursor.execute(query)
                return cursor.fetchall()

    def create_table_if_not_exists(self, table_name: str, schema: str):
        """타겟 DB에 테이블 생성"""
        with self.get_connection(self.target_config) as conn:
            with conn.cursor() as cursor:
                schema_modified = schema.replace(
                    "CREATE TABLE", "CREATE TABLE IF NOT EXISTS"
                )
                try:
                    cursor.execute(schema_modified)
                    conn.commit()
                    print(f"  테이블 생성 완료: {table_name}")
                except pymysql.err.OperationalError as e:
                    if "already exists" in str(e):
                        print(f"  테이블 이미 존재: {table_name}")
                    else:
                        raise

    def insert_data(
        self,
        table_name: str,
        data: list[dict[str, Any]],
        batch_size: int = 100,
        truncate: bool = False,
    ) -> int:
        """타겟 DB에 데이터 삽입"""
        if not data:
            return 0

        columns = list(data[0].keys())
        placeholders = ", ".join(["%s"] * len(columns))
        columns_str = ", ".join([f"`{col}`" for col in columns])

        insert_query = f"""
            INSERT INTO {table_name} ({columns_str})
            VALUES ({placeholders})
            ON DUPLICATE KEY UPDATE {columns[0]}={columns[0]}
        """

        inserted = 0
        with self.get_connection(self.target_config) as conn:
            with conn.cursor() as cursor:
                if truncate:
                    cursor.execute(f"TRUNCATE TABLE {table_name}")
                    print(f"  테이블 초기화: {table_name}")

                for i in range(0, len(data), batch_size):
                    batch = data[i : i + batch_size]
                    values = [tuple(row[col] for col in columns) for row in batch]
                    try:
                        cursor.executemany(insert_query, values)
                        inserted += len(batch)
                    except Exception as e:
                        print(f"  삽입 오류 (batch {i}): {e}")
                        for row in batch:
                            try:
                                cursor.execute(
                                    insert_query,
                                    tuple(row[col] for col in columns)
                                )
                                inserted += 1
                            except Exception as e2:
                                print(f"    개별 삽입 오류: {e2}")

                conn.commit()

        return inserted

    def migrate_table(
        self,
        table_name: str,
        where: str,
        limit: int | None = None,
        create_table: bool = True,
        truncate: bool = False,
    ) -> dict[str, Any]:
        """단일 테이블 마이그레이션"""
        print(f"\n[마이그레이션] {table_name}")
        if where == "1=1":
            print("  조건: 전체 데이터")
        else:
            print(f"  조건: {where}")
        if limit:
            print(f"  제한: {limit}건")

        result = {
            "table": table_name,
            "status": "success",
            "fetched": 0,
            "inserted": 0,
            "error": None,
        }

        try:
            if create_table:
                schema = self.get_table_schema(table_name)
                self.create_table_if_not_exists(table_name, schema)

            data = self.fetch_data(table_name, where, limit)
            result["fetched"] = len(data)
            print(f"  조회된 데이터: {len(data)}건")

            if data:
                inserted = self.insert_data(table_name, data, truncate=truncate)
                result["inserted"] = inserted
                print(f"  삽입된 데이터: {inserted}건")

        except Exception as e:
            result["status"] = "error"
            result["error"] = str(e)
            print(f"  오류: {e}")

        return result

    def migrate_all(
        self,
        tables_config: list[dict[str, Any]],
        create_tables: bool = True,
        truncate: bool = False,
    ) -> list[dict[str, Any]]:
        """여러 테이블 마이그레이션"""
        results = []

        for table_cfg in tables_config:
            result = self.migrate_table(
                table_name=table_cfg["name"],
                where=table_cfg["where"],
                limit=table_cfg.get("limit"),
                create_table=create_tables,
                truncate=truncate,
            )
            results.append(result)

        return results


def main():
    parser = argparse.ArgumentParser(
        description="MySQL 데이터 마이그레이션 (환경변수 또는 CLI 인자 사용)"
    )

    # 소스 DB 설정 (환경변수 기본값 사용)
    parser.add_argument(
        "--source-host",
        default=settings.source.host,
        help=f"소스 DB 호스트 (기본: {settings.source.host})",
    )
    parser.add_argument(
        "--source-port",
        type=int,
        default=settings.source.port,
        help=f"소스 DB 포트 (기본: {settings.source.port})",
    )
    parser.add_argument(
        "--source-user",
        default=settings.source.user,
        help=f"소스 DB 사용자 (기본: {settings.source.user})",
    )
    parser.add_argument(
        "--source-password",
        default=settings.source.password,
        help="소스 DB 비밀번호 (기본: 환경변수)",
    )
    parser.add_argument(
        "--source-database",
        default=settings.source.database,
        help=f"소스 DB 데이터베이스 (기본: {settings.source.database})",
    )

    # 타겟 DB 설정 (환경변수 기본값 사용)
    parser.add_argument(
        "--target-host",
        default=settings.target.host,
        help=f"타겟 DB 호스트 (기본: {settings.target.host})",
    )
    parser.add_argument(
        "--target-port",
        type=int,
        default=settings.target.port,
        help=f"타겟 DB 포트 (기본: {settings.target.port})",
    )
    parser.add_argument(
        "--target-user",
        default=settings.target.user,
        help=f"타겟 DB 사용자 (기본: {settings.target.user})",
    )
    parser.add_argument(
        "--target-password",
        default=settings.target.password,
        help="타겟 DB 비밀번호 (기본: 환경변수)",
    )
    parser.add_argument(
        "--target-database",
        default=settings.target.database,
        help=f"타겟 DB 데이터베이스 (기본: {settings.target.database})",
    )

    # 마이그레이션 옵션
    parser.add_argument(
        "--config",
        type=str,
        help="마이그레이션 설정 JSON 파일 경로",
    )
    parser.add_argument(
        "--table",
        type=str,
        action="append",
        help="마이그레이션할 테이블 (여러 개 지정 가능)",
    )
    parser.add_argument(
        "--where",
        type=str,
        help="WHERE 조건 (--table 사용 시)",
    )
    parser.add_argument(
        "--full",
        action="store_true",
        help="전체 데이터 마이그레이션 (--table 사용 시 --where 대신 사용)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        help="조회 제한 건수",
    )
    parser.add_argument(
        "--truncate",
        action="store_true",
        help="삽입 전 테이블 초기화",
    )
    parser.add_argument(
        "--no-create-table",
        action="store_true",
        help="테이블 생성 건너뛰기",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="실제 마이그레이션 없이 설정만 확인",
    )
    parser.add_argument(
        "--show-config",
        action="store_true",
        help="현재 DB 설정 출력",
    )

    args = parser.parse_args()

    # 설정 확인 모드
    if args.show_config:
        print("현재 DB 설정 (환경변수 기반):")
        print(f"\n[소스 DB]")
        print(f"  Host: {settings.source.host}")
        print(f"  Port: {settings.source.port}")
        print(f"  User: {settings.source.user}")
        print(f"  Database: {settings.source.database}")
        print(f"\n[타겟 DB]")
        print(f"  Host: {settings.target.host}")
        print(f"  Port: {settings.target.port}")
        print(f"  User: {settings.target.user}")
        print(f"  Database: {settings.target.database}")
        return

    # DB 설정
    source_config = {
        "host": args.source_host,
        "port": args.source_port,
        "user": args.source_user,
        "password": args.source_password,
        "database": args.source_database,
    }

    target_config = {
        "host": args.target_host,
        "port": args.target_port,
        "user": args.target_user,
        "password": args.target_password,
        "database": args.target_database,
    }

    # 테이블 설정 로드
    if args.config:
        config_path = Path(args.config)
        if not config_path.is_absolute():
            # 현재 디렉토리 또는 프로젝트 루트 기준 상대 경로
            if (Path(__file__).parent / args.config).exists():
                config_path = Path(__file__).parent / args.config
            else:
                config_path = Path(__file__).parent.parent.parent / args.config
        with open(config_path) as f:
            config = json.load(f)
            tables_config = config.get("tables", [])

        # WHERE 조건 정규화 (빈 값 → 1=1)
        for cfg in tables_config:
            cfg["where"] = normalize_where(cfg.get("where"))

    elif args.table:
        if not args.where and not args.full:
            print("오류: --table 사용 시 --where 또는 --full 옵션이 필요합니다.")
            print("  예: --table users --where \"created_at >= '2024-01-01'\"")
            print("  또는: --table users --full")
            sys.exit(1)

        where_condition = "1=1" if args.full else args.where

        tables_config = [
            {
                "name": table,
                "where": where_condition,
                "limit": args.limit,
            }
            for table in args.table
        ]
    else:
        print("오류: 마이그레이션할 테이블을 지정해야 합니다.")
        print("  --config <config.json>")
        print("  또는 --table <테이블명> --where <조건>")
        print("  또는 --table <테이블명> --full")
        sys.exit(1)

    # 설정 출력
    print("=" * 60)
    print("MySQL 데이터 마이그레이션")
    print("=" * 60)
    print(f"소스: {source_config['host']}:{source_config['port']}/{source_config['database']}")
    print(f"타겟: {target_config['host']}:{target_config['port']}/{target_config['database']}")
    print(f"테이블 수: {len(tables_config)}")
    print("=" * 60)

    if args.dry_run:
        print("\n[DRY-RUN] 마이그레이션 설정:")
        for cfg in tables_config:
            where_display = "전체 데이터" if cfg.get("where") == "1=1" else cfg.get("where")
            print(f"  - {cfg['name']}")
            print(f"    WHERE: {where_display}")
            if cfg.get("limit"):
                print(f"    LIMIT: {cfg.get('limit')}")
        return

    # 마이그레이션 실행
    migrator = MySQLMigrator(source_config, target_config)
    results = migrator.migrate_all(
        tables_config,
        create_tables=not args.no_create_table,
        truncate=args.truncate,
    )

    # 결과 출력
    print("\n" + "=" * 60)
    print("마이그레이션 결과")
    print("=" * 60)

    total_fetched = 0
    total_inserted = 0
    errors = []

    for r in results:
        status_icon = "OK" if r["status"] == "success" else "NG"
        print(f"[{status_icon}] {r['table']}: {r['inserted']}/{r['fetched']}건")
        total_fetched += r["fetched"]
        total_inserted += r["inserted"]
        if r["error"]:
            errors.append(f"  - {r['table']}: {r['error']}")

    print("-" * 60)
    print(f"총계: {total_inserted}/{total_fetched}건 마이그레이션")

    if errors:
        print("\n오류 목록:")
        for e in errors:
            print(e)


if __name__ == "__main__":
    main()
