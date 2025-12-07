# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Airflow 작업 결과 검증 도구 (airflow-validator) - MySQL에서 동기화 실패 데이터를 조회하고, Airflow REST API 또는 PostgreSQL에서 DAG 실패 상세 정보를 확인하여 XLSX 리포트를 생성하는 Python CLI 도구.

## Project Structure

```
oncall-claude/
├── src/airflow_validator/     # DAG 검증 도구
│   ├── api/                   # Airflow REST API 클라이언트
│   ├── db/                    # MySQL, PostgreSQL 클라이언트
│   ├── services/              # 비즈니스 로직
│   ├── exporters/             # XLSX 내보내기
│   └── cli.py                 # CLI 진입점
│
├── scripts/migration/         # 데이터 마이그레이션 도구
│   ├── config.py              # 마이그레이션 설정 (pydantic-settings)
│   ├── migrate.py             # 마이그레이션 실행 스크립트
│   └── migration_tables.json  # 마이그레이션 대상 테이블 목록
│
└── docker/                    # Docker 초기화 스크립트
```

## Development Commands

```bash
# 환경 설정
uv venv && source .venv/bin/activate
uv pip install -e .

# 테스트 환경 (Docker)
docker compose up -d                    # MySQL 3307, PostgreSQL 5433
cp .env.test .env                       # 테스트 환경 설정 적용

# DAG 검증 CLI 실행
airflow-validator check-connection      # DB/API 연결 확인
airflow-validator validate -d 2024-01-15  # 검증 실행 (기본: REST API)
airflow-validator validate --use-db     # PostgreSQL 직접 조회
airflow-validator show-mapping          # Provider→DAG 매핑 확인

# 데이터 마이그레이션 (환경변수 사용)
cd scripts/migration
python migrate.py --show-config         # 현재 설정 확인
python migrate.py --config migration_tables.json --dry-run  # 설정 확인
python migrate.py --config migration_tables.json            # 실행

# 데이터 마이그레이션 (CLI 인자로 오버라이드)
python migrate.py \
  --source-host <prod-host> --source-database laplace \
  --target-host localhost --target-port 3307 --target-database laplace \
  --table users --where "created_at >= '2024-01-01'"
```

## Architecture

```
MySQL (laplace DB)     →  Airflow 조회           →  XLSX
동기화 실패 데이터        [기본] REST API          검증 리포트
(SYNC_FAILURE_QUERY)     [--use-db] PostgreSQL
```

### DAG Validator Components

- **`services/dag_query.py`**: MySQL 쿼리 실행, `PROVIDER_DAG_MAPPING` dict로 Provider→DAG ID 변환
- **`services/failure_checker.py`**: `FailureCheckerBase` 추상 클래스, `APIFailureChecker`(REST API)와 `DBFailureChecker`(PostgreSQL) 구현체
- **`api/airflow_client.py`**: Airflow 2.6.1 REST API 클라이언트 (httpx 사용)
- **`db/postgres_client.py`**: Airflow 메타데이터 DB 직접 조회, `dag_run.conf` Pickle 디코딩 처리
- **`exporters/xlsx_exporter.py`**: 4개 시트 생성 (Sync Failures, DAG Run Summary, Failed Tasks, DAG Configs)
- **`config.py`**: pydantic-settings 기반, `.env` 파일에서 자동 로드

### Migration Components

- **`scripts/migration/config.py`**: 소스/타겟 DB 설정 (pydantic-settings)
- **`scripts/migration/migrate.py`**: 마이그레이션 실행, WHERE 조건 필수
- **`migration_tables.json`**: 마이그레이션 대상 테이블 및 조건 설정

### Environment Variables

```bash
# DAG Validator
MYSQL_*          # 동기화 결과 DB (laplace)
AIRFLOW_API_*    # Airflow REST API (기본 조회 방식)
POSTGRES_*       # Airflow 메타데이터 DB (--use-db 옵션용)

# Migration
SOURCE_DB_*      # 소스 DB (마이그레이션 원본)
TARGET_DB_*      # 타겟 DB (마이그레이션 대상)
```

## Important Notes

- `PROVIDER_DAG_MAPPING` (dag_query.py:10-18)은 테스트용 샘플 - 실제 매핑값으로 교체 필요
- Tailscale 연결 필요 시: `sudo tailscale up --accept-routes`
- MySQL 외래키 참조 테이블이 있어 마이그레이션 시 FK 검사 비활성화 고려
- 마이그레이션은 반드시 WHERE 조건 필요 (전체 데이터 마이그레이션 방지)
