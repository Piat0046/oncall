# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

데이터 마이그레이션 도구 (data-migrator) - MySQL 및 Trino 간 데이터 마이그레이션을 지원하는 Python CLI 도구. 비동기 처리 및 병렬 마이그레이션 지원.

## Project Structure

```
data-migrator/
├── src/
│   ├── mysql_migration/       # MySQL 마이그레이션 도구
│   │   ├── config.py          # 소스/타겟 DB 설정 (pydantic-settings)
│   │   ├── migrator.py        # 비동기 마이그레이션 핵심 로직
│   │   └── cli.py             # CLI 진입점
│   │
│   └── trino_migration/       # Trino 마이그레이션 도구
│       ├── config.py          # 소스/타겟 Trino + S3 설정
│       ├── client.py          # Trino 클라이언트 래퍼
│       ├── extractor.py       # 메타데이터 추출 (DDL, 파티션)
│       ├── s3_copier.py       # S3 데이터 복사 (boto3)
│       ├── migrator.py        # 마이그레이션 오케스트레이터
│       └── cli.py             # CLI 진입점
│
├── docker/                    # Docker 테스트 환경
│   ├── hive/                  # Hive Metastore 설정
│   ├── trino/                 # Trino 설정
│   └── localstack/            # LocalStack 초기화 (S3)
│
└── docs/
    └── trino-migration.md     # Trino 마이그레이션 가이드
```

## Development Commands

```bash
# 환경 설정
uv venv && source .venv/bin/activate
uv pip install -e .

# Docker 테스트 환경
docker compose up -d                    # 전체 환경 시작
docker compose up -d mysql localstack   # 필요한 서비스만

# MySQL 마이그레이션
mysql-migrate show-config               # 현재 설정 확인
mysql-migrate init                      # 예시 YAML 설정 파일 생성
mysql-migrate run migration.yaml        # YAML 기반 마이그레이션
mysql-migrate run migration.yaml --dry-run

# Trino 마이그레이션
trino-migrate show-config               # 현재 설정 확인
trino-migrate analyze -c hive -s my_schema  # 소스 환경 분석
trino-migrate analyze -c hive -s my_schema -t my_table --show-ddl

# CLI로 마이그레이션
trino-migrate migrate -c hive -s my_schema
trino-migrate migrate -c hive -s my_schema -t users -t orders
trino-migrate migrate -c hive -s my_schema --method insert_select --where "dt >= '2024-01-01'"
trino-migrate migrate -c hive -s my_schema --partition-filter "dt >= '2024-01-01'"
trino-migrate migrate -c hive -s my_schema --target-catalog iceberg --dry-run

# YAML로 마이그레이션
trino-migrate init                      # 예시 YAML 생성
trino-migrate run migration.yaml
trino-migrate run migration.yaml --dry-run
```

## Architecture

### MySQL Migration

```
소스 MySQL  →  aiomysql (비동기)  →  타겟 MySQL
            └─ 병렬 테이블 처리
            └─ FK 기반 자동 순서 정렬
            └─ 스트리밍 + 배치 INSERT
```

### Trino Migration

```
소스 Trino  →  S3 복사 (boto3)      →  타겟 Trino
            │  └─ 파티션 필터링
            │  └─ 병렬 복사
            │
            └─ INSERT SELECT (대안)  →  타겟 Trino
               └─ WHERE 조건 지원
               └─ CTAS 방식
```

#### 마이그레이션 방식

| 방식 | 장점 | 단점 | 사용 시점 |
|------|------|------|-----------|
| **S3 복사** | 빠름, Trino 부하 없음 | 동일 S3 접근 필요 | 대용량, 파티션 단위 |
| **INSERT SELECT** | WHERE 조건 지원 | Trino 부하, 느림 | 조건부 추출 |

### Docker 테스트 환경

| 서비스 | 포트 | 용도 |
|--------|------|------|
| MySQL | 3307 | 마이그레이션 타겟 |
| PostgreSQL | 5433 | Hive Metastore 백엔드 |
| LocalStack | 4566 | S3 |
| Hive Metastore | 9083 | 테이블 메타데이터 |
| Trino | 8080 | 분산 SQL 쿼리 |

### Environment Variables

```bash
# MySQL Migration
SOURCE_DB_*      # 소스 DB
TARGET_DB_*      # 타겟 DB

# Trino Migration
SOURCE_TRINO_*   # 소스 Trino (HOST, PORT, USER)
TARGET_TRINO_*   # 타겟 Trino (HOST, PORT, USER)

# S3
S3_TARGET_BUCKET         # 타겟 S3 버킷
S3_AWS_PROFILE           # AWS 프로필
S3_AWS_REGION            # AWS 리전
S3_SOURCE_ENDPOINT_URL   # 소스 S3 endpoint (LocalStack용)
S3_TARGET_ENDPOINT_URL   # 타겟 S3 endpoint (LocalStack용)
```

## Important Notes

### Trino Migration
- **카탈로그 필수**: 모든 테이블/스키마 설정에 `catalog` 필드 필수
- **S3 권한**: S3 복사 방식 사용 시 소스/타겟 S3 버킷 접근 권한 필요
- **파티션 동기화**: S3 복사 후 `sync_partition_metadata` 자동 실행
- **기존 테이블 덮어쓰기**: 타겟에 동일 테이블 존재 시 DROP 후 재생성

### MySQL Migration
- 외래키 참조 테이블은 자동으로 FK 기반 순서 정렬됨
- 날짜 suffix 테이블 (예: `table_20240101`)은 기본 제외
- YAML 설정으로 여러 DB 병렬 마이그레이션 가능
