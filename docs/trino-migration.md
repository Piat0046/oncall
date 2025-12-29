# Trino Migration Tool

Trino 테이블/스키마를 다른 환경으로 마이그레이션하는 CLI 도구입니다.

## 기능

- **S3 복사 방식**: 소스 테이블의 S3 데이터를 타겟 S3로 복사하고 메타데이터 등록
- **INSERT SELECT 방식**: Trino 쿼리를 통한 데이터 마이그레이션 (WHERE 조건 지원)
- **파티션 필터링**: 특정 파티션만 선택적으로 마이그레이션
- **카탈로그 지원**: hive → iceberg 등 다른 카탈로그로 마이그레이션
- **CLI 및 YAML 설정 지원**

## 설치

```bash
uv pip install -e .
```

## 환경 설정

`.env` 파일 또는 환경변수로 설정:

```bash
# 소스 Trino
SOURCE_TRINO_HOST=source-trino.example.com
SOURCE_TRINO_PORT=8080
SOURCE_TRINO_USER=trino

# 타겟 Trino
TARGET_TRINO_HOST=target-trino.example.com
TARGET_TRINO_PORT=8080
TARGET_TRINO_USER=trino

# S3 설정
S3_AWS_PROFILE=default
S3_AWS_REGION=ap-northeast-2
S3_TARGET_BUCKET=target-data-lake

# S3 Endpoint (LocalStack 등 사용 시)
S3_SOURCE_ENDPOINT_URL=              # 소스 S3 endpoint (AWS면 비워둠)
S3_TARGET_ENDPOINT_URL=http://localhost:4566  # 타겟 S3 endpoint
```

## 사용법

### 1. 설정 확인

```bash
trino-migrate show-config
```

### 2. 소스 환경 분석

```bash
# 스키마 분석
trino-migrate analyze -c hive -s my_schema

# 특정 테이블 상세 분석
trino-migrate analyze -c hive -s my_schema -t my_table --show-ddl --show-partitions
```

### 3. CLI로 마이그레이션

```bash
# 스키마 전체 마이그레이션 (S3 복사)
trino-migrate migrate -c hive -s my_schema

# 특정 테이블만 마이그레이션
trino-migrate migrate -c hive -s my_schema -t users -t orders

# INSERT SELECT 방식 + WHERE 조건
trino-migrate migrate -c hive -s my_schema -t events \
  --method insert_select \
  --where "dt >= '2024-01-01'"

# 파티션 필터
trino-migrate migrate -c hive -s my_schema \
  --partition-filter "dt >= '2024-01-01'" \
  --partition-filter "dt <= '2024-12-31'"

# 다른 카탈로그로 마이그레이션
trino-migrate migrate -c hive -s my_schema \
  --target-catalog iceberg \
  --target-schema my_schema_new

# Dry-run (실제 마이그레이션 없이 확인)
trino-migrate migrate -c hive -s my_schema --dry-run
```

### 4. YAML 설정으로 마이그레이션

```bash
# 예시 설정 파일 생성
trino-migrate init

# YAML 파일로 마이그레이션 실행
trino-migrate run migration.yaml

# Dry-run
trino-migrate run migration.yaml --dry-run
```

## YAML 설정 형식

```yaml
# 전역 설정
parallel_tables: 3
parallel_partitions: 5
dry_run: false

# S3 설정 (환경변수 오버라이드)
# target_bucket: target-data-lake

# 개별 테이블 마이그레이션
tables:
  - catalog: hive           # 카탈로그 (필수)
    schema: analytics
    table: events
    method: s3_copy         # s3_copy 또는 insert_select

  - catalog: hive
    schema: analytics
    table: daily_metrics
    method: s3_copy
    partition_filter:       # 파티션 필터 (s3_copy용)
      - "dt >= '2024-01-01'"

  - catalog: hive
    schema: analytics
    table: users
    method: insert_select
    where: "created_at >= '2024-01-01'"  # WHERE 조건 (insert_select용)

  - catalog: hive
    schema: prod
    table: orders
    method: s3_copy
    target_catalog: iceberg  # 타겟 카탈로그 (선택)
    target_schema: prod_new  # 타겟 스키마 (선택)
    target_table: orders_v2  # 타겟 테이블 (선택)

# 스키마 단위 마이그레이션
schemas:
  - catalog: hive
    schema: raw_data
    method: s3_copy
    exclude:                 # 제외할 테이블
      - temp_table
      - staging_table

  - catalog: hive
    schema: logs
    method: s3_copy
    partition_filter:
      - "dt >= '2024-06-01'"
    target_catalog: iceberg
    target_schema: logs_archive
```

## 마이그레이션 방식 비교

| 방식 | 장점 | 단점 | 사용 시점 |
|------|------|------|-----------|
| **S3 복사** | 빠름, Trino 부하 없음 | 동일 S3 접근 필요 | 대용량 테이블, 전체 또는 파티션 단위 |
| **INSERT SELECT** | WHERE 조건 지원 | Trino 부하, 느림 | 조건부 데이터 추출, 소량 데이터 |

## 주의사항

1. **카탈로그 필수**: 모든 테이블/스키마 설정에 `catalog` 필드 필수
2. **S3 권한**: S3 복사 방식 사용 시 소스/타겟 S3 버킷 접근 권한 필요
3. **파티션 동기화**: S3 복사 후 `sync_partition_metadata` 자동 실행
4. **기존 테이블 덮어쓰기**: 타겟에 동일 테이블 존재 시 DROP 후 재생성
