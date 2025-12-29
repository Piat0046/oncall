# data-migrate

MySQL 데이터 마이그레이션 CLI 도구

## 설치

```bash
uv pip install -e .
```

## 환경 설정

`.env` 파일에 소스/타겟 DB 연결 정보 설정:

```bash
# 소스 DB (마이그레이션 원본)
SOURCE_DB_HOST=source-db.example.com
SOURCE_DB_PORT=3306
SOURCE_DB_USER=reader
SOURCE_DB_PASSWORD=password

# 타겟 DB (마이그레이션 대상)
TARGET_DB_HOST=localhost
TARGET_DB_PORT=3307
TARGET_DB_USER=laplace
TARGET_DB_PASSWORD=laplace123
```

## 사용법

### 1. 단일 데이터베이스 마이그레이션 (CLI)

```bash
# 전체 테이블 마이그레이션
data-migrate migrate -d laplace --all

# 특정 테이블만
data-migrate migrate -d laplace --table users --table orders

# 특정 테이블 제외
data-migrate migrate -d laplace --all --exclude large_logs

# WHERE 조건 적용
data-migrate migrate -d laplace --all --where "created_at >= '2024-01-01'"

# Dry-run (설정만 확인)
data-migrate migrate -d laplace --all --dry-run
```

### 2. YAML 설정 파일로 마이그레이션

```bash
# 예시 YAML 파일 생성
data-migrate init

# YAML 설정으로 실행
data-migrate run migration.yaml

# Dry-run
data-migrate run migration.yaml --dry-run
```

### 3. 현재 연결 설정 확인

```bash
data-migrate show-config
```

## YAML 설정 파일

### 기본 구조

```yaml
# 전역 설정
auto_order: true           # 외래키 기반 자동 순서 정렬
truncate: false            # 삽입 전 테이블 초기화
create_tables: true        # 테이블 자동 생성
exclude_date_tables: true  # 날짜 suffix 테이블 제외 (예: table_20240101)

# 정적 데이터베이스 목록
databases:
  - name: laplace
    mode: all
    exclude:
      - large_log_table

# 동적 데이터베이스 (쿼리 기반)
dynamic_databases:
  - pattern: "laplacian_{user_id}"
    lookup_query:
      database: laplace
      sql: "SELECT user_id FROM user_subscription WHERE ..."
    mode: all
```

### 정적 데이터베이스 설정

```yaml
databases:
  # 전체 테이블 마이그레이션
  - name: laplace
    mode: all
    exclude:
      - large_log_table
      - temp_table

  # 특정 테이블만
  - name: analytics
    target_name: analytics_backup  # 다른 이름의 타겟 DB로
    mode: tables
    tables:
      - users
      - orders
      - name: events
        where: "created_at >= '2024-01-01'"
        limit: 10000

  # 전체 테이블에 조건 적용
  - name: archive
    mode: all
    where: "updated_at >= '2024-01-01'"
    limit: 50000
```

### 동적 데이터베이스 설정

쿼리 결과를 기반으로 여러 데이터베이스를 자동 마이그레이션:

```yaml
dynamic_databases:
  - pattern: "laplacian_{user_id}"
    lookup_query:
      database: laplace  # 쿼리 실행할 DB
      sql: |
        SELECT user_id
        FROM user_subscription
        WHERE plan_end_datetime_id > '2025-12-05 00:00:00'
    mode: all
    exclude:
      - large_log_table
```

위 설정은 다음과 같이 동작:
1. `laplace` DB에서 SQL 실행
2. 결과로 나온 `user_id` 목록 (예: 10630, 10631, 10632)
3. 각각 `laplacian_10630`, `laplacian_10631`, `laplacian_10632` DB 마이그레이션

## 주요 기능

### 외래키 기반 자동 순서 정렬

`auto_order: true` 설정 시, 외래키 의존성을 분석하여 부모 테이블을 먼저 마이그레이션합니다.

### 날짜 테이블 자동 제외

`exclude_date_tables: true` (기본값) 설정 시, 테이블명 뒤에 날짜가 붙은 테이블을 자동 제외합니다.

매칭 패턴:
- `table_20240101` (YYYYMMDD)
- `table_240101` (YYMMDD)
- `table_2024_01_01` (YYYY_MM_DD)
- `table_2024-01-01` (YYYY-MM-DD)

날짜 테이블을 포함하려면:
```yaml
databases:
  - name: logs
    mode: all
    exclude_date_tables: false
```

또는 CLI에서:
```bash
data-migrate migrate -d logs --all --include-date-tables
```

### 스트리밍 마이그레이션

Server-Side Cursor를 사용하여 대용량 테이블도 메모리 효율적으로 마이그레이션합니다.
- 소스: 스트리밍으로 배치 단위 조회
- 타겟: INSERT IGNORE로 배치 단위 삽입 (중복 자동 스킵)

### 타겟 DB 자동 생성

타겟 데이터베이스가 없으면 생성 여부를 확인합니다.

## CLI 옵션

### `migrate` 명령어

| 옵션 | 설명 |
|------|------|
| `-d, --database` | 마이그레이션할 데이터베이스명 (필수) |
| `--target-database` | 타겟 데이터베이스명 (기본: 소스와 동일) |
| `--all` | 모든 테이블 마이그레이션 |
| `--table` | 특정 테이블 지정 (여러 개 가능) |
| `--exclude` | 제외할 테이블 |
| `--where` | WHERE 조건 |
| `--limit` | 조회 제한 건수 |
| `--truncate` | 삽입 전 테이블 초기화 |
| `--no-create-table` | 테이블 생성 건너뛰기 |
| `--auto-order/--no-auto-order` | FK 기반 자동 순서 정렬 (기본: 활성) |
| `--exclude-date-tables/--include-date-tables` | 날짜 테이블 제외 (기본: 제외) |
| `--dry-run` | 설정만 확인 |
| `--source-host`, `--source-port`, etc. | 소스 DB 연결 오버라이드 |
| `--target-host`, `--target-port`, etc. | 타겟 DB 연결 오버라이드 |

### `run` 명령어

```bash
data-migrate run <yaml_file> [--dry-run]
```

### `init` 명령어

```bash
data-migrate init  # migration.yaml 생성
```

### `show-config` 명령어

```bash
data-migrate show-config  # 현재 연결 설정 출력
```
