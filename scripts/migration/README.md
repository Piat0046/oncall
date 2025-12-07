# MySQL 데이터 마이그레이션

소스 DB에서 타겟 DB로 테이블 데이터를 마이그레이션하는 도구입니다.

## 사전 준비

### 1. 환경변수 설정

프로젝트 루트에 `.env` 파일 생성:

```bash
cp ../../.env.example ../../.env
```

`.env` 파일 수정:

```bash
# 소스 DB (마이그레이션 원본)
SOURCE_DB_HOST=your-source-host.rds.amazonaws.com
SOURCE_DB_PORT=3306
SOURCE_DB_USER=your_user
SOURCE_DB_PASSWORD=your_password
SOURCE_DB_DATABASE=laplace

# 타겟 DB (마이그레이션 대상)
TARGET_DB_HOST=127.0.0.1
TARGET_DB_PORT=3307
TARGET_DB_USER=laplace
TARGET_DB_PASSWORD=laplace123
TARGET_DB_DATABASE=laplace
```

### 2. 타겟 DB (Docker) 실행

```bash
cd ../..
docker compose up -d
```

### 3. 가상환경 활성화

```bash
source ../../.venv/bin/activate
```

## 사용법

### 설정 확인

```bash
# 현재 DB 설정 확인
python migrate.py --show-config
```

### JSON 설정 파일로 마이그레이션

```bash
# 설정 파일 확인 (dry-run)
python migrate.py --config migrate_config.example.json --dry-run

# 실제 마이그레이션 실행
python migrate.py --config migrate_config.example.json
```

### 특정 테이블 마이그레이션

```bash
# 전체 데이터
python migrate.py --table users --full

# 조건부 데이터
python migrate.py --table users --where "created_at >= '2024-01-01'"

# 여러 테이블
python migrate.py --table users --table orders --full

# 건수 제한
python migrate.py --table users --full --limit 1000
```

### CLI 옵션으로 DB 설정 오버라이드

```bash
python migrate.py \
  --source-host other-host.rds.amazonaws.com \
  --source-database other_db \
  --target-host 127.0.0.1 \
  --target-port 3307 \
  --config migrate_config.example.json
```

## 옵션 설명

| 옵션 | 설명 |
|------|------|
| `--config <file>` | 마이그레이션 설정 JSON 파일 |
| `--table <name>` | 마이그레이션할 테이블 (여러 개 가능) |
| `--where <condition>` | WHERE 조건 |
| `--full` | 전체 데이터 마이그레이션 |
| `--limit <n>` | 조회 건수 제한 |
| `--truncate` | 삽입 전 타겟 테이블 초기화 |
| `--no-create-table` | 테이블 생성 건너뛰기 |
| `--dry-run` | 실제 실행 없이 설정만 확인 |
| `--show-config` | 현재 DB 설정 출력 |

## JSON 설정 파일 형식

`migrate_config.example.json`:

```json
{
  "tables": [
    {
      "name": "users",
      "where": "",
      "limit": null
    },
    {
      "name": "orders",
      "where": "created_at >= '2024-01-01'",
      "limit": 10000
    }
  ]
}
```

| 필드 | 설명 |
|------|------|
| `name` | 테이블명 (필수) |
| `where` | WHERE 조건 (빈 문자열 = 전체 데이터) |
| `limit` | 조회 건수 제한 (null = 제한 없음) |

## 문제 해결

### 소켓 연결 오류

```
Can't connect to local MySQL server through socket '/tmp/mysql.sock'
```

`localhost` 대신 `127.0.0.1` 사용:

```bash
TARGET_DB_HOST=127.0.0.1
```

### Docker 연결 오류

```bash
# 컨테이너 상태 확인
docker ps

# 컨테이너 재시작
docker compose restart

# 로그 확인
docker logs airflow-validator-mysql
```

### 외래키 오류

FK 참조가 있는 테이블은 참조되는 테이블을 먼저 마이그레이션:

```json
{
  "tables": [
    {"name": "plan", "where": "", "limit": null},
    {"name": "data_source", "where": "", "limit": null},
    {"name": "data_source_v2", "where": "", "limit": null}
  ]
}
```
