#!/bin/bash
# 테스트 환경 설정 스크립트

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

cd "$PROJECT_DIR"

echo "=========================================="
echo "테스트 환경 설정"
echo "=========================================="

# 1. Docker 컨테이너 시작
echo ""
echo "[1/4] Docker 컨테이너 시작..."
docker compose up -d

# 2. 컨테이너 상태 확인 (헬스체크 대기)
echo ""
echo "[2/4] 컨테이너 준비 대기..."
sleep 10

# MySQL 연결 대기
echo "  MySQL 연결 대기 중..."
for i in {1..30}; do
    if docker exec airflow-validator-mysql mysqladmin ping -h localhost -u root -prootpassword --silent 2>/dev/null; then
        echo "  MySQL 준비 완료!"
        break
    fi
    sleep 2
done

# PostgreSQL 연결 대기
echo "  PostgreSQL 연결 대기 중..."
for i in {1..30}; do
    if docker exec airflow-validator-postgres pg_isready -U airflow -q 2>/dev/null; then
        echo "  PostgreSQL 준비 완료!"
        break
    fi
    sleep 2
done

# 3. 테스트 환경 파일 복사
echo ""
echo "[3/4] 테스트 환경 설정..."
cp .env.test .env.local

# 4. 프로덕션 DB에서 데이터 마이그레이션
echo ""
echo "[4/4] 데이터 마이그레이션..."
echo "  마이그레이션을 실행하려면:"
echo ""
echo "  source .venv/bin/activate"
echo "  python scripts/migrate_mysql.py \\"
echo "    --source-host laplace-analytics.chki0fzyxy39.ap-northeast-2.rds.amazonaws.com \\"
echo "    --source-user laplace \\"
echo "    --source-password 'laplace7&&*()' \\"
echo "    --target-host localhost \\"
echo "    --target-port 3307 \\"
echo "    --target-user laplace \\"
echo "    --target-password laplace123 \\"
echo "    --target-database laplace"
echo ""

echo "=========================================="
echo "테스트 환경 설정 완료!"
echo "=========================================="
echo ""
echo "컨테이너 상태:"
docker compose ps
echo ""
echo "테스트 실행:"
echo "  cp .env.test .env"
echo "  source .venv/bin/activate"
echo "  airflow-validator check-connection"
