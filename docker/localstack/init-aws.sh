#!/bin/bash
# LocalStack 초기화 스크립트 - S3, SecretsManager 설정

set -e

echo "=== LocalStack Initialization ==="

# ============================================================
# S3 버킷 생성
# ============================================================
echo "Creating S3 buckets..."

awslocal s3 mb s3://warehouse || true
awslocal s3 mb s3://datalake || true
awslocal s3 mb s3://raw-data || true
awslocal s3 mb s3://laplace-dashboard || true

echo "S3 buckets:"
awslocal s3 ls

# ============================================================
# SecretsManager 시크릿 생성
# ============================================================
echo "Creating SecretsManager secrets..."

# Database credentials
awslocal secretsmanager create-secret \
    --name "dev/database/mysql" \
    --description "MySQL database credentials" \
    --secret-string '{"host":"mysql","port":3306,"username":"laplace","password":"laplace123","database":"laplace"}' || true

awslocal secretsmanager create-secret \
    --name "dev/database/postgres" \
    --description "PostgreSQL database credentials" \
    --secret-string '{"host":"postgres","port":5432,"username":"airflow","password":"airflow123","database":"airflow"}' || true

# S3 credentials
awslocal secretsmanager create-secret \
    --name "dev/storage/s3" \
    --description "LocalStack S3 credentials" \
    --secret-string '{"endpoint":"http://localstack:4566","access_key":"test","secret_key":"test","region":"us-east-1"}' || true

# Trino connection
awslocal secretsmanager create-secret \
    --name "dev/trino/connection" \
    --description "Trino connection info" \
    --secret-string '{"host":"trino-coordinator","port":8080,"catalog":"iceberg","schema":"default"}' || true

echo "=== LocalStack Initialization Complete ==="
