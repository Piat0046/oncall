#!/bin/bash
# LocalStack 초기화 스크립트 - S3, SecretsManager 설정

set -e

echo "=== LocalStack Initialization ==="

# AWS CLI 설정 (LocalStack용)
export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test
export AWS_DEFAULT_REGION=ap-northeast-2

# ============================================================
# S3 버킷 생성
# ============================================================
echo "Creating S3 buckets..."

awslocal s3 mb s3://warehouse || true
awslocal s3 mb s3://datalake || true
awslocal s3 mb s3://raw-data || true

echo "S3 buckets created successfully!"

# ============================================================
# SecretsManager 시크릿 생성
# ============================================================
echo "Creating SecretsManager secrets..."

# Database credentials
awslocal secretsmanager create-secret \
    --name "dev/database/mysql" \
    --description "MySQL database credentials for development" \
    --secret-string '{"host":"mysql","port":3306,"username":"laplace","password":"laplace123","database":"laplace"}' || true

awslocal secretsmanager create-secret \
    --name "dev/database/postgres" \
    --description "PostgreSQL database credentials for development" \
    --secret-string '{"host":"postgres","port":5432,"username":"airflow","password":"airflow123","database":"airflow"}' || true

# S3 credentials (LocalStack)
awslocal secretsmanager create-secret \
    --name "dev/storage/s3" \
    --description "LocalStack S3 credentials for development" \
    --secret-string '{"endpoint":"http://localstack:4566","access_key":"test","secret_key":"test","region":"ap-northeast-2"}' || true

# Trino credentials
awslocal secretsmanager create-secret \
    --name "dev/trino/connection" \
    --description "Trino connection info for development" \
    --secret-string '{"host":"trino","port":8080,"catalog":"hive","schema":"default"}' || true

# API keys (샘플)
awslocal secretsmanager create-secret \
    --name "dev/api/keys" \
    --description "Sample API keys for development" \
    --secret-string '{"airflow_api_key":"test-api-key-12345","internal_api_key":"internal-key-67890"}' || true

echo "SecretsManager secrets created successfully!"

echo "=== LocalStack Initialization Complete ==="
