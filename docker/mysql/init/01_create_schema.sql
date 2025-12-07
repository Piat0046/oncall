-- laplace 스키마 생성
CREATE DATABASE IF NOT EXISTS laplace;
USE laplace;

-- data_meta 테이블
CREATE TABLE IF NOT EXISTS data_meta (
    data_id BIGINT PRIMARY KEY AUTO_INCREMENT,
    data_source_id BIGINT,
    provider VARCHAR(50) NOT NULL,
    data_category VARCHAR(50),
    mall_id VARCHAR(100),
    mall_name VARCHAR(255),
    data_set_name VARCHAR(255),
    daily_yn CHAR(1) DEFAULT 'N',
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_provider (provider),
    INDEX idx_data_source_id (data_source_id)
);

-- user_data_log 테이블
CREATE TABLE IF NOT EXISTS user_data_log (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    user_id BIGINT NOT NULL,
    data_id BIGINT NOT NULL,
    use_yn CHAR(1) DEFAULT 'Y',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_user_id (user_id),
    INDEX idx_data_id (data_id)
);

-- cafe_24_app_install 테이블
CREATE TABLE IF NOT EXISTS cafe_24_app_install (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    mall_id VARCHAR(100) NOT NULL,
    install_yn CHAR(1) DEFAULT 'N',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_mall_id (mall_id)
);

-- user_subscription 테이블
CREATE TABLE IF NOT EXISTS user_subscription (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    user_id BIGINT NOT NULL,
    plan_end_datetime_id TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_user_id (user_id)
);

-- data_source_status 테이블
CREATE TABLE IF NOT EXISTS data_source_status (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    data_source_id BIGINT NOT NULL,
    user_id BIGINT NOT NULL,
    status VARCHAR(50),
    use_yn CHAR(1) DEFAULT 'Y',
    synced_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_data_source_id (data_source_id),
    INDEX idx_user_id (user_id)
);
