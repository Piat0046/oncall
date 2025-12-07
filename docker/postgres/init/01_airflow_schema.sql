-- Airflow 메타데이터 테이블 (테스트용 간소화 버전)

-- dag_run 테이블
CREATE TABLE IF NOT EXISTS dag_run (
    id SERIAL PRIMARY KEY,
    dag_id VARCHAR(250) NOT NULL,
    run_id VARCHAR(250) NOT NULL,
    execution_date TIMESTAMP WITH TIME ZONE NOT NULL,
    state VARCHAR(50),
    start_date TIMESTAMP WITH TIME ZONE,
    end_date TIMESTAMP WITH TIME ZONE,
    conf BYTEA,
    run_type VARCHAR(50),
    external_trigger BOOLEAN DEFAULT FALSE,
    UNIQUE (dag_id, run_id)
);

CREATE INDEX idx_dag_run_dag_id ON dag_run(dag_id);
CREATE INDEX idx_dag_run_state ON dag_run(state);
CREATE INDEX idx_dag_run_execution_date ON dag_run(execution_date);

-- task_instance 테이블
CREATE TABLE IF NOT EXISTS task_instance (
    dag_id VARCHAR(250) NOT NULL,
    task_id VARCHAR(250) NOT NULL,
    run_id VARCHAR(250),
    execution_date TIMESTAMP WITH TIME ZONE NOT NULL,
    state VARCHAR(20),
    start_date TIMESTAMP WITH TIME ZONE,
    end_date TIMESTAMP WITH TIME ZONE,
    duration DOUBLE PRECISION,
    try_number INTEGER,
    max_tries INTEGER DEFAULT -1,
    operator VARCHAR(1000),
    pool VARCHAR(256),
    queue VARCHAR(256),
    hostname VARCHAR(1000),
    PRIMARY KEY (dag_id, task_id, execution_date)
);

CREATE INDEX idx_ti_dag_id ON task_instance(dag_id);
CREATE INDEX idx_ti_state ON task_instance(state);
CREATE INDEX idx_ti_execution_date ON task_instance(execution_date);

-- 샘플 데이터 삽입
INSERT INTO dag_run (dag_id, run_id, execution_date, state, start_date, end_date, run_type)
VALUES
    ('naver_sync_dag', 'scheduled__2024-12-04', '2024-12-04 00:00:00+09', 'failed', '2024-12-04 01:00:00+09', '2024-12-04 01:30:00+09', 'scheduled'),
    ('kakao_sync_dag', 'scheduled__2024-12-04', '2024-12-04 00:00:00+09', 'failed', '2024-12-04 02:00:00+09', '2024-12-04 02:15:00+09', 'scheduled'),
    ('cafe24_sync_dag', 'scheduled__2024-12-04', '2024-12-04 00:00:00+09', 'success', '2024-12-04 03:00:00+09', '2024-12-04 03:45:00+09', 'scheduled');

INSERT INTO task_instance (dag_id, task_id, run_id, execution_date, state, duration, try_number, operator)
VALUES
    ('naver_sync_dag', 'fetch_data', 'scheduled__2024-12-04', '2024-12-04 00:00:00+09', 'failed', 120.5, 3, 'PythonOperator'),
    ('naver_sync_dag', 'transform_data', 'scheduled__2024-12-04', '2024-12-04 00:00:00+09', 'upstream_failed', NULL, 0, 'PythonOperator'),
    ('kakao_sync_dag', 'api_call', 'scheduled__2024-12-04', '2024-12-04 00:00:00+09', 'failed', 45.2, 2, 'PythonOperator');
