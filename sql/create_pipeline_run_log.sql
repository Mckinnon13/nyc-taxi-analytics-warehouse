CREATE TABLE pipeline_run_log (
    id               SERIAL PRIMARY KEY,
    pipeline_run_id  VARCHAR(50) NOT NULL,
    processed_file_name VARCHAR(255) NOT NULL,
    year             INT NOT NULL,
    month            INT NOT NULL,
    row_count        BIGINT,
    status           VARCHAR(30) NOT NULL,  -- 'SUCCESS', 'FAILED', 'IN_PROGRESS'
    processed_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);