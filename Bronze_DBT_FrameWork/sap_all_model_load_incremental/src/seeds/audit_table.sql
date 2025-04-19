CREATE TABLE IF NOT EXISTS {{ this.database }}.{{ this.schema }}.{{ config.get('audit_table', 'default_audit') }} (
    table_name STRING,
    process_type STRING,
    total_records_processed INT,
    unique_records_loaded INT,
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    status STRING,
    error_message STRING,
    run_id STRING
);
