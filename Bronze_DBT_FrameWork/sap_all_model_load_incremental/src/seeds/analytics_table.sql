CREATE TABLE IF NOT EXISTS {{ this.database }}.{{ this.schema }}.{{ config.get('analytics_table', 'default_analytics') }} (
    table_name STRING,
    run_date DATE,
    processing_time_seconds DOUBLE,
    duplicates_removed INT,
    error_count INT,
    reliability_score DOUBLE,
    run_id STRING
);
