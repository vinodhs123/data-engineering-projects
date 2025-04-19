SELECT 
    table_name,
    process_type,
    total_records_processed,
    unique_records_loaded,
    start_time,
    end_time,
    status,
    run_id
FROM {{ this.database }}.{{ this.schema }}.{{ config.get('audit_table') }}
WHERE table_name = 'adr6'
ORDER BY end_time DESC;
