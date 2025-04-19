{% macro log_audit_and_analytics(source_system, table_name, process_type, status, run_id) %}
    {% set catalog_name = config.get('catalog', target.database) %}
    {% set schema_name = config.get('schema', target.schema) %}
    {% set audit_table_name = config.get('audit_table', 'audit') %}
    {% set analytics_table_name = config.get('analytics_table', 'analytics') %}

    {% set audit_table = catalog_name ~ '.' ~ schema_name ~ '.' ~ audit_table_name %}
    {% set analytics_table = catalog_name ~ '.' ~ schema_name ~ '.' ~ analytics_table_name %}

    CREATE TABLE IF NOT EXISTS {{ audit_table }} (
        table_name STRING,
        run_id STRING,
        run_date TIMESTAMP,
        process_type BOOLEAN,
        status STRING,
        record_count BIGINT,
        error_count BIGINT,
        processing_time_seconds DOUBLE
    );

    CREATE TABLE IF NOT EXISTS {{ analytics_table }} (
        table_name STRING,
        run_id STRING,
        run_date TIMESTAMP,
        duplicates_removed BIGINT,
        reliability_score DOUBLE
    );

    WITH audit_metrics AS (
        SELECT
            '{{ table_name }}' AS table_name,
            '{{ run_id }}' AS run_id,
            CURRENT_TIMESTAMP() AS run_date,
            {{ process_type }} AS process_type,
            '{{ status }}' AS status,
            COUNT(*) AS record_count,
            0 AS error_count, 
            UNIX_TIMESTAMP(CURRENT_TIMESTAMP()) - UNIX_TIMESTAMP('{{ run_id }}') AS processing_time_seconds
        FROM {{ this }}
    )
    INSERT INTO {{ audit_table }}
    SELECT * FROM audit_metrics;

    WITH analytics_metrics AS (
        SELECT
            '{{ table_name }}' AS table_name,
            '{{ run_id }}' AS run_id,
            CURRENT_TIMESTAMP() AS run_date,
            COUNT(*) - COUNT(DISTINCT {{ composite_keys | join(', ') }}) AS duplicates_removed,
            ROUND(COUNT(DISTINCT {{ composite_keys | join(', ') }}) / COUNT(*), 2) AS reliability_score
        FROM {{ this }}
    )
    INSERT INTO {{ analytics_table }}
    SELECT * FROM analytics_metrics;
{% endmacro %}
