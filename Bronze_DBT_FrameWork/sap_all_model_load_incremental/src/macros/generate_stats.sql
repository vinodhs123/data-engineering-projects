{% macro generate_full_stats_report(table_name, target_table, all_columns) %}
WITH column_stats AS (
    {% for col in all_columns %}
    SELECT
        '{{ table_name }}' AS source_table,
        '{{ run_started_at }}' AS ingestion_id,
        '{{ this.schema }}' AS schema_name,
        '{{ this.name }}' AS model_name,
        '{{ col.name }}' AS column_name,
        '{{ col.data_type }}' AS column_type,
        COUNT(*) AS total_count,
        COUNT(DISTINCT {{ col.name }}) AS distinct_count,
        SUM(CASE WHEN {{ col.name }} IS NULL THEN 1 ELSE 0 END) AS null_count,
        {% if col.data_type in ['INT', 'BIGINT', 'FLOAT', 'DOUBLE', 'DECIMAL'] %}
            MIN({{ col.name }}) AS min_value,
            MAX({{ col.name }}) AS max_value,
            AVG({{ col.name }}) AS avg_value,
            STDDEV({{ col.name }}) AS std_dev_value,
            NULL AS min_date,
            NULL AS max_date,
            NULL AS max_length,
            NULL AS min_length,
            NULL AS avg_length
        {% elif col.data_type in ['DATE', 'TIMESTAMP'] %}
            NULL AS min_value,
            NULL AS max_value,
            NULL AS avg_value,
            NULL AS std_dev_value,
            MIN({{ col.name }}) AS min_date,
            MAX({{ col.name }}) AS max_date,
            NULL AS max_length,
            NULL AS min_length,
            NULL AS avg_length
        {% elif col.data_type in ['STRING', 'VARCHAR', 'TEXT'] %}
            NULL AS min_value,
            NULL AS max_value,
            NULL AS avg_value,
            NULL AS std_dev_value,
            NULL AS min_date,
            NULL AS max_date,
            MAX(LENGTH({{ col.name }})) AS max_length,
            MIN(LENGTH({{ col.name }})) AS min_length,
            AVG(LENGTH({{ col.name }})) AS avg_length
        {% else %}
            NULL AS min_value,
            NULL AS max_value,
            NULL AS avg_value,
            NULL AS std_dev_value,
            NULL AS min_date,
            NULL AS max_date,
            NULL AS max_length,
            NULL AS min_length,
            NULL AS avg_length
        {% endif %}
    FROM {{ table_name }} where is_active = TRUE
    {% if not loop.last %} UNION ALL {% endif %}
    {% endfor %}
)

INSERT INTO {{ target_table }}
SELECT * FROM column_stats;
{% endmacro %}
