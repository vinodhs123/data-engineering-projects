{% macro load_staging_data(ingestion_id, current_model_name, staging_table, incremental_source, all_columns, composite_keys, change_date_column, timekey_column = 'timekey_col') %}
{% set checkpoint_table = var('ops_catalog') ~ ".edp_ops.checkpoint" %}
CREATE OR REPLACE TABLE {{ staging_table }} AS
WITH incremental_raw AS (
    SELECT 
        {% for col in all_columns %}
            CAST({{ col.name }} AS {{ col.data_type }}) AS {{ col.name }}{% if not loop.last %}, {% endif %}
        {% endfor %},
        _metadata.file_path AS _filename,
        regexp_extract(_metadata.file_path, '([^/]+$)', 1) AS _file_basename,
        CURRENT_TIMESTAMP() AS _loadtime,
        'Incremental' AS _loadtype,
        '{{ingestion_id}}' AS _ingestion_id,
        tas_rp_change_date AS valid_from,
        timekey_col AS timekey_col
    FROM parquet.`{{ incremental_source }}` 
    WHERE _metadata.file_path NOT IN 
    (SELECT file_name FROM {{ checkpoint_table }} WHERE table_name= '{{current_model_name}}')
),

ranked_data AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY {% for key in composite_keys %} {{ key }} {% if not loop.last %}, {% endif %}{% endfor %}
            ORDER BY {{ change_date_column }} DESC, {{ timekey_column }} DESC
        ) AS row_num,
        LEAD({{ change_date_column }}) OVER (
            PARTITION BY {% for key in composite_keys %} {{ key }} {% if not loop.last %}, {% endif %}{% endfor %}
            ORDER BY {{ change_date_column }} DESC, {{ timekey_column }} DESC
        ) AS next_valid_to
    FROM incremental_raw
)

SELECT *, 
    CASE 
        WHEN row_num = 1 THEN '9999-01-01 00:00:00'
        ELSE next_valid_to 
    END AS valid_to,
    CASE 
        WHEN row_num = 1 THEN TRUE 
        ELSE FALSE 
    END AS is_active
FROM ranked_data;
{% endmacro %}
