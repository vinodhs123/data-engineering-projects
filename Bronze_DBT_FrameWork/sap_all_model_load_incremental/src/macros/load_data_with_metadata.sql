{% macro load_data_with_metadata_back(
    historical_source,
    incremental_source,
    all_columns,
    composite_keys,
    transformation_column="tas_rp_opcode",
    transformation_map={"D": 0, "I": 1, "U": 2},
    load_type="bootstrap"
) %}

{% set audit_table = var('ops_catalog') ~ ".edp_ops.audit" %}

{% set analytics_table = var('ops_catalog') ~ ".edp_ops.analytics" %}
{% set checkpoint_table = var('ops_catalog') ~ ".edp_ops.checkpoint" %}
{% set current_model_name = this.name %}
{% set ingestion_id = run_started_at.timestamp() ~ "_" ~ current_model_name %}

{% do log("ingestion_id: " ~ ingestion_id, info=True) %}
{% do log("Audit table: " ~ audit_table, info=True) %}
{% do log("Analytics table: " ~ analytics_table, info=True) %}

{% set audit_insert_sql = "INSERT INTO " ~ audit_table ~ " SELECT '" ~ current_model_name ~ "', '" ~ ingestion_id ~ "' ,'" ~ run_started_at ~ "', CURRENT_TIMESTAMP(), " ~ 'True' ~ ", 'success', COUNT(*), 0, UNIX_TIMESTAMP(CURRENT_TIMESTAMP()) - UNIX_TIMESTAMP('" ~ run_started_at ~ "') FROM " ~ this ~ " where _ingestion_id ='" ~ ingestion_id ~ "'" %}
{% set analytics_insert_sql = "INSERT INTO " ~ analytics_table ~ " SELECT '" ~ current_model_name ~ "', '" ~ run_started_at ~ "', CURRENT_TIMESTAMP(), COUNT(*) - COUNT(DISTINCT " ~ composite_keys | join(', ') ~ "), ROUND(COUNT(DISTINCT " ~ composite_keys | join(', ') ~ ") / COUNT(*), 2) FROM " ~ this ~ " where _ingestion_id ='" ~ ingestion_id ~ "'"%}
{% set processfiles_insert_sql = "INSERT INTO " ~ checkpoint_table ~ " SELECT DISTINCT '" ~ current_model_name ~ "', _filename, CURRENT_TIMESTAMP() FROM " ~ this ~ " where _ingestion_id ='" ~ ingestion_id ~ "'"%}


{{ config(
            materialized='table', 
            post_hook=[
                audit_insert_sql,
                analytics_insert_sql,
                processfiles_insert_sql
            ]
        ) }}
WITH historical_raw AS (
    SELECT 
        {% for col in all_columns %}
            {{ col }}{% if not loop.last %}, {% endif %}
        {% endfor %},
        _metadata.file_path AS _filename,
        regexp_extract(_metadata.file_path, '([^/]+$)', 1) AS _file_basename,
        CURRENT_TIMESTAMP() AS _loadtime,
        '{{ load_type }}' AS _loadtype
    FROM parquet.`{{ historical_source }}`
),
incremental_raw AS (
    SELECT 
        {% for col in all_columns %}
            {{ col }}{% if not loop.last %}, {% endif %}
        {% endfor %},
        _metadata.file_path AS _filename,
        regexp_extract(_metadata.file_path, '([^/]+$)', 1) AS _file_basename,
        CURRENT_TIMESTAMP() AS _loadtime,
        '{{ load_type }}' AS _loadtype
    FROM parquet.`{{ incremental_source }}`
),
transformed_historical AS (
    SELECT 
        {% for col in all_columns %}
            {% if col != transformation_column %}
                {{ col }}{% if not loop.last %}, {% endif %}
            {% endif %}
        {% endfor %}
        CASE 
            {% for key, value in transformation_map.items() %}
                WHEN UPPER({{ transformation_column }}) = '{{ key }}' THEN {{ value }}
            {% endfor %}
            ELSE NULL
        END AS {{ transformation_column }},
        _filename,
        _file_basename,
        _loadtime,
        _loadtype
    FROM historical_raw
),
deduplicated_historical AS (
    SELECT *
    FROM transformed_historical hist
    WHERE NOT EXISTS (
        SELECT 1
        FROM incremental_raw inc
        WHERE 
            {% for key in composite_keys %}
                hist.{{ key }} = inc.{{ key }}
                {% if not loop.last %} AND {% endif %}
            {% endfor %}
    )
)
SELECT *, '{{ ingestion_id }}' AS _ingestion_id 
FROM deduplicated_historical
UNION ALL
SELECT *, '{{ ingestion_id }}' AS _ingestion_id
FROM incremental_raw
{% endmacro %}