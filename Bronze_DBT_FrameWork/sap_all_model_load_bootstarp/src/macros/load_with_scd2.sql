{% macro load_data_with_metadata(
    historical_source,
    incremental_source,
    all_columns,
    composite_keys,
    transformation_column="tas_rp_opcode",
    change_date_column="tas_rp_change_date",
    transformation_map={"D": 0, "I": 1, "U": 2, '1':1,'2':2,'0':0},
    load_type_historical="Bootstrap",
    load_type_incremental="Incremental"
) %}
{% set current_model_name = this.name %}
{% set audit_table = var('ops_catalog') ~ ".edp_ops.audit" %}
{% set stats_report = var('ops_catalog') ~ ".edp_ops.stats_report" %}
{% set analytics_table = var('ops_catalog') ~ ".edp_ops.analytics" %}
{% set checkpoint_table = var('ops_catalog') ~ ".edp_ops.checkpoint" %}

{% set ingestion_id = run_started_at.timestamp() ~ "_" ~ current_model_name %}

{% do log("ingestion_id: " ~ ingestion_id, info=True) %}
{% do log("Audit table: " ~ audit_table, info=True) %}
{% do log("Analytics table: " ~ analytics_table, info=True) %}

{% set audit_insert_sql = "INSERT INTO " ~ audit_table ~ " SELECT '" ~ current_model_name ~ "', '" ~ ingestion_id ~ "' ,'" ~ run_started_at ~ "', CURRENT_TIMESTAMP(), " ~ 'True' ~ ", 'success', COUNT(*), 0, UNIX_TIMESTAMP(CURRENT_TIMESTAMP()) - UNIX_TIMESTAMP('" ~ run_started_at ~ "') FROM " ~ this ~ " where _ingestion_id ='" ~ ingestion_id ~ "'" %}
{% set analytics_insert_sql = "INSERT INTO " ~ analytics_table ~ " SELECT '" ~ current_model_name ~ "', '" ~ run_started_at ~ "', CURRENT_TIMESTAMP(), COUNT(*) - COUNT(DISTINCT " ~ composite_keys | join(', ') ~ "), ROUND(COUNT(DISTINCT " ~ composite_keys | join(', ') ~ ") / COUNT(*), 2) FROM " ~ this ~ " where _ingestion_id ='" ~ ingestion_id ~ "'"%}
{% set processfiles_insert_sql = "INSERT INTO " ~ checkpoint_table ~ " SELECT DISTINCT '" ~ current_model_name ~ "', _filename, CURRENT_TIMESTAMP() FROM " ~ this ~ " where _ingestion_id ='" ~ ingestion_id ~ "'"%}



{% set audit_running_sql = "INSERT INTO " ~ audit_table ~ " SELECT '" ~ current_model_name ~ "', '" ~ ingestion_id ~ "', '" ~ run_started_at ~ "', CURRENT_TIMESTAMP(), true, 'Running', 0, 0, NULL" %}
{% set audit_success_update_sql = "UPDATE " ~ audit_table ~ " SET status = 'Success', record_count = (SELECT COUNT(*) FROM " ~ this ~ " WHERE _ingestion_id = '" ~ ingestion_id ~ "'), error_count = 0, processing_time_seconds = UNIX_TIMESTAMP(CURRENT_TIMESTAMP()) - UNIX_TIMESTAMP('" ~ run_started_at ~ "') WHERE table_name = '" ~ current_model_name ~ "' AND _ingestion_id
 = '" ~ ingestion_id ~ "'" %}

{% set stats_generation_sql = generate_full_stats_report(this,stats_report, all_columns) %}

{{ config(
            materialized='table', 
            post_hook=[
                processfiles_insert_sql
            ]
        ) }}
with 
historical_data AS (
    SELECT {{ change_date_column }} AS change_datetime,
        {% for col in all_columns %}
            {% if col.name == transformation_column %}
                CAST({{ col.name }} AS STRING) AS {{ col.name }}{% if not loop.last %}, {% endif %}
            {% else %}
                CAST({{ col.name }} AS {{ col.data_type }}) AS {{ col.name }}{% if not loop.last %}, {% endif %}
            {% endif %}
        {% endfor %},
        _metadata.file_path AS _filename,
        regexp_extract(_metadata.file_path, '([^/]+$)', 1) AS _file_basename,
        CURRENT_TIMESTAMP() AS _loadtime,
        '{{ load_type_historical }}' AS _loadtype
    FROM parquet.`{{ historical_source }}`
),
deactivated_historical AS (
    SELECT 
        h.*,
        TRUE AS is_active,
        '1900-01-01 00:00:00' AS valid_from,
        '9999-01-01 00:00:00' AS valid_to
    FROM historical_data h
)
SELECT 
    {% for col in all_columns %}
            {% if col.name != transformation_column %}
                {{ col.name }}{% if not loop.last %}, {% endif %}
            {% endif %}
        {% endfor %}
    '{{ ingestion_id }}' AS _ingestion_id,_filename,_file_basename,_loadtime,_loadtype,is_active,valid_from,valid_to,
    CASE 
        WHEN upper({{ transformation_column }}) = 'I' THEN 1
        WHEN upper({{ transformation_column }}) = 'U' THEN 2
        WHEN upper({{ transformation_column }}) = 'D' THEN 0
        ELSE CAST({{ transformation_column }} AS INT)
    END AS {{ transformation_column }},change_datetime
FROM deactivated_historical

{% endmacro %}
