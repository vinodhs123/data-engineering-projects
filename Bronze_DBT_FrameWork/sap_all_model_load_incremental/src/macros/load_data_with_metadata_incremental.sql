{% macro load_data_with_metadata(
    historical_source,
    incremental_source,
    all_columns,
    composite_keys,
    transformation_column="tas_rp_opcode",
    transformation_map={"D": 0, "I": 1, "U": 2},
    load_type="incremental"
) %}

{% set audit_table = var('ops_catalog') ~ ".edp_ops.audit" %}

{% set analytics_table = var('ops_catalog') ~ ".edp_ops.analytics" %}
{% set checkpoint_table = var('ops_catalog') ~ ".edp_ops.checkpoint" %}
{% set current_model_name = this.name %}
{% set change_date_column = "tas_rp_change_date" %}
{% set target_cst_name = var('target_catalog') ~ ".sap_staging." %}
{% set main_target_tbl_name = target_cst_name ~ current_model_name %}
{% set ingestion_id = run_started_at.timestamp() ~ "_" ~ current_model_name %}

{% do log("ingestion_id: " ~ ingestion_id, info=True) %}
{% do log("Audit table: " ~ audit_table, info=True) %}
{% do log("Analytics table: " ~ analytics_table, info=True) %}
{% set stats_report = var('ops_catalog') ~ ".edp_ops.stats_report" %}
{% set audit_insert_sql = "INSERT INTO " ~ audit_table ~ " SELECT '" ~ current_model_name ~ "', '" ~ ingestion_id ~ "' ,'" ~ run_started_at ~ "', CURRENT_TIMESTAMP(), " ~ 'True' ~ ", 'success', COUNT(*), 0, UNIX_TIMESTAMP(CURRENT_TIMESTAMP()) - UNIX_TIMESTAMP('" ~ run_started_at ~ "') FROM " ~ this ~ " where _ingestion_id ='" ~ ingestion_id ~ "'" %}
{% set analytics_insert_sql = "INSERT INTO " ~ analytics_table ~ " SELECT '" ~ current_model_name ~ "', '" ~ run_started_at ~ "', CURRENT_TIMESTAMP(), COUNT(*) - COUNT(DISTINCT " ~ composite_keys | join(', ') ~ "), ROUND(COUNT(DISTINCT " ~ composite_keys | join(', ') ~ ") / COUNT(*), 2) FROM " ~ this ~ " where _ingestion_id ='" ~ ingestion_id ~ "'"%}
{% set processfiles_insert_sql = "INSERT INTO " ~ checkpoint_table ~ " SELECT DISTINCT '" ~ current_model_name ~ "', _filename, CURRENT_TIMESTAMP() FROM " ~ this ~ " where _ingestion_id ='" ~ ingestion_id ~ "'"%}
{% set stats_generation_sql = generate_full_stats_report(this,stats_report, all_columns) %}
{% set stage_sql = load_staging_data(ingestion_id,current_model_name, main_target_tbl_name,incremental_source, all_columns,composite_keys,change_date_column) %}
{% set invalidate_sql = invalidate_target_records(this, main_target_tbl_name, composite_keys,change_date_column) %}

-- stage_sql,invalidate_sql
{{ config(
            materialized='incremental', 
            unique_key=None,
            pre_hook=[stage_sql,invalidate_sql],
            post_hook=[
                audit_insert_sql,
                processfiles_insert_sql
            ]
        ) }}
SELECT 
        *,
{{ change_date_column }} as change_datetime
FROM {{ main_target_tbl_name }};
{% endmacro %}
