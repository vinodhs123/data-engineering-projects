{% macro latest_folder_csvs(source_path, files_format, delimiter, forecast_col, all_columns, tar_cols) %}

    {% set latest_folder = get_latest_folder_ibp(source_path,files_format) %}
    {% set encoded_path = decode_source_path(source_path ~ '/' ~ latest_folder) %}
    {% set current_model_name = this.name %}
    {% set target_cst_name = var('stg_catalog') ~ "." %}
    {% set main_target_tbl_name = target_cst_name ~ current_model_name %}
    {% set target_table = this.database ~ "." ~ this.schema ~ "." ~ this.name %}
    {% set staging_cols_sql = all_columns | join(',\n    ') %}
    {% set target_cols_sql = tar_cols | join(',\n    ') %}
    {% set sap_tbl_name = var('sap_tbl_name') %}

    {% set stage_sql_main = load_staging_data_ibp(latest_folder, main_target_tbl_name, files_format, staging_cols_sql, source_path, delimiter, forecast_col) %}
    {% set delete_sql =  delete_kfdate_range(target_table, main_target_tbl_name) %}

    {{ config(
        materialized='incremental',
        alias=this.name,
        pre_hook=[stage_sql_main,delete_sql]
    ) }}


    SELECT DISTINCT
        {{target_cols_sql}},
        CAST(STG.{{forecast_col}} AS DECIMAL(18,6)) AS FORECAST_QUANTITY
    FROM {{ main_target_tbl_name }} AS STG
    JOIN (
        SELECT MATNR
        FROM {{ sap_tbl_name }}
    ) AS M
        ON LTRIM(M.MATNR, '0') = LTRIM(STG.PRDID, '0')
    WHERE
        LEN(STG.PRDID) <= 18
    AND STG.{{forecast_col}} <> '<Null>';


{% endmacro %}
