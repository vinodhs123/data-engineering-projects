{% macro load_tms_macro(
    source_path,
    merge_col,
    files_format,
    delimiter,
    all_columns,
    staging_columns,
    target_columns
) %}

{% set current_model_name = this.name %}
{% set target_cst_name = var('target_catalog') ~ "." ~ var('staging_schema') ~ "." %}
{% set staging_table = target_cst_name ~ current_model_name %}
{% set ingestion_id = run_started_at.timestamp() ~ "_" ~ current_model_name %}

{% set checkpoint_table = var('ops_catalog') ~ "." ~ var('checkpoint_table') %}
{% set target_cols_sql = target_columns | join(',\n    ') %}
{% set staging_cols_sql = staging_columns | join(',\n    ') %}

{% set target_table = this.database ~ "." ~ this.schema ~ "." ~ this.name %}

{% set latest_folder = get_latest_folder_test(source_path, files_format, checkpoint_table) %}

{% set stage_sql_main = load_staging_data_main(latest_folder, staging_table, files_format, all_columns, source_path, delimiter, ingestion_id) %}
{% set processfiles_insert_sql = "INSERT INTO " ~ checkpoint_table ~ " SELECT DISTINCT '" ~ current_model_name ~ "', folder, CURRENT_TIMESTAMP() FROM " ~ staging_table ~ " where _ingestion_id ='" ~ ingestion_id ~ "'"%}



{{ config(
            materialized='table',
            unique_key=None,
            pre_hook = [stage_sql_main],
            post_hook=[processfiles_insert_sql]
        ) }}

WITH cte AS (
    SELECT DISTINCT
        {{ staging_cols_sql }}
    FROM {{ staging_table }} a
),
cte1 as (
SELECT
     {{ target_cols_sql }}
FROM cte a
JOIN (
    SELECT {{merge_col}},
        MAX(DATEADD(
            SECOND,
            CAST(SUBSTRING(SourceFileName, CHARINDEX('-', SourceFileName, CHARINDEX('-', SourceFileName, 11)+1)+1, 10) AS INT),
            CAST('1970-01-01' AS TIMESTAMP)
        )) AS SourceFile_date
    FROM cte
    GROUP BY {{merge_col}}
) b ON a.{{merge_col}} = b.{{merge_col}}
AND DATEADD(
    SECOND,
    CAST(SUBSTRING(a.SourceFileName, CHARINDEX('-', a.SourceFileName, CHARINDEX('-', a.SourceFileName, 11)+1)+1, 10) AS INT),
    CAST('1970-01-01' AS TIMESTAMP)
) = b.SourceFile_date),
dst AS (
    SELECT
        {{merge_col}}, SourceFile_date FROM {{target_table}}
),
filtered_src AS (
    SELECT cte1.*
    FROM cte1
    LEFT JOIN dst ON cte1.{{merge_col}} = dst.{{merge_col}}
    WHERE dst.{{merge_col}} IS NULL OR cte1.SourceFile_date >= dst.SourceFile_date
)
SELECT *
FROM {{target_table}}
WHERE {{merge_col}} NOT IN (SELECT {{merge_col}} FROM filtered_src)
UNION
SELECT * FROM filtered_src where {{merge_col}} IS NOT NULL;

{% endmacro %}
