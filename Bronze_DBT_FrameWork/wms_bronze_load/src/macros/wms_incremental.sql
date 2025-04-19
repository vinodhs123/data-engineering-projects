{% macro load_wms_incremental(
    source_path,
    all_columns
) %}

{% set cleaned = clean_column_names_with_types(all_columns) %}

{{ config(
            materialized='table'
        ) }}
SELECT DISTINCT
    {% for col in cleaned %}
        CAST(TRIM(`{{ col.raw }}`) AS {{ col.type }}) AS {{ col.clean }}{% if not loop.last %},{% endif %}
    {% endfor %},
    _metadata.file_path AS _filename,
    regexp_extract(_metadata.file_path, '([^/]+$)', 1) AS _file_basename,
    CURRENT_TIMESTAMP() AS _loadtime
FROM parquet.`{{ source_path }}` 
{% endmacro %}
