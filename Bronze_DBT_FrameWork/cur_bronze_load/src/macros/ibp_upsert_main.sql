{% macro latest_folder_ibp(source_path, files_format, delimiter, merge_col, all_columns) %}

    {% set latest_folder = get_latest_folder(source_path,files_format) %}
    {% set encoded_path = decode_source_path(source_path ~ '/' ~ latest_folder) %}
    {% set staging_cols_sql = all_columns | join(',\n    ') %}

    {{ config(
        materialized='incremental',
        unique_key=merge_col,
        alias=this.name
    ) }}

    {% if files_format == 'csv' %}
        {% set options_clause = "WITH (header = true, delimiter = '" ~ delimiter ~ "')" %}
    {% else %}
        {% set options_clause = "" %}
    {% endif %}

    SELECT DISTINCT
        {{ staging_cols_sql }}
    FROM {{ files_format }}.`{{ encoded_path }}/*.{{ files_format }}`
    {{ options_clause }}

{% endmacro %}
