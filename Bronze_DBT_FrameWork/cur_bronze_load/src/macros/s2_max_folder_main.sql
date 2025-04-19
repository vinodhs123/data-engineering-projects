{% macro latest_folder_csv(source_path, files_format, delimiter, all_columns) %}

    {% set latest_folder = get_latest_folder(source_path,files_format) %}
    {% set encoded_path = decode_source_path(source_path ~ '/' ~ latest_folder) %}

    {{ config(
        materialized='table',
        alias=this.name
    ) }}

    {% if files_format == 'csv' %}
        {% set options_clause = "WITH (header = true, delimiter = '" ~ delimiter ~ "')" %}
    {% else %}
        {% set options_clause = "" %}
    {% endif %}

    SELECT
        {% for col in all_columns %}
            CAST({{ col.name }} AS {{ col.data_type }}) AS {{ col.name }}{% if not loop.last %}, {% endif %}
        {% endfor %}
    FROM {{ files_format }}.`{{ encoded_path }}/*.{{ files_format }}`
    {{ options_clause }}

{% endmacro %}
