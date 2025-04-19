{% macro create_type_one_views(seed_name) %}
{% set seed_table = ref(seed_name) %}
{% set seed_data = run_query('SELECT * FROM ' ~ seed_table) %}

{% if seed_data is none %}
    {% do exceptions.raise_compiler_error("Seed data is empty or not loaded properly.") %}
{% endif %}
{% for row in seed_data %}
    {% set source_table = row['source_table'] %}
    {% set target_catalog = row['target_catlog'] %}
    {% set target_schema = row['target_schema'] %}
    {% set unique_keys = row['unique_keys'] %}
    {% set updated_column = row['updated_column'] %}
    {% if source_table is none or target_catalog is none or target_schema is none or unique_keys is none or updated_column is none %}
        {% do exceptions.raise_compiler_error("Missing required fields in seed data for row: " ~ row) %}
    {% endif %}
        CREATE OR REPLACE VIEW {{ target_catalog }}.{{ target_schema }}.{{ source_table }} AS
        WITH ranked_data AS (
            SELECT
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY {{ unique_keys | replace(',', ', ') }}
                    ORDER BY {{ updated_column }} DESC
                ) AS row_num
            FROM {{ target_catalog }}.{{ target_schema }}.{{ source_table }}
        )
        SELECT
            *
        FROM ranked_data
        WHERE row_num = 1;
{% endfor %}
{% endmacro %}
