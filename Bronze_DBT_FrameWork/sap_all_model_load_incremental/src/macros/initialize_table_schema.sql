{% macro initialize_table_schema(source_system, table_name) %}
{% set table_config = load_table_config(source_system, table_name) %}
{% set business_columns = table_config['columns'] %}
{% set metadata_columns = [
    {"name": "load_start_time", "type": "TIMESTAMP"},
    {"name": "load_end_time", "type": "TIMESTAMP"},
    {"name": "source_file", "type": "STRING"},
    {"name": "process_type", "type": "STRING"},
    {"name": "run_id", "type": "STRING"}
] %}
{% set all_columns = business_columns + metadata_columns %}

CREATE TABLE IF NOT EXISTS {{ target.database }}.{{ target.schema }}.{{ table_name }} (
    {% for column in all_columns %}
        {{ column['name'] }} {{ column['type'] }}{% if not loop.last %}, {% endif %}
    {% endfor %}
)
USING DELTA;
{% endmacro %}
