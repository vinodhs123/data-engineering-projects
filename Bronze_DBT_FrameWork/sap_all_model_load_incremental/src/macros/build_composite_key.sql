{% macro build_composite_key(source_system, table_name) %}
{% set table_config = load_table_config(source_system, table_name) %}
{% set composite_keys = table_config['composite_keys'] %}

CONCAT({% for key in composite_keys %}
    {{ key }}{% if not loop.last %}, '_', {% endif %}
{% endfor %}
)
{% endmacro %}
