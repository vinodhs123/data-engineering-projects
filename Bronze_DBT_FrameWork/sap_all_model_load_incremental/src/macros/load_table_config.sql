{% macro load_table_config(source_system, table_name) %}
    {% set file_path = 'models/' ~ source_system ~ '/schema/' ~ table_name ~ '.yml' %}
    {{ log('Attempting to load configuration from: ' ~ file_path, info=True) }}
    {% set yaml_content = """
    {% include file_path %}
    """ %}
    {% set parsed_content = fromyaml(yaml_content.strip()) %}
    {{ return(parsed_content) }}
{% endmacro %}
