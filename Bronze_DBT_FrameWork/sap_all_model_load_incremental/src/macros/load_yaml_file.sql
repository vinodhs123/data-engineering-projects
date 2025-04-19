{% macro read_yaml(file_path) %}
    {# Load the YAML file and return the parsed content #}
    {% set yaml_content = load_yaml(open(file_path).read()) %}
    {{ return(yaml_content) }}
{% endmacro %}
