{% macro optimize(relation, config) %}
    {{ log("Optimize relation: " ~ relation, info=True) }}
    {{ log("Optimize config: " ~ config, info=True) }}
    {% if relation is none or config is none %}
        {{ exceptions.raise_compiler_error("Optimize macro received a NoneType argument.") }}
    {% endif %}
{% endmacro %}
