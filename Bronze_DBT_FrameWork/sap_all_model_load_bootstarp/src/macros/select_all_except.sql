{% macro get_columns_from_manifest(model_name) %}
    {% set model = graph.nodes['model.' ~ target.project ~ '.' ~ model_name] %}
    {% if model is none %}
        {{ exceptions.raise_compiler_error("Model schema for `" ~ model_name ~ "` not found in manifest.") }}
    {% endif %}
    {% set columns = model.columns %}
    {% if columns is none %}
        {{ exceptions.raise_compiler_error("No columns defined for model `" ~ model_name ~ "`.") }}
    {% endif %}
    {% set column_names = columns.keys() %}
    {{ column_names | join(", ") }}
{% endmacro %}
