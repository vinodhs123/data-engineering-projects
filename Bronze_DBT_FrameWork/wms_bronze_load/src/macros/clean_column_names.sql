{% macro clean_column_names_with_types(columns) %}
    {% set cleaned_columns = [] %}

    {% for col in columns %}
        {% set raw_name = col.name %}
        {% set col_type = col.data_type %}
        {% set cleaned_name = raw_name
            | replace('#', '')
            | lower
        %}
        {% do cleaned_columns.append({'raw': raw_name, 'clean': cleaned_name, 'type': col_type}) %}
    {% endfor %}

    {{ return(cleaned_columns) }}
{% endmacro %}
