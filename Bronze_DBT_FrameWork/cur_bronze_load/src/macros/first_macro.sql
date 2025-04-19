{% macro first_macro(source_path, file_type, delimiter) %}

{% if file_type == 'parquet' %}
    WITH incremental_raw AS (
        SELECT * FROM parquet.`{{ source_path }}`
    )select * from incremental_raw;
{% else %}
    {# Commented Part == Handle unsupported file types or provide a default behavior  #}
    {{ exceptions.raise_compiler_error("Unsupported file type: " ~ file_type ~ ". Supported types are 'parquet' and 'csv'.") }}
{% endif %}

{% endmacro %}