{% macro load_staging_data_ibp(latest_folder, staging_table, files_format, all_columns, source_path, delimiter, forecast_col) %}
    {% if latest_folder | length > 0 %}
        {% set ctes = [] %}
        {% set selects = [] %}

        {% for folder in latest_folder %}
            {% set cte_name = (files_format ~ "_" ~ folder) | replace("-", "_") | replace("/", "_") | replace(".", "_") | replace(" ", "_") %}

            {% if files_format == 'csv' %}
                {% set options_clause = "WITH (header = true, delimiter = '" ~ delimiter ~ "')" %}
            {% else %}
                {% set options_clause = "" %}
            {% endif %}

            {% set cte_query %}
                {{ cte_name }} AS (
                    SELECT DISTINCT
                        {{ all_columns }}
                    FROM {{ files_format }}.`{{ source_path }}/{{ folder }}/*.{{ files_format }}`
                    {{ options_clause }}
                    {% if forecast_col == 'ZLATESTRELM1' %}
                        WHERE LAG = 1
                    {% endif %}
                    {% if forecast_col == 'ZLATESTRELM2' %}
                        WHERE LAG = 2
                    {% endif %}
                )
            {% endset %}

            {% do ctes.append(cte_query | trim) %}
            {% do selects.append("SELECT * FROM " ~ cte_name) %}
        {% endfor %}

        {% set final_sql %}
            CREATE OR REPLACE TABLE {{ staging_table }} AS
            WITH
            {{ ctes | join(",\n") }}
            {{ selects | join("\nUNION ALL\n") }}
        {% endset %}

        {# {% do log("Final rendered SQL:\n" ~ final_sql, info=True) %} #}

        {% do run_query(final_sql) %}

    {% else %}
        {% set final_sql %}
            CREATE OR REPLACE TABLE {{ staging_table }} AS
            SELECT
                {% for col in all_columns %}
                    CAST(NULL AS {{ col.data_type }}) AS {{ col.name }}{% if not loop.last %}, {% endif %}
                {% endfor %}
            WHERE 1 = 0
        {% endset %}

        {% do run_query(final_sql) %}

    {% endif %}
{% endmacro %}