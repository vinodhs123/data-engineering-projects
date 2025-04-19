{% macro new_folder_csv(source_path, files_format, delimiter, all_columns) %}

    {% set checkpoint_table = var('ops_catalog') ~ "." ~ var('checkpoint_table') %}
    {% set current_model_name = this.name %}

    {% set latest_folder = get_latest_folder_test(source_path, files_format, checkpoint_table) %}
    {% set initial_checkpoint = initial_checkpoint(checkpoint_table) %}
    {% set update_checkpoint_table = update_checkpoint(source_path, checkpoint_table,files_format,delimiter) %}


    {{ config(
        materialized='incremental',
        alias=this.name,
        unique_key=None,
        pre_hook=[initial_checkpoint, latest_folder],
        post_hook=[
            update_checkpoint_table
        ]
    ) }}

    {% if latest_folder | length > 0 %}
        {% set ctes = [] %}
        {% set selects = [] %}
        {% set options_clause = "" %}

        {# Iterate over list of folders #}
        {% for year in latest_folder %}
            {% set clean_year = year | trim %}
            {% set cte_name = files_format ~ "_" ~ clean_year %}

            {% if files_format == 'csv' %}
                {% set options_clause = "WITH (header = true, delimiter = '" ~ delimiter ~ "')" %}
            {% else %}
                {% set options_clause = "" %}
            {% endif %}

            {% set cte_query %}
                {{ cte_name }} AS (
                    SELECT
                        {% for col in all_columns %}
                            CAST({{ col.name }} AS {{ col.data_type }}) AS {{ col.name }}{% if not loop.last %}, {% endif %}
                        {% endfor %}
                    FROM {{ files_format }}.`{{ source_path }}/{{ year }}/*.{{ files_format }}`
                    {{ options_clause }}
                )
            {% endset %}
            {% do ctes.append(cte_query | trim) %}
            {% do selects.append("SELECT DISTINCT * FROM " ~ cte_name) %}
        {% endfor %}

        WITH
        {{ ctes | join(",\n") }}
        {{ selects | join("\nUNION ALL\n") }}

        {% else %}
            SELECT
                {% for col in all_columns %}
                    CAST(NULL AS {{ col.data_type }}) AS {{ col.name }}{% if not loop.last %}, {% endif %}
                {% endfor %}
            WHERE 1 = 0
    {% endif %}

{% endmacro %}
