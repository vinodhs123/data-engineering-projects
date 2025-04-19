
{% macro encode_source_path(source_path) %}
    {# Replaces spaces with %20 in a given path #}
    {% do return(source_path | replace(" ", "%20")) %}
{% endmacro %}

{% macro decode_source_path(encoded_path) %}
    {# Replaces %20 with space in a given path #}
    {% do return(encoded_path | replace("%20", " ")) %}
{% endmacro %}

{% macro get_latest_folder_ibp(source_path,files_format) %}
    {#
        Reads all distinct folders from a source path and returns the maximum one (numerically sorted).
        Automatically encodes the path to avoid space-related issues.
    #}
    {% set encoded_path = encode_source_path(source_path) %}

    {% set sql %}
        WITH latest_folder AS (
            SELECT folder_name
            FROM (
                SELECT DISTINCT
                    REPLACE(
                        regexp_extract(_metadata.file_path, '{{ encoded_path }}/([^/]+)/', 1), '%20', ' '
                    ) AS folder_name,
                    TRY_CAST(
                        REPLACE(
                            regexp_extract(_metadata.file_path, '{{ encoded_path }}/([^/]+)/', 1), '%20', ' '
                        ) AS BIGINT
                    ) AS folder_num
                FROM read_files(
                    '{{ source_path }}/'|| date_format(current_date(), 'yyyyMM') ||'*/*.{{ files_format }}',
                    format => 'text'
                )
			UNION ALL
                SELECT DISTINCT
                    REPLACE(
                        regexp_extract(_metadata.file_path, '{{ encoded_path }}/([^/]+)/', 1), '%20', ' '
                    ) AS folder_name,
                    TRY_CAST(
                        REPLACE(
                            regexp_extract(_metadata.file_path, '{{ encoded_path }}/([^/]+)/', 1), '%20', ' '
                        ) AS BIGINT
                    ) AS folder_num
                FROM read_files(
                    '{{ source_path }}/'|| date_format(add_months(current_date(), -1), 'yyyyMM') ||'*/*.{{ files_format }}',
                    format => 'text'
                )
            ) folders
            ORDER BY folder_num DESC
            LIMIT 1
        )
        SELECT folder_name FROM latest_folder
    {% endset %}


    {% if execute %}
        {% set result = run_query(sql) %}
        {% set folder_names = result.columns[0].values()| list %}
        {{ return(folder_names) }}
    {% endif %}
{% endmacro %}

{% macro get_latest_folder(source_path,files_format) %}
    {#
        Reads all distinct folders from a source path and returns the maximum one (numerically sorted).
        Automatically encodes the path to avoid space-related issues.
    #}
    {% set encoded_path = encode_source_path(source_path) %}

    {% set sql %}
        WITH latest_folder AS (
            SELECT folder_name
            FROM (
                SELECT DISTINCT
                    REPLACE(
                        regexp_extract(_metadata.file_path, '{{ encoded_path }}/([^/]+)/', 1), '%20', ' '
                    ) AS folder_name,
                    TRY_CAST(
                        REPLACE(
                            regexp_extract(_metadata.file_path, '{{ encoded_path }}/([^/]+)/', 1), '%20', ' '
                        ) AS BIGINT
                    ) AS folder_num
                FROM read_files(
                    '{{ source_path }}/**/*.{{ files_format }}',
                    format => 'text'
                )
            ) folders
            ORDER BY folder_num DESC
            LIMIT 1
        )
        SELECT folder_name FROM latest_folder
    {% endset %}


    {% if execute %}
        {% set result = run_query(sql) %}
        {% set folder_name = result.columns[0].values()[0] %}
        {{ return(folder_name) }}
    {% endif %}
{% endmacro %}


{% macro get_latest_folder_test(source_path, files_format, checkpoint_table) %}
    {% set encoded_path = encode_source_path(source_path) %}
    {% set current_model_name = this.identifier %}

    {% set sql %}
        WITH file_paths_with_folders AS (
            SELECT
                _metadata.file_path,
                REPLACE(
                    regexp_extract(_metadata.file_path, '{{ encoded_path }}/(.*?)/', 1),
                    '%20', ' '
                ) AS folder_name
            FROM read_files(
                '{{ source_path }}/**/*.{{ files_format }}',
                format => 'text'
            )
        ),
        distinct_folders AS (
            SELECT DISTINCT folder_name, '{{ current_model_name }}'  AS model_name
            FROM file_paths_with_folders
        ),
        new_folders AS (
            SELECT df.folder_name
            FROM distinct_folders df
            LEFT JOIN {{ checkpoint_table }} cp
            ON df.folder_name = cp.folder_name
            AND df.model_name = cp.model_name
            WHERE cp.folder_name IS NULL
        )
        SELECT DISTINCT folder_name
        FROM new_folders
        WHERE folder_name IS NOT NULL
        ORDER BY folder_name ASC
        LIMIT 1
    {% endset %}

    {% if execute %}
        {% set result = run_query(sql) %}
        {% if result %}
            {% set folder_names = result.columns[0].values() %}
            {{ return(folder_names) }}
        {% else %}
            {% do log("No folders found.", info=True) %}
            {{ return([]) }}
        {% endif %}
    {% endif %}
{% endmacro %}



