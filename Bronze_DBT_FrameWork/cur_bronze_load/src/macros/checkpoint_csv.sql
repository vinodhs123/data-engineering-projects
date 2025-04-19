{% macro update_checkpoint(source_path, checkpoint_table, files_format,delimiter) %}
{% set current_model_name = this.name %}
{% set encoded_path = encode_source_path(source_path) %}

INSERT INTO {{ checkpoint_table }}
SELECT * FROM (
    WITH file_paths_with_folders AS (
        SELECT
            _metadata.file_path,
            REPLACE(
                regexp_extract(_metadata.file_path, '{{ encoded_path }}/(.*?)/', 1),'%20',' ') AS folder_name
        FROM read_files(
            '{{ source_path }}/**/*.{{ files_format }}',
            format => 'text')
    ),
    new_folders AS ( 
        SELECT 
            a.model_name,
            a.folder_name,
            CURRENT_TIMESTAMP AS processed_at
        FROM 
            (SELECT DISTINCT folder_name,'{{ current_model_name }}' AS model_name FROM file_paths_with_folders) a
        LEFT JOIN {{ checkpoint_table }} b
        ON a.folder_name = b.folder_name
        AND a.model_name = b.model_name
        WHERE b.folder_name IS NULL
    )
    SELECT DISTINCT folder_name
    FROM new_folders
    WHERE folder_name IS NOT NULL
    ORDER BY folder_name ASC
    LIMIT 1
);
{% endmacro %}
