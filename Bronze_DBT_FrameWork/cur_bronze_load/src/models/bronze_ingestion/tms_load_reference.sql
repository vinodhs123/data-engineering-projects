{% set container_value1 = var('container_1') %}
{% set container_value2 = var('container_2') %}
{% set storage_account_value = var('storage_account') %}


{{ load_tms_macro(
    source_path="abfss://" ~ container_value2 ~ "@" ~ storage_account_value ~ ".dfs.core.windows.net/TMS/Temp_TMS_Load_Reference",
	merge_col = 'loadid',
    files_format = "csv",
    delimiter = '|',
    all_columns = [
        {"name": "processing_date", "data_type": "STRING"},
        {"name": "o_SourceFileName", "data_type": "STRING"},
        {"name": "loadid", "data_type": "INTEGER"},
        {"name": "reference", "data_type": "STRING"},
        {"name": "id", "data_type": "INTEGER"},
        {"name": "type", "data_type": "STRING"}
    ],
    staging_columns = [
        "a.reference","a.id","a.type",
        "TRIM(CAST(a.loadid AS CHAR(30))) AS loadid",
        "TO_TIMESTAMP(a.processing_date, 'MM/dd/yyyy HH:mm:ss.SSSSSS') AS processing_date",
        "a.o_SourceFileName AS SourceFileName"
    ],
    target_columns = [
        "CAST(a.reference AS STRING) AS reference",
        "CAST(a.id AS STRING) AS id",
        "CAST(a.type AS STRING) AS type",
        "CAST(a.loadid AS INT) AS loadid",
        "CAST(a.processing_date AS TIMESTAMP) AS processing_date",
        "CAST(a.SourceFileName AS STRING) AS SourceFileName",
        "DATEADD(SECOND, CAST(SUBSTRING(a.SourceFileName, CHARINDEX('-', a.SourceFileName, CHARINDEX('-', a.SourceFileName, 11)+1)+1, 10) AS INT), CAST('1970-01-01' AS TIMESTAMP)) AS SourceFile_date"
    ]
) }}