{% set container_value1 = var('container_1') %}
{% set storage_account_value = var('storage_account') %}
{% set incremental_location_value = var('incremental_location') %}

{{ load_wms_incremental(
    source_path="abfss://" ~ container_value1 ~ "@" ~ storage_account_value ~ ".dfs.core.windows.net/" ~ incremental_location_value ~ "/NZL/WMS_WFIST_NZL_230_parquet",
    all_columns = [
        {"name": "ISWHS", "data_type": "STRING"},
        {"name": "ISISTS", "data_type": "STRING"},
        {"name": "ISDESC", "data_type": "STRING"},
        {"name": "ISHWHS", "data_type": "STRING"},
        {"name": "ISADJF", "data_type": "STRING"},
        {"name": "ISSTYP", "data_type": "STRING"},
        {"name": "ISRSTS", "data_type": "STRING"}
    ]
) }}