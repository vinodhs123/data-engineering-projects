{% set container_value1 = var('container_1') %}
{% set storage_account_value = var('storage_account') %}
{% set incremental_location_value = var('incremental_location') %}

{{ load_wms_incremental(
    source_path="abfss://" ~ container_value1 ~ "@" ~ storage_account_value ~ ".dfs.core.windows.net/" ~ incremental_location_value ~ "/UK/WMS_XFQRC_UK_300_Parquet",
    all_columns = [
        {"name": "RCORG", "data_type": "STRING"},
        {"name": "RCTYP", "data_type": "STRING"},
        {"name": "RCID", "data_type": "STRING"},
        {"name": "RCDSC", "data_type": "STRING"}
    ]
) }}