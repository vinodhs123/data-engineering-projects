{% set container_value1 = var('container_1') %}
{% set storage_account_value = var('storage_account') %}
{% set incremental_location_value = var('incremental_location') %}

{{ load_wms_incremental(
    source_path="abfss://" ~ container_value1 ~ "@" ~ storage_account_value ~ ".dfs.core.windows.net/" ~ incremental_location_value ~ "/CAN/WMS_TFWHS_CAN_99_parquet",
    all_columns = [
        {"name": "WHORG", "data_type": "STRING"},
        {"name": "WHWHS", "data_type": "STRING"},
        {"name": "WHWDSC", "data_type": "STRING"},
        {"name": "WHDTE", "data_type": "DECIMAL(7,0)"},
        {"name": "WHTME", "data_type": "DECIMAL(7,0)"},
        {"name": "WHUSR", "data_type": "STRING"}
    ]
) }}