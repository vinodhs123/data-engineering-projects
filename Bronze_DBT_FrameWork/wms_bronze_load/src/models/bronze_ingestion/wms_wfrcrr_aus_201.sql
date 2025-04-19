{% set container_value1 = var('container_1') %}
{% set storage_account_value = var('storage_account') %}
{% set incremental_location_value = var('incremental_location') %}

{{ load_wms_incremental(
    source_path="abfss://" ~ container_value1 ~ "@" ~ storage_account_value ~ ".dfs.core.windows.net/" ~ incremental_location_value ~ "/AUS/WMS_WFRCRR_AUS_201_parquet",
    all_columns = [
        {"name": "RRLCPL", "data_type": "DECIMAL(20,0)"},
        {"name": "RRPTYP", "data_type": "STRING"},
        {"name": "RRCSTM", "data_type": "DECIMAL(6,0)"},
        {"name": "RRPRDT", "data_type": "DECIMAL(7,0)"},
        {"name": "RRBOL_", "data_type": "STRING"},
        {"name": "RRLIN_", "data_type": "STRING"}
    ]
) }}