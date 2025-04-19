{% set container_value1 = var('container_1') %}
{% set storage_account_value = var('storage_account') %}
{% set incremental_location_value = var('incremental_location') %}

{{ load_wms_incremental(
    source_path="abfss://" ~ container_value1 ~ "@" ~ storage_account_value ~ ".dfs.core.windows.net/" ~ incremental_location_value ~ "/AUS/WMS_XFQAH_AUS_201_parquet",
    all_columns = [
        {"name": "QHID", "data_type": "STRING"},
        {"name": "QHLOT", "data_type": "STRING"},
        {"name": "QHITM", "data_type": "STRING"},
        {"name": "QHVRSN", "data_type": "STRING"},
        {"name": "QHLCPL", "data_type": "DECIMAL(20,0)"},
        {"name": "QHLSTS", "data_type": "STRING"},
        {"name": "QHEDTE", "data_type": "DECIMAL(7,0)"},
        {"name": "QHDTE", "data_type": "DECIMAL(7,0)"},
        {"name": "QHTME", "data_type": "DECIMAL(7,0)"},
        {"name": "QHUSR", "data_type": "STRING"},
        {"name": "QHEDSC", "data_type": "STRING"},
        {"name": "QHRC", "data_type": "STRING"},
        {"name": "QHHQTY", "data_type": "DECIMAL(13,0)"},
        {"name": "QHWHS", "data_type": "STRING"}
    ]
) }}