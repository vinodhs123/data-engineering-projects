{% set container_value1 = var('container_1') %}
{% set storage_account_value = var('storage_account') %}
{% set incremental_location_value = var('incremental_location') %}

{{ load_wms_incremental(
    source_path="abfss://" ~ container_value1 ~ "@" ~ storage_account_value ~ ".dfs.core.windows.net/" ~ incremental_location_value ~ "/NZL/WMS_WFLTM_NZL_230_parquet",
    all_columns = [
        {"name": "LOLOT", "data_type": "STRING"},
        {"name": "LOITM", "data_type": "STRING"},
        {"name": "LOVRSN", "data_type": "STRING"},
        {"name": "LOEDTE", "data_type": "DECIMAL(7,0)"},
        {"name": "LOUDA", "data_type": "STRING"},
        {"name": "LOUDN", "data_type": "DECIMAL(9,0)"},
        {"name": "LOLSTS", "data_type": "STRING"},
        {"name": "LODTE", "data_type": "DECIMAL(7,0)"},
        {"name": "LOTME", "data_type": "DECIMAL(7,0)"},
        {"name": "LOUSR", "data_type": "STRING"}
    ]
) }}