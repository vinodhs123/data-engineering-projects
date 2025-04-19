{% set container_value1 = var('container_1') %}
{% set storage_account_value = var('storage_account') %}
{% set incremental_location_value = var('incremental_location') %}

{{ load_wms_incremental(
    source_path="abfss://" ~ container_value1 ~ "@" ~ storage_account_value ~ ".dfs.core.windows.net/" ~ incremental_location_value ~ "/COL/WMS_WFITW_COL_500_parquet",
    all_columns = [
        {"name": "IWWHS", "data_type": "STRING"},
        {"name": "IWITM", "data_type": "STRING"},
        {"name": "IWVRSN", "data_type": "STRING"},
        {"name": "IWSCAT", "data_type": "STRING"},
        {"name": "IWPCAT", "data_type": "STRING"},
        {"name": "IWICAT", "data_type": "STRING"},
        {"name": "IWCOMC", "data_type": "STRING"},
        {"name": "IWPLST", "data_type": "DECIMAL(1,0)"},
        {"name": "IWRCAT", "data_type": "STRING"},
        {"name": "IWRCPK", "data_type": "DECIMAL(5,0)"},
        {"name": "IWLSMC", "data_type": "STRING"},
        {"name": "IWCSMC", "data_type": "STRING"},
        {"name": "IWTAMT", "data_type": "DECIMAL(6,0)"},
        {"name": "IWTPCT", "data_type": "DECIMAL(3,0)"},
        {"name": "IWTUOM", "data_type": "STRING"},
        {"name": "IWCCAT", "data_type": "STRING"},
        {"name": "IWCCDT", "data_type": "DECIMAL(7,0)"},
        {"name": "IWTOTC", "data_type": "DECIMAL(5,0)"},
        {"name": "IWDTYP", "data_type": "STRING"},
        {"name": "IWATYP", "data_type": "STRING"},
        {"name": "IWICTA", "data_type": "STRING"},
        {"name": "IWRCTA", "data_type": "STRING"},
        {"name": "IWCPLA", "data_type": "DECIMAL(5,0)"},
        {"name": "IWCLYA", "data_type": "DECIMAL(5,0)"},
        {"name": "IWALPR", "data_type": "STRING"},
        {"name": "IWBULK", "data_type": "STRING"}
    ]
) }}