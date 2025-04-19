{% set container_value2 = var('container_2') %}
{% set storage_account_value = var('storage_account') %}

{{ latest_folder_csv(
    source_path="abfss://" ~ container_value2 ~ "@" ~ storage_account_value ~ ".dfs.core.windows.net/HVR/bic_ohzohtasltm/bic_ohzohtasltm",
    files_format = "csv",
    delimiter = '|',
    all_columns = [
        {"name": "OHREQUid", "data_type": "INTEGER"},
        {"name": "DATAPAKID", "data_type": "INTEGER"},
        {"name": "RECORD", "data_type": "INTEGER"},
        {"name": "CALMONTH", "data_type": "INTEGER"},
        {"name": "BASE_UOM", "data_type": "STRING"},
        {"name": "BIC_ZCONSFCST", "data_type": "DOUBLE"},
        {"name": "MATERIAL", "data_type": "STRING"},
        {"name": "CALYEAR", "data_type": "INTEGER"},
        {"name": "CUSTOMER", "data_type": "STRING"},
        {"name": "SALESORG", "data_type": "STRING"},
        {"name": "CALWEEK", "data_type": "INTEGER"},
        {"name": "PLANT", "data_type": "INTEGER"},
        {"name": "SALES_OFF", "data_type": "STRING"},
        {"name": "OP_CODE", "data_type": "INTEGER"},
        {"name": "SRC_TIMESTAMP", "data_type": "TIMESTAMP"},
        {"name": "TIMEKEY_COL", "data_type": "STRING"}
    ]
) }}




