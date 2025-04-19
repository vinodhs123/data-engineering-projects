{% set container_value = var('container') %}
{% set storage_account_value = var('storage_account') %}
{% set bootstrap_storage_location = var('bootstrap_storage_location') %}
{% set incremental_storage_location = var('incremental_storage_location') %}
{{ config(
    materialized="table"
) }}

{{ load_data_with_metadata(
    historical_source="abfss://" ~ container_value ~ "@" ~ storage_account_value ~ ".dfs.core.windows.net/" ~ bootstrap_storage_location ~"/adr6.parquet",
    incremental_source="abfss://" ~ container_value ~ "@" ~ storage_account_value ~ ".dfs.core.windows.net/" ~ incremental_storage_location ~"/adr6/",
    all_columns=[
        {"name": "client", "data_type": "STRING"},
        {"name": "addrnumber", "data_type": "STRING"},
        {"name": "persnumber", "data_type": "STRING"},
        {"name": "date_from", "data_type": "STRING"},
        {"name": "consnumber", "data_type": "STRING"},
        {"name": "flgdefault", "data_type": "STRING"},
        {"name": "flg_nouse", "data_type": "STRING"},
        {"name": "home_flag", "data_type": "STRING"},
        {"name": "smtp_addr", "data_type": "STRING"},
        {"name": "smtp_srch", "data_type": "STRING"},
        {"name": "dft_receiv", "data_type": "STRING"},
        {"name": "r3_user", "data_type": "STRING"},
        {"name": "encode", "data_type": "STRING"},
        {"name": "tnef", "data_type": "STRING"},
        {"name": "tas_rp_change_date", "data_type": "TIMESTAMP"},
        {"name": "tas_rp_opcode", "data_type": "INT"}
    ],
    composite_keys=["client", "addrnumber", "persnumber", "date_from", "consnumber"],
    transformation_column="tas_rp_opcode",
    transformation_map={"D": 0, "I": 1, "U": 2}
) }}
