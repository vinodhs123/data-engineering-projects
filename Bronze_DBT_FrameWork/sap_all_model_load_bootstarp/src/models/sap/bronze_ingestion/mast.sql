{% set container_value = var('container') %}
{% set storage_account_value = var('storage_account') %}
{% set bootstrap_storage_location = var('bootstrap_storage_location') %}
{% set incremental_storage_location = var('incremental_storage_location') %}

{{ load_data_with_metadata(
    historical_source="abfss://" ~ container_value ~ "@" ~ storage_account_value ~ ".dfs.core.windows.net/" ~ bootstrap_storage_location ~"/mast.parquet",
    incremental_source="abfss://" ~ container_value ~ "@" ~ storage_account_value ~ ".dfs.core.windows.net/" ~ incremental_storage_location ~"/mast/",
    all_columns=[
		{"name": "mandt", "data_type": "STRING"},
		{"name": "matnr", "data_type": "STRING"},
		{"name": "werks", "data_type": "STRING"},
		{"name": "stlan", "data_type": "STRING"},
		{"name": "stlnr", "data_type": "STRING"},
		{"name": "stlal", "data_type": "STRING"},
		{"name": "losvn", "data_type": "DECIMAL(13,3)"},
		{"name": "losbs", "data_type": "DECIMAL(13,3)"},
		{"name": "andat", "data_type": "STRING"},
		{"name": "annam", "data_type": "STRING"},
		{"name": "aedat", "data_type": "STRING"},
		{"name": "aenam", "data_type": "STRING"},
		{"name": "cslty", "data_type": "STRING"},
		{"name": "tas_rp_change_date", "data_type": "TIMESTAMP"},
		{"name": "tas_rp_opcode", "data_type": "INT"}

    ],
    composite_keys=["mandt","matnr","werks","stlan","stlnr","stlal"],
    transformation_column="tas_rp_opcode",
    transformation_map={"D": 0, "I": 1, "U": 2}
) }}
