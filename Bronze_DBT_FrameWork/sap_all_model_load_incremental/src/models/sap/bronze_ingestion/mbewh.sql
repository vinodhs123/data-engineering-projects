{% set container_value = var('container') %}
{% set storage_account_value = var('storage_account') %}
{% set bootstrap_storage_location = var('bootstrap_storage_location') %}
{% set incremental_storage_location = var('incremental_storage_location') %}

{{ load_data_with_metadata(
    historical_source="abfss://" ~ container_value ~ "@" ~ storage_account_value ~ ".dfs.core.windows.net/" ~ bootstrap_storage_location ~"/mbewh.parquet",
    incremental_source="abfss://" ~ container_value ~ "@" ~ storage_account_value ~ ".dfs.core.windows.net/" ~ incremental_storage_location ~"/mbewh/",
    all_columns=[
		{"name": "mandt", "data_type": "STRING"},
		{"name": "matnr", "data_type": "STRING"},
		{"name": "bwkey", "data_type": "STRING"},
		{"name": "bwtar", "data_type": "STRING"},
		{"name": "lfgja", "data_type": "STRING"},
		{"name": "lfmon", "data_type": "STRING"},
		{"name": "lbkum", "data_type": "DECIMAL(13,3)"},
		{"name": "salk3", "data_type": "DECIMAL(13,3)"},
		{"name": "vprsv", "data_type": "STRING"},
		{"name": "verpr", "data_type": "DECIMAL(13,3)"},
		{"name": "stprs", "data_type": "DECIMAL(13,3)"},
		{"name": "peinh", "data_type": "DECIMAL(13,3)"},
		{"name": "bklas", "data_type": "STRING"},
		{"name": "salkv", "data_type": "DECIMAL(13,3)"},
		{"name": "vksal", "data_type": "DECIMAL(13,3)"},
		{"name": "tas_rp_change_date", "data_type": "TIMESTAMP"},
		{"name": "tas_rp_opcode", "data_type": "INT"}
    ],
    composite_keys=["mandt","matnr","bwkey","bwtar","lfgja","lfmon"],
    transformation_column="tas_rp_opcode",
    transformation_map={"D": 0, "I": 1, "U": 2}
) }}
