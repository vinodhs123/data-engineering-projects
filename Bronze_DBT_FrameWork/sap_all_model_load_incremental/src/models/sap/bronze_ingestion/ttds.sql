{% set container_value = var('container') %}
{% set storage_account_value = var('storage_account') %}
{% set bootstrap_storage_location = var('bootstrap_storage_location') %}
{% set incremental_storage_location = var('incremental_storage_location') %}

{{ load_data_with_metadata(
    historical_source="abfss://" ~ container_value ~ "@" ~ storage_account_value ~ ".dfs.core.windows.net/" ~ bootstrap_storage_location ~"/ttds.parquet",
    incremental_source="abfss://" ~ container_value ~ "@" ~ storage_account_value ~ ".dfs.core.windows.net/" ~ incremental_storage_location ~"/ttds/",
    all_columns=[
		{"name": "mandt", "data_type": "STRING"},
		{"name": "tplst", "data_type": "STRING"},
		{"name": "fabkl", "data_type": "STRING"},
		{"name": "adrnr", "data_type": "STRING"},
		{"name": "tpsid", "data_type": "STRING"},
		{"name": "vtparn", "data_type": "STRING"},
		{"name": "vtpart", "data_type": "STRING"},
		{"name": "kschl", "data_type": "STRING"},
		{"name": "nketp", "data_type": "STRING"},
		{"name": "traend", "data_type": "STRING"},
		{"name": "sttrg", "data_type": "STRING"},
		{"name": "tpssf", "data_type": "STRING"},
		{"name": "tpstx1", "data_type": "STRING"},
		{"name": "tpstx2", "data_type": "STRING"},
		{"name": "tpstx3", "data_type": "STRING"},
		{"name": "bukrs", "data_type": "STRING"},
		{"name": "stagew", "data_type": "STRING"},
		{"name": "stavol", "data_type": "STRING"},
		{"name": "stadis", "data_type": "STRING"},
		{"name": "stacur", "data_type": "STRING"},
		{"name": "tas_rp_change_date", "data_type": "TIMESTAMP"},
		{"name": "tas_rp_opcode", "data_type": "INT"}
	],
    composite_keys=["mandt","tplst"],
    transformation_column="tas_rp_opcode",
    transformation_map={"D": 0, "I": 1, "U": 2}
) }}

