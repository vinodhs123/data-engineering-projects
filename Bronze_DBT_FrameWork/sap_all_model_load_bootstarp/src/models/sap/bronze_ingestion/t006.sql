{% set container_value = var('container') %}
{% set storage_account_value = var('storage_account') %}
{% set bootstrap_storage_location = var('bootstrap_storage_location') %}
{% set incremental_storage_location = var('incremental_storage_location') %}

{{ load_data_with_metadata(
    historical_source="abfss://" ~ container_value ~ "@" ~ storage_account_value ~ ".dfs.core.windows.net/" ~ bootstrap_storage_location ~"/t006.parquet",
    incremental_source="abfss://" ~ container_value ~ "@" ~ storage_account_value ~ ".dfs.core.windows.net/" ~ incremental_storage_location ~"/t006/",
    all_columns=[
		{"name": "mandt", "data_type": "STRING"},
		{"name": "msehi", "data_type": "STRING"},
		{"name": "kzex3", "data_type": "STRING"},
		{"name": "kzex6", "data_type": "STRING"},
		{"name": "andec", "data_type": "SMALLINT"},
		{"name": "kzkeh", "data_type": "STRING"},
		{"name": "kzwob", "data_type": "STRING"},
		{"name": "kz1eh", "data_type": "STRING"},
		{"name": "kz2eh", "data_type": "STRING"},
		{"name": "dimid", "data_type": "STRING"},
		{"name": "zaehl", "data_type": "INT"},
		{"name": "nennr", "data_type": "INT"},
		{"name": "exp10", "data_type": "SMALLINT"},
		{"name": "addko", "data_type": "DECIMAL(13,3)"},
		{"name": "expon", "data_type": "SMALLINT"},
		{"name": "decan", "data_type": "SMALLINT"},
		{"name": "isocode", "data_type": "STRING"},
		{"name": "primary", "data_type": "STRING"},
		{"name": "temp_value", "data_type": "FLOAT"},
		{"name": "temp_unit", "data_type": "STRING"},
		{"name": "famunit", "data_type": "STRING"},
		{"name": "press_val", "data_type": "FLOAT"},
		{"name": "press_unit", "data_type": "STRING"},
		{"name": "tas_rp_change_date", "data_type": "TIMESTAMP"},
		{"name": "tas_rp_opcode", "data_type": "INT"}
	],
    composite_keys=["mandt","msehi"],
    transformation_column="tas_rp_opcode",
    transformation_map={"D": 0, "I": 1, "U": 2}
) }}
