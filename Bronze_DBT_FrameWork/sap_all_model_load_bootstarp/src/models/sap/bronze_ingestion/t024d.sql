{% set container_value = var('container') %}
{% set storage_account_value = var('storage_account') %}
{% set bootstrap_storage_location = var('bootstrap_storage_location') %}
{% set incremental_storage_location = var('incremental_storage_location') %}

{{ load_data_with_metadata(
    historical_source="abfss://" ~ container_value ~ "@" ~ storage_account_value ~ ".dfs.core.windows.net/" ~ bootstrap_storage_location ~"/t024d.parquet",
    incremental_source="abfss://" ~ container_value ~ "@" ~ storage_account_value ~ ".dfs.core.windows.net/" ~ incremental_storage_location ~"/t024d/",
    all_columns=[
		{"name": "mandt", "data_type": "STRING"},
		{"name": "werks", "data_type": "STRING"},
		{"name": "dispo", "data_type": "STRING"},
		{"name": "dsnam", "data_type": "STRING"},
		{"name": "dstel", "data_type": "STRING"},
		{"name": "ekgrp", "data_type": "STRING"},
		{"name": "mempf", "data_type": "STRING"},
		{"name": "gsber", "data_type": "STRING"},
		{"name": "prctr", "data_type": "STRING"},
		{"name": "usrtyp", "data_type": "STRING"},
		{"name": "usrkey", "data_type": "STRING"},
		{"name": "tas_rp_change_date", "data_type": "TIMESTAMP"},
		{"name": "tas_rp_opcode", "data_type": "INT"}
	],
    composite_keys=["mandt","werks","dispo"],
    transformation_column="tas_rp_opcode",
    transformation_map={"D": 0, "I": 1, "U": 2}
) }}

