{% set container_value = var('container') %}
{% set storage_account_value = var('storage_account') %}
{% set bootstrap_storage_location = var('bootstrap_storage_location') %}
{% set incremental_storage_location = var('incremental_storage_location') %}

{{ load_data_with_metadata(
    historical_source="abfss://" ~ container_value ~ "@" ~ storage_account_value ~ ".dfs.core.windows.net/" ~ bootstrap_storage_location ~"/t024.parquet",
    incremental_source="abfss://" ~ container_value ~ "@" ~ storage_account_value ~ ".dfs.core.windows.net/" ~ incremental_storage_location ~"/t024/",
    all_columns=[
		{"name": "mandt", "data_type": "STRING"},
		{"name": "ekgrp", "data_type": "STRING"},
		{"name": "eknam", "data_type": "STRING"},
		{"name": "ektel", "data_type": "STRING"},
		{"name": "ldest", "data_type": "STRING"},
		{"name": "telfx", "data_type": "STRING"},
		{"name": "tel_number", "data_type": "STRING"},
		{"name": "tel_extens", "data_type": "STRING"},
		{"name": "smtp_addr", "data_type": "STRING"},
		{"name": "tas_rp_change_date", "data_type": "TIMESTAMP"},
		{"name": "tas_rp_opcode", "data_type": "INT"}
	],
    composite_keys=["mandt","ekgrp"],
    transformation_column="tas_rp_opcode",
    transformation_map={"D": 0, "I": 1, "U": 2}
) }}

