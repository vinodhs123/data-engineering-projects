{% set container_value = var('container') %}
{% set storage_account_value = var('storage_account') %}
{% set bootstrap_storage_location = var('bootstrap_storage_location') %}
{% set incremental_storage_location = var('incremental_storage_location') %}

{{ load_data_with_metadata(
    historical_source="abfss://" ~ container_value ~ "@" ~ storage_account_value ~ ".dfs.core.windows.net/" ~ bootstrap_storage_location ~"/TCURR.parquet",
    incremental_source="abfss://" ~ container_value ~ "@" ~ storage_account_value ~ ".dfs.core.windows.net/" ~ incremental_storage_location ~"/tcurr/",
    all_columns=[
		{"name": "mandt", "data_type": "STRING"},
		{"name": "kurst", "data_type": "STRING"},
		{"name": "fcurr", "data_type": "STRING"},
		{"name": "tcurr", "data_type": "STRING"},
		{"name": "gdatu", "data_type": "STRING"},
		{"name": "ukurs", "data_type": "DECIMAL(13,3)"},
		{"name": "ffact", "data_type": "DECIMAL(13,3)"},
		{"name": "tfact", "data_type": "DECIMAL(13,3)"},
		{"name": "tas_rp_change_date", "data_type": "TIMESTAMP"},
		{"name": "tas_rp_opcode", "data_type": "INT"}
    ],
    composite_keys=["mandt","kurst","fcurr","tcurr","gdatu"],
    transformation_column="tas_rp_opcode",
    transformation_map={"D": 0, "I": 1, "U": 2}
) }}


