{% set container_value = var('container') %}
{% set storage_account_value = var('storage_account') %}
{% set bootstrap_storage_location = var('bootstrap_storage_location') %}
{% set incremental_storage_location = var('incremental_storage_location') %}

{{ load_data_with_metadata(
    historical_source="abfss://" ~ container_value ~ "@" ~ storage_account_value ~ ".dfs.core.windows.net/" ~ bootstrap_storage_location ~"/ausp.parquet",
    incremental_source="abfss://" ~ container_value ~ "@" ~ storage_account_value ~ ".dfs.core.windows.net/" ~ incremental_storage_location ~"/ausp/",
    all_columns=[
		{"name": "mandt", "data_type": "STRING"},
		{"name": "objek", "data_type": "STRING"},
		{"name": "atinn", "data_type": "STRING"},
		{"name": "atzhl", "data_type": "STRING"},
		{"name": "mafid", "data_type": "STRING"},
		{"name": "klart", "data_type": "STRING"},
		{"name": "adzhl", "data_type": "STRING"},
		{"name": "atwrt", "data_type": "STRING"},
		{"name": "atflv", "data_type": "FLOAT"},
		{"name": "atawe", "data_type": "STRING"},
		{"name": "atflb", "data_type": "FLOAT"},
		{"name": "ataw1", "data_type": "STRING"},
		{"name": "atcod", "data_type": "STRING"},
		{"name": "attlv", "data_type": "FLOAT"},
		{"name": "attlb", "data_type": "FLOAT"},
		{"name": "atprz", "data_type": "STRING"},
		{"name": "atinc", "data_type": "FLOAT"},
		{"name": "ataut", "data_type": "STRING"},
		{"name": "aennr", "data_type": "STRING"},
		{"name": "datuv", "data_type": "STRING"},
		{"name": "lkenz", "data_type": "STRING"},
		{"name": "atimb", "data_type": "STRING"},
		{"name": "atzis", "data_type": "STRING"},
		{"name": "atsrt", "data_type": "STRING"},
		{"name": "atvglart", "data_type": "STRING"},
		{"name": "datub", "data_type": "STRING"},
		{"name": "tas_rp_change_date", "data_type": "TIMESTAMP"},
		{"name": "tas_rp_opcode", "data_type": "INT"}
    ],
    composite_keys=["mandt","objek","atinn","atzhl","mafid","klart","adzhl"],
    transformation_column="tas_rp_opcode",
    transformation_map={"D": 0, "I": 1, "U": 2}
) }}
