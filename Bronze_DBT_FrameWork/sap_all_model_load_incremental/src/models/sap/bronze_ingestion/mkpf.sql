{% set container_value = var('container') %}
{% set storage_account_value = var('storage_account') %}
{% set bootstrap_storage_location = var('bootstrap_storage_location') %}
{% set incremental_storage_location = var('incremental_storage_location') %}

{{ load_data_with_metadata(
    historical_source="abfss://" ~ container_value ~ "@" ~ storage_account_value ~ ".dfs.core.windows.net/" ~ bootstrap_storage_location ~"/mkpf.parquet",
    incremental_source="abfss://" ~ container_value ~ "@" ~ storage_account_value ~ ".dfs.core.windows.net/" ~ incremental_storage_location ~"/mkpf/",
    all_columns=[
		{"name": "mandt", "data_type": "STRING"},
		{"name": "mblnr", "data_type": "STRING"},
		{"name": "mjahr", "data_type": "STRING"},
		{"name": "vgart", "data_type": "STRING"},
		{"name": "blart", "data_type": "STRING"},
		{"name": "blaum", "data_type": "STRING"},
		{"name": "bldat", "data_type": "STRING"},
		{"name": "budat", "data_type": "STRING"},
		{"name": "cpudt", "data_type": "STRING"},
		{"name": "cputm", "data_type": "STRING"},
		{"name": "aedat", "data_type": "STRING"},
		{"name": "usnam", "data_type": "STRING"},
		{"name": "tcode", "data_type": "STRING"},
		{"name": "xblnr", "data_type": "STRING"},
		{"name": "bktxt", "data_type": "STRING"},
		{"name": "frath", "data_type": "DECIMAL(13,3)"},
		{"name": "frbnr", "data_type": "STRING"},
		{"name": "wever", "data_type": "STRING"},
		{"name": "xabln", "data_type": "STRING"},
		{"name": "awsys", "data_type": "STRING"},
		{"name": "bla2d", "data_type": "STRING"},
		{"name": "tcode2", "data_type": "STRING"},
		{"name": "bfwms", "data_type": "STRING"},
		{"name": "exnum", "data_type": "STRING"},
		{"name": "spe_budat_uhr", "data_type": "STRING"},
		{"name": "spe_budat_zone", "data_type": "STRING"},
		{"name": "le_vbeln", "data_type": "STRING"},
		{"name": "spe_logsys", "data_type": "STRING"},
		{"name": "spe_mdnum_ewm", "data_type": "STRING"},
		{"name": "gts_cusref_no", "data_type": "STRING"},
		{"name": "fls_rsto", "data_type": "STRING"},
		{"name": "msr_active", "data_type": "STRING"},
		{"name": "knumv", "data_type": "STRING"},
		{"name": "delivery_no", "data_type": "STRING"},
		{"name": "zzcre_by_login", "data_type": "STRING"},
		{"name": "tas_rp_change_date", "data_type": "TIMESTAMP"},
		{"name": "tas_rp_opcode", "data_type": "INT"}
    ],
    composite_keys=["mandt","mjahr","mblnr"],
    transformation_column="tas_rp_opcode",
    transformation_map={"D": 0, "I": 1, "U": 2}
) }}


