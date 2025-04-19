{% set container_value = var('container') %}
{% set storage_account_value = var('storage_account') %}
{% set bootstrap_storage_location = var('bootstrap_storage_location') %}
{% set incremental_storage_location = var('incremental_storage_location') %}

{{ load_data_with_metadata(
    historical_source="abfss://" ~ container_value ~ "@" ~ storage_account_value ~ ".dfs.core.windows.net/" ~ bootstrap_storage_location ~"/tka01.parquet",
    incremental_source="abfss://" ~ container_value ~ "@" ~ storage_account_value ~ ".dfs.core.windows.net/" ~ incremental_storage_location ~"/tka01/",
    all_columns=[
		{"name": "mandt", "data_type": "STRING"},
		{"name": "kokrs", "data_type": "STRING"},
		{"name": "bezei", "data_type": "STRING"},
		{"name": "waers", "data_type": "STRING"},
		{"name": "ktopl", "data_type": "STRING"},
		{"name": "lmona", "data_type": "STRING"},
		{"name": "kokfi", "data_type": "STRING"},
		{"name": "logsystem", "data_type": "STRING"},
		{"name": "alemt", "data_type": "STRING"},
		{"name": "md_logsystem", "data_type": "STRING"},
		{"name": "khinr", "data_type": "STRING"},
		{"name": "komp1", "data_type": "STRING"},
		{"name": "komp0", "data_type": "STRING"},
		{"name": "komp2", "data_type": "STRING"},
		{"name": "erkrs", "data_type": "STRING"},
		{"name": "dprct", "data_type": "STRING"},
		{"name": "phinr", "data_type": "STRING"},
		{"name": "pcldg", "data_type": "STRING"},
		{"name": "pcbel", "data_type": "STRING"},
		{"name": "xwbuk", "data_type": "STRING"},
		{"name": "bphinr", "data_type": "STRING"},
		{"name": "xbpale", "data_type": "STRING"},
		{"name": "kstar_fin", "data_type": "STRING"},
		{"name": "kstar_fid", "data_type": "STRING"},
		{"name": "pcacur", "data_type": "STRING"},
		{"name": "pcacurtp", "data_type": "STRING"},
		{"name": "pcatrcur", "data_type": "STRING"},
		{"name": "ctyp", "data_type": "STRING"},
		{"name": "rclac", "data_type": "STRING"},
		{"name": "blart", "data_type": "STRING"},
		{"name": "fikrs", "data_type": "STRING"},
		{"name": "rcl_primac", "data_type": "STRING"},
		{"name": "pca_alemt", "data_type": "STRING"},
		{"name": "pca_valu", "data_type": "STRING"},
		{"name": "cvprof", "data_type": "STRING"},
		{"name": "cvact", "data_type": "STRING"},
		{"name": "vname", "data_type": "STRING"},
		{"name": "pca_acc_diff", "data_type": "STRING"},
		{"name": "tp_valohb", "data_type": "STRING"},
		{"name": "defprctr", "data_type": "STRING"},
		{"name": "f_obsolete", "data_type": "STRING"},
		{"name": "auth_use_no_std", "data_type": "STRING"},
		{"name": "auth_use_add1", "data_type": "STRING"},
		{"name": "auth_use_add2", "data_type": "STRING"},
		{"name": "auth_ke_no_std", "data_type": "STRING"},
		{"name": "auth_ke_use_add1", "data_type": "STRING"},
		{"name": "auth_ke_use_add2", "data_type": "STRING"},
		{"name": "tas_rp_change_date", "data_type": "TIMESTAMP"},
		{"name": "tas_rp_opcode", "data_type": "INT"}
	],
    composite_keys=["mandt","kokrs"],
    transformation_column="tas_rp_opcode",
    transformation_map={"D": 0, "I": 1, "U": 2}
) }}



