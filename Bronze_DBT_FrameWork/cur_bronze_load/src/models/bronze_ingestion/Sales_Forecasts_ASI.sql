{% set container_value2 = var('container_2') %}
{% set storage_account_value = var('storage_account') %}

{{ latest_folder_csv(
    source_path="abfss://" ~ container_value2 ~ "@" ~ storage_account_value ~ ".dfs.core.windows.net/HVR/bic_ohzohtasasi/bic_ohzohtasasi",
    files_format = "csv",
    delimiter = '|',
    all_columns=[
	  {"name": "ohrequid", "data_type": "INTEGER"},
	  {"name": "datapakid", "data_type": "INTEGER"},
	  {"name": "record", "data_type": "INTEGER"},
	  {"name": "calmonth", "data_type": "INTEGER"},
	  {"name": "base_uom", "data_type": "STRING"},
	  {"name": "bic_zconsfcst", "data_type": "DOUBLE"},
	  {"name": "material", "data_type": "STRING"},
	  {"name": "calyear", "data_type": "INTEGER"},
	  {"name": "customer", "data_type": "STRING"},
	  {"name": "salesorg", "data_type": "STRING"},
	  {"name": "calweek", "data_type": "INTEGER"},
	  {"name": "plant", "data_type": "STRING"},
	  {"name": "sales_off", "data_type": "STRING"},
	  {"name": "op_code", "data_type": "INTEGER"},
	  {"name": "src_timestamp", "data_type": "TIMESTAMP"},
	  {"name": "timekey_col", "data_type": "STRING"}
	]



) }}



