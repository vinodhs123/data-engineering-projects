{% set container_value2 = var('container_2') %}
{% set storage_account_value = var('storage_account') %}

{{ latest_ibp_csvs(
    source_path="abfss://" ~ container_value2 ~ "@" ~ storage_account_value ~ ".dfs.core.windows.net/IBP/FF_ZYTDYTGFULFILLEDDEMAND",
    files_format = "csv",
    delimiter = ',',
	forecast_col = ['ZYTDYTGFULFILLEDDEMAND'],
	all_columns = [
		"CAST(PRDID AS STRING) AS PRDID",
		"CAST(LOCID AS STRING) AS LOCID",
		"CAST(CUSTID AS STRING) AS CUSTID",
		"TO_DATE(REPLACE(Date_time, '.', '-'), 'yyyy-MM-dd') AS KFDATE",
		"CAST(KEYFIGURE AS STRING) AS ZYTDYTGFULFILLEDDEMAND"
	],
	tar_cols = [
		"CAST(PRDID AS STRING) AS PRODUCT_ID",
		"CAST(LOCID AS STRING) AS PLANT_ID",
		"CAST(CUSTID AS STRING) AS CUSTOMER_ID",
		"CAST(KFDATE AS DATE) AS KFDATE"
	]
) }}
