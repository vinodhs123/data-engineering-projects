{% set container_value2 = var('container_2') %}
{% set storage_account_value = var('storage_account') %}

{{ latest_folder_csvs(
    source_path="abfss://" ~ container_value2 ~ "@" ~ storage_account_value ~ ".dfs.core.windows.net/ibp/AZR_ZCONQTY",
    files_format = "csv",
    delimiter = ',',
	forecast_col = 'ZCONSQTY',
	all_columns = [
		"CAST(PRDID AS STRING) AS PRDID",
		"CAST(LOCID AS STRING) AS LOCID",
		"CAST(CUSTID AS STRING) AS CUSTID",
		"CAST(SORG AS STRING) AS SORG",
		"CAST(SOFF AS STRING) AS SOFF",
		"TO_DATE(REPLACE(KFDATE, '.', '-'), 'yyyy-MM-dd') AS KFDATE",
		"CAST(ZCONSQTY AS STRING) AS ZCONSQTY",
		"CAST(PERIODID AS INT) AS PERIODID"
	],
	tar_cols = [
		"CAST(PRDID AS STRING) AS PRODUCT_ID",
		"CAST(LOCID AS STRING) AS PLANT_ID",
		"CAST(CUSTID AS STRING) AS CUSTOMER_ID",
		"CAST(SORG AS STRING) AS SALES_ORGANISATION_ID",
		"CAST(SOFF AS STRING) AS SALES_OFFICE_ID",
		"CAST(KFDATE AS DATE) AS KFDATE"
	]
) }}
