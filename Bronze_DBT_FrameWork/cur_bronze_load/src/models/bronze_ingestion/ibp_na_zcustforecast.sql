{% set container_value2 = var('container_2') %}
{% set storage_account_value = var('storage_account') %}

{{ latest_ibp_csvs(
    source_path="abfss://" ~ container_value2 ~ "@" ~ storage_account_value ~ ".dfs.core.windows.net/IBP/FF_ZCUSTFORECAST",
    files_format = "csv",
    delimiter = ',',
	forecast_col = ['ZCUSTFORECAST'],
	all_columns = [
		"CAST(PRDID AS STRING) AS PRDID",
		"CAST(LOCID AS STRING) AS LOCID",
        "CAST(KUNNR AS STRING) AS KUNNR",
        "CAST(SalesOrg AS STRING) AS SalesOrg",
        "CAST(SalesOffc AS STRING) AS SalesOffc",
		"TO_DATE(REPLACE(Date_time, '.', '-'), 'yyyy-MM-dd') AS KFDATE",
		"CAST(Key_Figure AS STRING) AS ZCUSTFORECAST"
	],
	tar_cols = [
		"CAST(PRDID AS STRING) AS PRODUCT_ID",
		"CAST(LOCID AS STRING) AS PLANT_ID",
        "CAST(KUNNR AS STRING) AS CUSTOMER_ID",
        "CAST(SalesOrg AS STRING) AS SALES_ORGANISATION_ID",
        "CAST(SalesOffc AS STRING) AS SALES_OFFICE_ID",
		"CAST(KFDATE AS DATE) AS KFDATE"
	]
) }}
