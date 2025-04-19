{% set container_value2 = var('container_2') %}
{% set storage_account_value = var('storage_account') %}

{{ latest_ibp_csvs(
    source_path="abfss://" ~ container_value2 ~ "@" ~ storage_account_value ~ ".dfs.core.windows.net/IBP_CEU/FF_NAIVEFCST",
    files_format = "csv",
    delimiter = ',',
	forecast_col = ['ZDPNAIVEFCST'],
	all_columns = [
		"CAST(PRDID AS STRING) AS PRDID",
		"CAST(CUSTID AS STRING) AS CUSTID",
        "CAST(SALESORG AS STRING) AS SALESORG",
		"CAST(SALESOFC AS STRING) AS SALESOFC",
		"TO_DATE(REPLACE(KFDATE, '.', '-'), 'yyyy-MM-dd') AS KFDATE",
		"CAST(ZDPNAIVEFCST AS STRING) AS ZDPNAIVEFCST"
	],
	tar_cols = [
		"CAST(PRDID AS STRING) AS PRODUCT_ID",
		"CAST(CUSTID AS STRING) AS CUSTOMER_ID",
        "CAST(SALESORG AS STRING) AS SALES_ORGANISATION_ID",
        "CAST(SALESOFC AS STRING) AS SALES_OFFICE_ID",
		"CAST(KFDATE AS DATE) AS KFDATE"
	]
) }}
