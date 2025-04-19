{% set container_value2 = var('container_2') %}
{% set storage_account_value = var('storage_account') %}

{{ latest_folder_csvs(
    source_path="abfss://" ~ container_value2 ~ "@" ~ storage_account_value ~ ".dfs.core.windows.net/IBP_SAF/AZR_SUPPLY_PRODLOCSRC_APACSA_ZA",
    files_format = "csv",
    delimiter = ',',
	forecast_col = 'ZPOTATOCONSUMPTION',
	all_columns = [
		"CAST(PRDID AS STRING) AS PRDID",
		"CAST(LOCID AS STRING) AS LOCID",
		"CAST('ZA01' AS STRING) AS SORG",
		"TO_DATE(REPLACE(KFDATE, '.', '-'), 'yyyy-MM-dd') AS KFDATE",
		"CAST(ZPOTATOCONSUMPTION AS STRING) AS ZPOTATOCONSUMPTION",
		"CAST(PERIODID AS INT) AS PERIODID"
	],
	tar_cols = [
		"CAST(PRDID AS STRING) AS PRODUCT_ID",
		"CAST(LOCID AS STRING) AS PLANT_ID",
		"CAST(SORG AS STRING) AS SALES_ORGANISATION_ID",
		"CAST(KFDATE AS DATE) AS KFDATE"
	]
) }}