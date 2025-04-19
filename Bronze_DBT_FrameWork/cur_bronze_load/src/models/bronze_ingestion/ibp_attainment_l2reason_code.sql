{% set container_value2 = var('container_2') %}
{% set storage_account_value = var('storage_account') %}

{{ latest_folder_ibp(
    source_path="abfss://" ~ container_value2 ~ "@" ~ storage_account_value ~ ".dfs.core.windows.net/ibp/AZR_L2ATTREA",
    files_format = "csv",
    delimiter = ',',
	merge_col = ['RESOURCE_ID', 'PRODUCT_ID', 'PLANT_ID', 'KFDATE'],
	all_columns = [
		"CAST(RESID AS STRING) AS RESOURCE_ID",
		"CAST(PRDID AS STRING) AS PRODUCT_ID",
		"CAST(LOCID AS INT) AS PLANT_ID",
		"TO_DATE(KFDATE, 'yyyy.MM.dd') AS KFDATE",
		"CASE WHEN ZL2ATTR = '<Null>' THEN 'N/A' ELSE CAST(CAST(ZL2ATTR AS DECIMAL(2,0)) AS INT) END AS L2_REASON_CODE"
	]
) }}