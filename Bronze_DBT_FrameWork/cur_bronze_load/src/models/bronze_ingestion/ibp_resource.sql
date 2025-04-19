{% set container_value2 = var('container_2') %}
{% set storage_account_value = var('storage_account') %}

{{ latest_folder_ibp(
    source_path="abfss://" ~ container_value2 ~ "@" ~ storage_account_value ~ ".dfs.core.windows.net/ibp/AZR_RESOURCE",
    files_format = "csv",
    delimiter = ',',
    merge_col = ['RESOURCE_ID'],
	all_columns = [
		"CAST(RESID AS STRING) AS RESOURCE_ID",
		"CAST(DESC AS STRING) AS RESOURCE_DESCRIPTION",
        "CASE WHEN TYPE = '<Null>' THEN NULL ELSE CAST(TYPE AS STRING) END AS RESOURCE_TYPE"
	]
) }}
