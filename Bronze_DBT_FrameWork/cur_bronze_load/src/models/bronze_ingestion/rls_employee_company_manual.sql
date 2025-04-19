{% set container_value2 = var('container_2') %}
{% set storage_account_value = var('storage_account') %}

{{ latest_folder_ibp(
    source_path="abfss://" ~ container_value2 ~ "@" ~ storage_account_value ~ ".dfs.core.windows.net/Security/RLS_Employee_Company_Manual",
    files_format = "csv",
    delimiter = ',',
	merge_col = ['Employee_Email', 'Company'],
	all_columns = [
	    "CAST(Employee_Email AS STRING)",
	    "CAST(Company AS STRING)"
	]
) }}
