{% set container_value1 = var('container_1') %}
{% set container_value2 = var('container_2') %}
{% set storage_account_value = var('storage_account') %}


{{ load_tms_macro(
    source_path="abfss://" ~ container_value2 ~ "@" ~ storage_account_value ~ ".dfs.core.windows.net/TMS/Temp_TMS_Shipment_SL_Shipping_Status_Record",
	merge_col = 'shipmentid',
    files_format = "csv",
    delimiter = '|',
    all_columns = [
        {"name": "z_StatusCode_ID", "data_type": "STRING"},
        {"name": "o_comments", "data_type": "STRING"},
        {"name": "processing_date", "data_type": "STRING"},
        {"name": "o_SourceFileName", "data_type": "STRING"},
        {"name": "shipmentid", "data_type": "INTEGER"},
        {"name": "shipmentlegid", "data_type": "INTEGER"},
        {"name": "shippingstatusid", "data_type": "INTEGER"},
        {"name": "entityid", "data_type": "INTEGER"},
        {"name": "shippingstatusaction", "data_type": "STRING"},
        {"name": "id", "data_type": "INTEGER"},
        {"name": "statuscode", "data_type": "STRING"},
        {"name": "reasoncode", "data_type": "STRING"},
        {"name": "reasoncode_id", "data_type": "STRING"},
        {"name": "statusdate", "data_type": "TIMESTAMP"},
        {"name": "ignoretime", "data_type": "INTEGER"},
        {"name": "datasourcetype", "data_type": "STRING"},
        {"name": "datasourcetype_id", "data_type": "INTEGER"},
        {"name": "datasource", "data_type": "STRING"},
        {"name": "datasource_id", "data_type": "INTEGER"},
        {"name": "createdate", "data_type": "TIMESTAMP"},
        {"name": "username", "data_type": "STRING"}
    ],
    staging_columns = [
        "a.shippingstatusid", "a.entityid", "a.shippingstatusaction", "a.id",
        "a.statuscode", "a.z_StatusCode_ID AS statuscode_id", "a.reasoncode", "a.reasoncode_id",
        "a.statusdate", "a.ignoretime", "a.datasourcetype", "a.datasourcetype_id",
        "a.datasource", "a.datasource_id", "a.createdate", "a.username",
        "a.o_comments AS comments", "a.shipmentlegid",
        "TRIM(CAST(a.shipmentid AS CHAR(30))) AS shipmentid",
        "TO_TIMESTAMP(a.processing_date, 'MM/dd/yyyy HH:mm:ss.SSSSSS') AS processing_date",
        "a.o_SourceFileName AS SourceFileName"
    ],
    target_columns = [
        "CAST(a.shippingstatusid AS INT) AS shippingstatusid", "CAST(a.entityid AS INT) AS entityid",
        "CAST(a.shippingstatusaction AS STRING) AS shippingstatusaction", "CAST(a.id AS STRING) AS id",
        "CAST(a.statuscode AS STRING) AS statuscode", "CAST(a.statuscode_id AS STRING) AS statuscode_id",
        "CAST(a.reasoncode AS STRING) AS reasoncode", "CAST(a.reasoncode_id AS STRING) AS reasoncode_id",
        "CAST(a.statusdate AS STRING) AS statusdate", "CAST(a.ignoretime AS INT) AS ignoretime",
        "CAST(a.datasourcetype AS STRING) AS datasourcetype", "CAST(a.datasourcetype_id AS STRING) AS datasourcetype_id",
        "CAST(a.datasource AS STRING) AS datasource", "CAST(a.datasource_id AS STRING) AS datasource_id",
        "CAST(a.createdate AS TIMESTAMP) AS createdate", "CAST(a.username AS STRING) AS username",
        "CAST(a.comments AS STRING) AS comments", "CAST(a.shipmentlegid AS INT) AS shipmentlegid",
        "CAST(a.shipmentid AS INT) AS shipmentid",
        "CAST(a.processing_date AS TIMESTAMP) AS processing_date",
        "CAST(a.createdate AS STRING) AS createdate_TimeZone",
        "CAST(a.SourceFileName AS STRING) AS SourceFileName",
        "DATEADD(SECOND, CAST(SUBSTRING(a.SourceFileName, CHARINDEX('-', a.SourceFileName, CHARINDEX('-', a.SourceFileName, 11)+1)+1, 10) AS INT), CAST('1970-01-01' AS TIMESTAMP)) AS SourceFile_date"
    ]
) }}