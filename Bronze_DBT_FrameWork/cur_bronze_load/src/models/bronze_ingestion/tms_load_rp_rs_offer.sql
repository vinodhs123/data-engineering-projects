{% set container_value2 = var('container_2') %}
{% set storage_account_value = var('storage_account') %}

{{
  load_tms_macro(
    source_path = "abfss://" ~ container_value2 ~ "@" ~ storage_account_value ~ ".dfs.core.windows.net/TMS/Temp_TMS_Load_RP_RS_Offer",
    merge_col = 'loadid',
	files_format = "csv",
	delimiter = '|',
    all_columns = [
        {"name": "offergroup", "data_type": "STRING"},
        {"name": "processing_date", "data_type": "STRING"},
        {"name": "o_SourceFileName", "data_type": "STRING"},
        {"name": "loadid", "data_type": "STRING"},
        {"name": "planid", "data_type": "STRING"},
        {"name": "stepid", "data_type": "STRING"},
        {"name": "offerid", "data_type": "STRING"},
        {"name": "status", "data_type": "STRING"},
        {"name": "id", "data_type": "STRING"},
        {"name": "carrier", "data_type": "STRING"},
        {"name": "carrier_id", "data_type": "STRING"},
        {"name": "scac", "data_type": "STRING"},
        {"name": "vendorcode", "data_type": "STRING"},
        {"name": "createdate", "data_type": "STRING"},
        {"name": "acceptdate", "data_type": "STRING"},
        {"name": "expiredate", "data_type": "STRING"},
        {"name": "withdrawdate", "data_type": "STRING"},
        {"name": "rate", "data_type": "STRING"},
        {"name": "currencycode", "data_type": "STRING"},
        {"name": "contractrate", "data_type": "STRING"},
        {"name": "contractrate_currencycode", "data_type": "STRING"},
        {"name": "contractratespecified", "data_type": "STRING"}
    ],
    staging_columns = [
        "a.offerid", "a.status", "a.id", "a.carrier", "a.carrier_id", "a.scac",
        "a.vendorcode", "a.stepid", "a.planid",
        "TRIM(CAST(a.loadid AS CHAR(30))) AS loadid",
        "a.createdate", "a.acceptdate", "a.expiredate", "a.withdrawdate",
        "a.rate", "a.currencycode", "a.contractrate",
        "a.contractrate_currencycode", "a.contractratespecified",
        "SUBSTRING(a.offergroup, 1, 15) AS offergroup",
        "TO_TIMESTAMP(a.processing_date, 'MM/dd/yyyy HH:mm:ss.SSSSSS') AS processing_date",
        "a.o_SourceFileName AS SourceFileName"
    ],
    target_columns = [
        "CAST(a.offerid AS INT) AS offerid",
        "CAST(a.status AS STRING) AS status",
        "CAST(a.id AS STRING) AS id",
        "CAST(a.carrier AS STRING) AS carrier",
        "CAST(a.carrier_id AS STRING) AS carrier_id",
        "CAST(a.scac AS STRING) AS scac",
        "CAST(a.vendorcode AS STRING) AS vendorcode",
        "CAST(a.createdate AS TIMESTAMP) AS createdate",
        "CAST(a.acceptdate AS STRING) AS acceptdate",
        "CAST(a.expiredate AS STRING) AS expiredate",
        "CAST(a.withdrawdate AS STRING) AS withdrawdate",
        "CAST(a.rate AS STRING) AS rate",
        "CAST(a.currencycode AS STRING) AS currencycode",
        "CAST(a.contractrate AS STRING) AS contractrate",
        "CAST(a.contractrate_currencycode AS STRING) AS contractrate_currencycode",
        "CAST(a.contractratespecified AS INT) AS contractratespecified",
        "CAST(a.offergroup AS STRING) AS offergroup",
        "CAST(a.stepid AS INT) AS stepid",
        "CAST(a.planid AS INT) AS planid",
        "CAST(a.loadid AS INT) AS loadid",
        "CAST(a.processing_date AS TIMESTAMP) AS processing_date",
        "CAST(a.createdate AS STRING) AS createdate_TimeZone",
        "CAST(a.SourceFileName AS STRING) AS SourceFileName",
        "DATEADD(SECOND, CAST(SUBSTRING(a.SourceFileName, CHARINDEX('-', a.SourceFileName, CHARINDEX('-', a.SourceFileName, 11)+1)+1, 10) AS INT), CAST('1970-01-01' AS TIMESTAMP)) AS SourceFile_date"
    ]
  )
}}
