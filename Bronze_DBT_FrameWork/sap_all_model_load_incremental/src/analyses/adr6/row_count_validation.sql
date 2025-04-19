WITH source_row_count AS (
    SELECT 
        COUNT(*) AS total_rows
    FROM {{ source('sap', 'adr6') }}
),
bronze_row_count AS (
    SELECT 
        COUNT(*) AS total_rows
    FROM {{ ref('adr6') }}
)
SELECT 
    source_row_count.total_rows AS source_rows,
    bronze_row_count.total_rows AS bronze_rows,
    source_row_count.total_rows = bronze_row_count.total_rows AS row_count_match
FROM source_row_count, bronze_row_count;
