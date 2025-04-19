SELECT 
    'addrnumber' AS column_name,
    COUNT(DISTINCT addrnumber) AS unique_values,
    COUNT(*) AS total_records
FROM {{ ref('adr6') }}
UNION ALL
SELECT 
    'client' AS column_name,
    COUNT(DISTINCT client),
    COUNT(*)
FROM {{ ref('adr6') }}
UNION ALL
SELECT 
    'persnumber' AS column_name,
    COUNT(DISTINCT persnumber),
    COUNT(*)
FROM {{ ref('adr6') }};
