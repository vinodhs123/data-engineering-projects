SELECT 
    COUNT(*) AS null_client_count
FROM {{ ref('adr6') }}
WHERE client IS NULL
UNION ALL
SELECT 
    COUNT(*) AS null_addrnumber_count
FROM {{ ref('adr6') }}
WHERE addrnumber IS NULL
UNION ALL
SELECT 
    COUNT(*) AS null_persnumber_count
FROM {{ ref('adr6') }}
WHERE persnumber IS NULL
UNION ALL
SELECT 
    COUNT(*) AS null_date_from_count
FROM {{ ref('adr6') }}
WHERE date_from IS NULL
UNION ALL
SELECT 
    COUNT(*) AS null_consnumber_count
FROM {{ ref('adr6') }}
WHERE consnumber IS NULL;
