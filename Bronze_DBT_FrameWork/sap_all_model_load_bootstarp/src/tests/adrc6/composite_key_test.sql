SELECT 
    COUNT(*) AS total_rows,
    COUNT(DISTINCT CONCAT(client, '_', addrnumber, '_', persnumber, '_', date_from, '_', consnumber)) AS unique_composite_keys
FROM {{ ref('adr6') }}
HAVING COUNT(*) = COUNT(DISTINCT CONCAT(client, '_', addrnumber, '_', persnumber, '_', date_from, '_', consnumber));
