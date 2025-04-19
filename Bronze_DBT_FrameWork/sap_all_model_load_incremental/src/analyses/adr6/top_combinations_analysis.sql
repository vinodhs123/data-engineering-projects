SELECT 
    client, 
    addrnumber, 
    persnumber, 
    date_from, 
    consnumber,
    COUNT(*) AS occurrence_count
FROM {{ ref('adr6') }}
GROUP BY client, addrnumber, persnumber, date_from, consnumber
ORDER BY occurrence_count DESC
LIMIT 10;
