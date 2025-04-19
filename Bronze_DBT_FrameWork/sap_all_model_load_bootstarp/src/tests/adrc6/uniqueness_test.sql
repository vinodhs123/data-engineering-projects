SELECT 
    client,
    addrnumber,
    persnumber,
    date_from,
    consnumber,
    COUNT(*) AS duplicate_count
FROM {{ ref('adr6') }}
GROUP BY client, addrnumber, persnumber, date_from, consnumber
HAVING COUNT(*) > 1;
