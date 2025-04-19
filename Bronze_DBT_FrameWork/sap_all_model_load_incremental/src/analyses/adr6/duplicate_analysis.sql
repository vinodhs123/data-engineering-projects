WITH duplicates AS (
    SELECT 
        client, 
        addrnumber, 
        persnumber, 
        date_from, 
        consnumber,
        COUNT(*) AS duplicate_count
    FROM {{ ref('adr6') }}
    GROUP BY client, addrnumber, persnumber, date_from, consnumber
    HAVING COUNT(*) > 1
)
SELECT 
    client, 
    addrnumber, 
    persnumber, 
    date_from, 
    consnumber, 
    duplicate_count
FROM duplicates
ORDER BY duplicate_count DESC;
