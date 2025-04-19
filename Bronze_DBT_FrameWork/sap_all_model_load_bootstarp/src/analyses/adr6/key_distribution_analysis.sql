SELECT 
    client, 
    COUNT(*) AS record_count
FROM {{ ref('adr6') }}
GROUP BY client
ORDER BY record_count DESC;
