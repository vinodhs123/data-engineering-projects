SELECT 
    DISTINCT a.addrnumber
FROM {{ ref('adr6') }} a
LEFT JOIN {{ ref('addresses') }} b
ON a.addrnumber = b.addrnumber
WHERE b.addrnumber IS NULL;
