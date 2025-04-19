SELECT 
    DISTINCT client
FROM {{ ref('adr6') }}
WHERE client NOT IN ('100', '200', '300');
