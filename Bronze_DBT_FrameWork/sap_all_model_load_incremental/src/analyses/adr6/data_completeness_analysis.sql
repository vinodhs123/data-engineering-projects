SELECT 
    COUNT(*) AS total_records,
    SUM(CASE WHEN client IS NULL THEN 1 ELSE 0 END) AS null_client_count,
    SUM(CASE WHEN addrnumber IS NULL THEN 1 ELSE 0 END) AS null_addrnumber_count,
    SUM(CASE WHEN persnumber IS NULL THEN 1 ELSE 0 END) AS null_persnumber_count,
    SUM(CASE WHEN date_from IS NULL THEN 1 ELSE 0 END) AS null_date_from_count,
    SUM(CASE WHEN consnumber IS NULL THEN 1 ELSE 0 END) AS null_consnumber_count
FROM {{ ref('adr6') }};
