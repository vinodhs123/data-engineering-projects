{% macro delete_kfdate_range(target_table, staging_table) %}
    DELETE FROM {{ target_table }}
    WHERE KFDATE BETWEEN
        (SELECT MIN(KFDATE) FROM {{ staging_table }})
        AND
        (SELECT MAX(KFDATE) FROM {{ staging_table }})
{% endmacro %}
