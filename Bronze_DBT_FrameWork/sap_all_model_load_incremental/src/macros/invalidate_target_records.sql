{% macro invalidate_target_records(target_table, staging_table, composite_keys, change_date_column) %}
MERGE INTO {{ target_table }} AS target
USING (
    SELECT 
        {% for key in composite_keys %}
            {{ key }},
        {% endfor %}
        MAX({{ change_date_column }}) AS max_change_date
    FROM {{ staging_table }}
    WHERE row_num = 1 
    AND is_active = TRUE
    GROUP BY 
        {% for key in composite_keys %}
            {{ key }}{% if not loop.last %}, {% endif %}
        {% endfor %}
) AS staging
ON 
    {% for key in composite_keys %}
        target.{{ key }} = staging.{{ key }}
        {% if not loop.last %} AND {% endif %}
    {% endfor %}
WHEN MATCHED THEN
    UPDATE SET 
        target.is_active = FALSE,
        target.valid_to = staging.max_change_date;
{% endmacro %}
