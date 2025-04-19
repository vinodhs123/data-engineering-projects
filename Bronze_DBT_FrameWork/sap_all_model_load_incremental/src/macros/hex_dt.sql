{% macro hex_to_timestamp(hex_value) %}
DATE_FORMAT(
    TIMESTAMP_SECONDS(
        CAST(CONV(SUBSTRING({{ hex_value }}, 1, 8), 16, 10) AS BIGINT)
    ) + 
    (CAST(CONV(SUBSTRING({{ hex_value }}, 9, 8), 16, 10) AS DOUBLE) / 1000) * INTERVAL 1 SECOND,
    'yyyy-MM-dd HH:mm:ss.SSS'
)
{% endmacro %}