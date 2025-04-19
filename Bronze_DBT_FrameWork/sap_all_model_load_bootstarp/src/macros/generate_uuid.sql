{% macro generate_uuid() %}
    {{ now("iso-8601") | replace(":", "") | replace("-", "") | replace("T", "") | replace("Z", "") }}-{{ range(10) | map("random_letter") | join("") }}
{% endmacro %}