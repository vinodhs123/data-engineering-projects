{% macro initial_checkpoint(checkpoint_table) %}

CREATE TABLE IF NOT EXISTS {{ checkpoint_table }} (model_name STRING, folder_name STRING, processed_at TIMESTAMP);

{% endmacro %}

