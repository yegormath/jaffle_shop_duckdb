{% macro conditional_sum(column, condition_column, condition_value) %}
    SUM(CASE WHEN {{ condition_column }} = '{{ condition_value }}' THEN {{ column }} ELSE 0 END)
{% endmacro %}