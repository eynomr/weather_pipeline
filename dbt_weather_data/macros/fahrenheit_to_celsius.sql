
{% macro fahrenheit_to_celsius(temp) %}
    ({{ fahrenheit }} - 32) * 5 / 9
{% endmacro %}