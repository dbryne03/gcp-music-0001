{% macro normalise_name(column) %}
    lower(regexp_replace(trim({{ column }}), r'[^a-z0-9 ]', ''))
{% endmacro %}
