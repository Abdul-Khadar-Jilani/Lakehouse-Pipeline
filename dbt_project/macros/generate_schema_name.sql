/*
  Override dbt's default schema generation.

  By default, dbt creates datasets named: {default_dataset}_{custom_schema}
  (e.g., weather_raw_weather_staging). This macro makes it use the exact
  custom schema name instead (e.g., just "weather_staging").
*/

{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
