{% macro cast(source_table, casts, overwrite_columns) %}

{%- if overwrite_columns == true -%}

{%- set source_columns = get_columns(source_table) -%}
{%- set untouched_cols = source_columns | reject('in', casts) -%}

SELECT {% for col in untouched_cols %}{{ col }},{% endfor %}
{%- for target_col, type in casts.items() %}
    CAST({{target_col}} AS {{type}}) AS {{target_col}}{{", " if not loop.last else ""}}
{%- endfor %}
FROM {{ source_table }}

{%- else -%}

SELECT *
{%- for target_col, type in casts.items() %}
    , CAST({{target_col}} AS {{type}}) AS {{cleanse_name(target_col)+'_'+cleanse_name(type)}}
{%- endfor %}
FROM {{ source_table }}

{%- endif -%}

{% endmacro %}
