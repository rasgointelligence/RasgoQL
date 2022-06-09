{% macro drop_columns(source_table, include_cols, exclude_cols)%}

{% if include_cols and exclude_cols is defined %}
{{ raise_exception('You cannot pass both an include_cols list and an exclude_cols list') }}
{% else %}

{%- if include_cols is defined -%}
SELECT
{%- for col in include_cols %}
    {{col}}{{ ", " if not loop.last else " " }}
{%- endfor %}
FROM {{source_table}}
{%- endif -%}

{%- if exclude_cols is defined -%}
{%- set source_col_names = get_columns(source_table) -%}
{%- set new_columns = source_col_names | reject('in', exclude_cols) -%}

SELECT
{%- for col in new_columns %}
    {{ col }}{{ ", " if not loop.last else " " }}
{%- endfor %}
FROM {{ source_table }}

{%- endif -%}
{%- endif -%}

{% endmacro %}
