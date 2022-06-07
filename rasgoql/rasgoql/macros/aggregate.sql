{% macro aggregate(source_table, group_by, aggregations)}

{%- set final_col_list = [] -%}
{%- set entropy_aggs = {} -%}
{%- set from_tables = [source_table] -%}
{%- for col, aggs in aggregations.items() -%}
    {%- if 'ENTROPY' in aggs -%}
        {%- set _ = entropy_aggs.update({col: aggs}) -%}
    {%- endif -%}
{%- endfor -%}

WITH BASIC_AGGS AS (
SELECT
{%- for group_item in group_by %}
    {{ group_item }},
    {%- do final_col_list.append('BASIC_AGGS.' ~ group_item) -%}
{%- endfor -%}
{%- for col, aggs in aggregations.items() %}
 {%- set outer_loop = loop -%}
  {%- for agg in aggs %}
   {%- if agg == 'ENTROPY' -%}
    {%- set entropy_flag = True -%}
   {%- endif -%}
    {%- if not entropy_flag %}
     {%- if ' DISTINCT' in agg %}
      {{ agg|replace(" DISTINCT", "") }}(DISTINCT {{ col }}) as {{ col ~ '_' ~ agg|replace(" DISTINCT", "") ~ 'DISTINCT'}},
      {%- do final_col_list.append('BASIC_AGGS.' ~ col ~ '_' ~ agg|replace(" DISTINCT", "") ~ 'DISTINCT') -%}
     {%- else %}
     {{ agg }}({{ col }}) as {{ col ~ '_' ~ agg }},
     {%- do final_col_list.append('BASIC_AGGS.' ~ col ~ '_' ~ agg) -%}
     {%- endif -%}
    {%- endif -%}
{%- endfor -%}
{%- endfor %}
COUNT(1) AS AGG_ROW_COUNT
FROM {{ source_table }}
GROUP BY {{ group_by | join(', ') }})
{%- for col, aggs in entropy_aggs.items() -%}
,
CTE_{{ col }} AS (
SELECT
{%- for group_item in group_by %}
    {{ group_item }},
{%- endfor -%}
    {{ col }},
    COUNT(1) AS C
FROM {{ source_table }}
GROUP BY {{ group_by | join(', ') }},{{ col }}
),
CTE_{{ col }}_RATIO AS (
SELECT
{%- for group_item in group_by %}
    {{ group_item }},
{%- endfor -%}
    {{ col }},
    C / SUM(C) OVER (PARTITION BY {{ group_by | join(', ') }}) AS P
FROM CTE_{{ col }}
),
CTE_{{ col }}_ENTROPY AS (
SELECT {%- for group_item in group_by %}
    {{ group_item }},
{%- endfor -%} -SUM(P*LOG(2,P)) AS {{ col }}_ENTROPY
FROM CTE_{{ col }}_RATIO
GROUP BY {{ group_by | join(', ') }}

{%- do final_col_list.append('CTE_' ~ col ~ '_ENTROPY.' ~ col ~ '_ENTROPY') -%}
){%- endfor %}

SELECT {{ final_col_list|join(', ') }}
FROM BASIC_AGGS
{%- for col, aggs in entropy_aggs.items() %}
LEFT OUTER JOIN CTE_{{ col }}_ENTROPY ON
{%- for group_item in group_by %}
    BASIC_AGGS.{{ group_item }} = CTE_{{ col }}_ENTROPY.{{ group_item }}{{ '' if loop.last else ' AND ' }}
{%- endfor -%}
{%- endfor %}

{% endmacro %}
