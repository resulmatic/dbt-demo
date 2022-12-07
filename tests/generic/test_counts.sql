{% test counts(model, column_name, column_value, required_value) %}

with cnts as (

    select
        count(*) as cnt

    from {{ model }} where {{ column_name }} = '{{ column_value }}'

)

select *
from cnts
where cnt < {{ required_value }}

{% endtest %}