{{
 config(
 materialized = 'incremental',
 on_schema_change='fail'
 )
}}

select * from {{ ref('stg_dummydata') }} 
where

{% if is_incremental() %}
  id > (select coalesce(max(id), 0) from {{ this }})
{% endif %}
