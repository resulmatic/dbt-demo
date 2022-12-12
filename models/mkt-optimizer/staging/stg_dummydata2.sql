{{
 config(
 materialized = 'incremental',
 on_schema_change='fail'
 )
}}
select * from {{ ref('stg_dummydata') }} 