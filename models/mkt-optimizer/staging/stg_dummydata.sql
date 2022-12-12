
{{
 config(
 materialized = 'table'
 )
}}

WITH myCte AS
(
    select *, current_timestamp() AS updated_at from {{ source('dlt_martin_live_dummydata', 'dummydata') }}
)
select * from myCte