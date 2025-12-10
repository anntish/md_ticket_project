{{
    config(
        materialized = 'table',
        unique_key = 'id'
    )
}}

select *
from {{ source("raw_data", "aviasales_api_log") }}
where valid_to_dttm = '5999-12-31 00:00:00.000000 +00:00'
  and endpoint = 'https://api.travelpayouts.com/aviasales/v3/prices_for_dates'