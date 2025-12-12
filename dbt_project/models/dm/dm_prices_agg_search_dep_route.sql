{{ config(
    materialized = 'table',
    schema = 'dm',
    tags = ['dm']
) }}

select
    search_date,
    any_value(search_weekday_short) as search_weekday,
    departure_date,
    any_value(departure_weekday_short) as departure_weekday,
    origin,
    destination,
    avg(price_rub) as avg_price_rub
from {{ ref('dm_fact_ticket_offers')}}
group by search_date, departure_date, origin, destination