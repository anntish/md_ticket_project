{{ config(
    materialized = 'table',
    schema = 'dm',
    tags = ['dm']
) }}

select
    departure_date,
    any_value(departure_weekday_short) as departure_weekday,
    origin,
    destination,
    avg(price_rub) as avg_price_rub
from {{ ref('dm_fact_ticket_offers')}}
group by departure_date, origin, destination