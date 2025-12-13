{{ config(
    materialized = 'table',
    schema = 'dm',
    tags = ['dm']
) }}

select
    origin,
    destination,
    avg(price_rub) as avg_price_rub
from {{ ref('dm_fact_ticket_offers')}}
group by origin,
    destination