{{ config(
    materialized = 'table',
    schema = 'dm',
    tags = ['dm']
) }}
with airports_and_cities as(
select
    origin_airport as airport_code,
    origin as city_code
from {{ref('dm_fact_ticket_offers')}}
union
select
    destination_airport as airport_code,
    destination as city_code
from {{ ref('dm_fact_ticket_offers')}})

select distinct airport_code, city_code from airports_and_cities
