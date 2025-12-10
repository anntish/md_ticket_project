{{ config(
    materialized = 'incremental',
    unique_key   = ['aviasales_id', 'link']
) }}

with src as (

    select
        -- подставь свои реальные имена колонок из stg-модели
        aviasales_id,
        endpoint,
        status_code,
        error,
        created_at,          -- время запроса
        updated_at,         -- время ответа

        request::jsonb  as req_json,   -- JSON запроса
        response::jsonb as resp_json   -- JSON ответа
    from {{ ref('stg_ticket_offers') }}

    {% if is_incremental() %}
      -- берём только новые/обновлённые строки из stg
      where updated_at > (
        select coalesce(max(updated_at), '1970-01-01'::timestamptz)
        from {{ this }}
      )
    {% endif %}

),

-- расплющиваем массив data в ответе
flattened as (

    select
        s.aviasales_id,
        s.endpoint,
        s.status_code,
        s.error,
        s.created_at,
        s.updated_at,

        -- параметры запроса
        s.req_json ->> 'origin'        as req_origin,
        s.req_json ->> 'destination'   as req_destination,
        to_date(s.req_json ->> 'departure_at', 'YYYY-MM') as req_departure_month,
        s.req_json ->> 'currency'      as req_currency,

        -- общие поля ответа
        s.resp_json ->> 'currency'     as resp_currency,

        f.elem
    from src s
    cross join lateral jsonb_array_elements(s.resp_json -> 'data') as f(elem)

),

final as (

    select
        aviasales_id,
        endpoint,
        status_code,
        error,
        created_at,
        updated_at,

        req_origin,
        req_destination,
        req_departure_month,
        req_currency,
        resp_currency,

        elem ->> 'link'                as link,
        (elem ->> 'price')::int        as price_rub,
        elem ->> 'origin'              as origin,
        elem ->> 'origin_airport'      as origin_airport,
        elem ->> 'destination'         as destination,
        elem ->> 'destination_airport' as destination_airport,
        elem ->> 'airline'             as airline,
        elem ->> 'flight_number'       as flight_number,
        (elem ->> 'duration')::int     as duration_min,
        (elem ->> 'transfers')::int    as transfers_cnt,
        (elem ->> 'duration_to')::int  as duration_to_min,
        (elem ->> 'duration_back')::int        as duration_back_min,
        (elem ->> 'return_transfers')::int     as return_transfers_cnt,
        (elem ->> 'departure_at')::timestamptz as departure_at_utc,

        to_date(
          substring(elem ->> 'link' from 'search_date=([0-9]{8})'),
          'DDMMYYYY'
        ) as search_date

    from flattened
)

select *
from final
