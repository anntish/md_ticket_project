{{ config(
    materialized = 'incremental',
    unique_key   = ['aviasales_id', 'link'],
    incremental_strategy = 'merge'
) }}

with src as (

    select
        aviasales_id,
        endpoint,
        batch_started_at,
        status_code,
        error,
        created_at,           -- время запроса
        updated_at,           -- время ответа
        valid_from_dttm,      -- "ingested_at" из SCD2-лога

        request::jsonb  as req_json,   -- JSON запроса
        response::jsonb as resp_json   -- JSON ответа
    from {{ ref('stg_ticket_offers') }}

    {% if is_incremental() %}
      -- берём только новые/обновлённые строки из stg
      -- относительно уже загруженных processed_src_dttm
      where valid_from_dttm >= (
        select coalesce(max(processed_src_dttm), '1970-01-01'::timestamptz)
        from {{ this }}
      )
    {% endif %}

),

-- расплющиваем массив data в ответе
flattened as (

    select
        s.aviasales_id,
        s.endpoint,
        s.batch_started_at,
        s.status_code,
        s.error,
        s.created_at,
        s.updated_at,
        s.valid_from_dttm,

        -- параметры запроса
        s.req_json ->> 'origin'        as req_origin,
        s.req_json ->> 'destination'   as req_destination,
        to_date(s.req_json ->> 'departure_at', 'YYYY-MM') as req_departure_month,
        s.req_json ->> 'currency'      as req_currency,
        s.req_json ->> 'one_way'       as req_one_way,

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
        batch_started_at,
        status_code,
        error,
        created_at,
        updated_at,

        -- технические поля для инкремента/даунстрима
        valid_from_dttm              as processed_src_dttm,
        current_timestamp            as processed_at,

        req_origin,
        req_destination,
        req_departure_month,
        req_currency,
        req_one_way,
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
    where req_one_way = 'true'
)

select *
from final
