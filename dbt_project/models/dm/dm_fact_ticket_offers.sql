{{ config(
    materialized = 'incremental',
    schema = 'dm',
    tags = ['dm']
) }}

with src as (

    select
        aviasales_id,
        link,
        batch_started_at,
        endpoint,
        status_code,
        error,
        created_at,
        updated_at,
        processed_src_dttm,
        processed_at,
        req_departure_month,
        resp_currency as currency,
        price_rub,
        origin,
        origin_airport,
        destination,
        destination_airport,
        airline,
        flight_number,
        duration_min,
        transfers_cnt,
        duration_to_min,
        duration_back_min,
        return_transfers_cnt,
        departure_at_utc,
        search_date,

        case when transfers_cnt = 0 then true else false end as is_direct,

        case
            when price_rub < 5000 then 'cheap'
            when price_rub between 5000 and 15000 then 'medium'
            else 'expensive'
        end as price_bucket,

        date_trunc('day',   departure_at_utc)::date as departure_date,
        date_trunc('month', departure_at_utc)::date as departure_month,


        to_char(departure_at_utc, 'Dy') as departure_weekday_short,
        to_char(departure_at_utc, 'FMDay') as departure_weekday_full,

        to_char(search_date, 'Dy') as search_weekday_short,
        to_char(search_date, 'FMDay') as search_weekday_full


    from {{ ref('ods_ticket_offers') }}
    -- инкремент по watermark из ODS
    {% if is_incremental() %}
      where processed_src_dttm > (
        select coalesce(max(processed_src_dttm), '1970-01-01'::timestamptz)
        from {{ this }}
      )
    {% endif %}
)

select *
from src
