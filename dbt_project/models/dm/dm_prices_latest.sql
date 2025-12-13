{{ config(
    materialized = 'incremental',
    schema = 'dm',
    tags = ['dm'],
    strategy = 'merge'
) }}

with base as (

    select
        *
    from {{ ref('dm_fact_ticket_offers') }}
    {% if is_incremental() %}
      where processed_src_dttm > (
        select coalesce(max(processed_src_dttm), '1970-01-01'::timestamptz)
        from {{ this }}
      )
    {% endif %}

),

ranked as (

    select
        base.*,

        row_number() over (
            partition by departure_date, destination, origin
            order by created_at asc, processed_src_dttm asc, aviasales_id asc
        ) as rn

    from base

)

select
    *
from ranked
where rn = 1
