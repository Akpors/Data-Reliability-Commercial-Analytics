-- One row per payment_id; the latest _loaded_at wins. duplicate_count = extra raw copies of this payment.
with ranked as (
    select
        *,
        row_number() over (partition by payment_id order by _loaded_at desc) as rn,
        count(*) over (partition by payment_id) - 1 as duplicate_count
    from {{ ref('stg_payments') }}
)

select
    payment_id,
    order_id,
    payment_ts,
    amount,
    currency,
    method,
    status,
    business_date,
    _loaded_at,
    duplicate_count
from ranked
where rn = 1
