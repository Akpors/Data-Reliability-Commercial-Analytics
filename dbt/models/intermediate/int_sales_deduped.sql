-- One row per order_line_id; the latest _loaded_at wins. duplicate_count = extra raw copies of this line
-- (sum it by business_date for the per-day duplicate volume).
with ranked as (
    select
        *,
        row_number() over (partition by order_line_id order by _loaded_at desc) as rn,
        count(*) over (partition by order_line_id) - 1 as duplicate_count
    from {{ ref('stg_sales') }}
)

select
    order_id,
    order_line_id,
    order_date,
    order_ts,
    customer_id,
    product_id,
    quantity,
    unit_price,
    line_amount,
    currency,
    line_type,
    channel,
    business_date,
    _loaded_at,
    duplicate_count
from ranked
where rn = 1
