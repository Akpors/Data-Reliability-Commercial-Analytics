-- As arrived, typed. No trimming, cleaning or deduplication. Blank strings become NULL so that a bad
-- value surfaces as a failing not_null test instead of a cast error.
select
    order_id,
    order_line_id,
    nullif(order_date, '')::date as order_date,
    nullif(order_ts, '')::timestamptz as order_ts,
    customer_id,
    product_id,
    nullif(quantity, '')::int as quantity,
    nullif(unit_price, '')::numeric(12, 2) as unit_price,
    (nullif(quantity, '')::int * nullif(unit_price, '')::numeric(12, 2))::numeric(12, 2) as line_amount,
    currency,
    line_type,
    channel,
    business_date,
    _loaded_at,
    _sim_loaded_at,
    _source_file,
    _batch_id
from {{ source('raw', 'sales') }}
