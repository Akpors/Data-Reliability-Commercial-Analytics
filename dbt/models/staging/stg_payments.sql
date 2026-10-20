select
    payment_id,
    order_id,
    nullif(payment_ts, '')::timestamptz as payment_ts,
    nullif(amount, '')::numeric(12, 2) as amount,
    currency,
    method,
    status,
    business_date,
    _loaded_at,
    _sim_loaded_at,
    _source_file,
    _batch_id
from {{ source('raw', 'payments') }}
