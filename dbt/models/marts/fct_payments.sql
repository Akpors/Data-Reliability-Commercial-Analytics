select
    payment_id,
    order_id,
    business_date,
    payment_ts,
    amount,
    currency,
    method,
    status,
    matched
from {{ ref('int_payments_matched') }}
