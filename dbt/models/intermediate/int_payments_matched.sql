-- Deduplicated payments left-joined to orders. matched = the payment's order has at least one sales line.
select
    p.payment_id,
    p.order_id,
    p.payment_ts,
    p.amount,
    p.currency,
    p.method,
    p.status,
    p.business_date,
    p.duplicate_count,
    (o.order_id is not null) as matched
from {{ ref('int_payments_deduped') }} as p
left join (select distinct order_id from {{ ref('int_sales_deduped') }}) as o
    on o.order_id = p.order_id
