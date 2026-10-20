-- As-of FX join: each line takes the latest rate on or before its order_date (weekends and holidays fall back
-- to the previous business day). GBP needs no rate. revenue_gbp stays NULL when no rate exists, which the
-- not_null test on fct_sales catches.
select
    s.order_id,
    s.order_line_id,
    s.order_date,
    s.order_ts,
    s.customer_id,
    s.product_id,
    s.quantity,
    s.unit_price,
    s.line_amount,
    s.currency,
    s.line_type,
    s.channel,
    s.business_date,
    s.duplicate_count,
    case when s.currency = 'GBP' then 1::numeric else fx.rate end as fx_rate,
    case
        when s.currency = 'GBP' then s.line_amount
        else round(s.line_amount / fx.rate, 2)
    end as revenue_gbp
from {{ ref('int_sales_deduped') }} as s
left join lateral (
    select f.rate
    from {{ ref('stg_fx_rates') }} as f
    where f.base = 'GBP'
      and f.quote = s.currency
      and f.rate_date <= s.order_date
    order by f.rate_date desc
    limit 1
) as fx on true
