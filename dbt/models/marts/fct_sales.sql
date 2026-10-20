select
    order_line_id,
    order_id,
    business_date,
    order_date,
    customer_id,
    product_id,
    currency,
    line_type,
    quantity,
    unit_price,
    line_amount,
    revenue_gbp
from {{ ref('int_orders_enriched') }}
