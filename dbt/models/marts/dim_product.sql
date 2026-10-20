select
    product_id,
    sku,
    product_name,
    category,
    list_price_gbp,
    is_active
from {{ ref('stg_products') }}
