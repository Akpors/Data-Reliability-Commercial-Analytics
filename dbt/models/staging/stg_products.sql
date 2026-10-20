-- Daily files are full snapshots: keep only the latest one.
select
    product_id,
    sku,
    product_name,
    category,
    nullif(list_price_gbp, '')::numeric(12, 2) as list_price_gbp,
    (lower(nullif(is_active, '')))::boolean as is_active,
    business_date,
    _loaded_at,
    _sim_loaded_at,
    _source_file,
    _batch_id
from {{ source('raw', 'products') }}
where business_date = (select max(business_date) from {{ source('raw', 'products') }})
