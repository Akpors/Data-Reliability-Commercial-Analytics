-- Daily files are full snapshots: keep only the latest one.
select
    customer_id,
    email,
    full_name,
    country,
    nullif(signup_date, '')::date as signup_date,
    segment,
    business_date,
    _loaded_at,
    _sim_loaded_at,
    _source_file,
    _batch_id
from {{ source('raw', 'customers') }}
where business_date = (select max(business_date) from {{ source('raw', 'customers') }})
