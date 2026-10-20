select
    customer_id,
    email,
    full_name,
    country,
    signup_date,
    segment
from {{ ref('stg_customers') }}
