-- Rates are quoted per 1 unit of base (GBP). ECB publishes business days only, so weekends have no rows.
select
    rate_date,
    base,
    quote,
    rate,
    _loaded_at,
    _sim_loaded_at,
    _batch_id
from {{ source('raw', 'fx_rates') }}
