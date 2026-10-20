-- Raw tables hold source columns as text; typing happens in dbt staging.
-- Every raw row carries the metadata columns business_date .. _extra.

create table raw.sales (
    order_id text,
    order_line_id text,
    order_date text,
    order_ts text,
    customer_id text,
    product_id text,
    quantity text,
    unit_price text,
    currency text,
    line_type text,
    channel text,
    business_date  date,
    _loaded_at     timestamptz not null default now(),
    _sim_loaded_at timestamptz,
    _source_file   text,
    _batch_id      text,
    _extra         jsonb
);

create table raw.payments (
    payment_id text,
    order_id text,
    payment_ts text,
    amount text,
    currency text,
    method text,
    status text,
    business_date  date,
    _loaded_at     timestamptz not null default now(),
    _sim_loaded_at timestamptz,
    _source_file   text,
    _batch_id      text,
    _extra         jsonb
);

create table raw.customers (
    customer_id text,
    email text,
    full_name text,
    country text,
    signup_date text,
    segment text,
    business_date  date,
    _loaded_at     timestamptz not null default now(),
    _sim_loaded_at timestamptz,
    _source_file   text,
    _batch_id      text,
    _extra         jsonb
);

create table raw.products (
    product_id text,
    sku text,
    product_name text,
    category text,
    list_price_gbp text,
    is_active text,
    business_date  date,
    _loaded_at     timestamptz not null default now(),
    _sim_loaded_at timestamptz,
    _source_file   text,
    _batch_id      text,
    _extra         jsonb
);

create table raw.fx_rates (
    rate_date      date not null,
    base           text not null,
    quote          text not null,
    rate           numeric(18, 8) not null,
    _loaded_at     timestamptz not null default now(),
    _sim_loaded_at timestamptz,
    _batch_id      text,
    primary key (rate_date, base, quote)
);

create table quarantine.files (
    id               serial primary key,
    source           text not null,
    file_name        text not null,
    stored_path      text,
    contract_version int,
    diff             jsonb,
    sim_now          timestamptz,
    quarantined_at   timestamptz not null default now(),
    replayed_at      timestamptz
);
