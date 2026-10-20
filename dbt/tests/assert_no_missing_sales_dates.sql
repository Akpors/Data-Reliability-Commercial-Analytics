{{ config(severity='warn') }}

-- Dates in dim_date, up to the latest sales business date, that have no stg_sales rows at all.
select d.date_day
from {{ ref('dim_date') }} as d
where d.date_day <= (select max(business_date) from {{ ref('stg_sales') }})
  and not exists (
      select 1
      from {{ ref('stg_sales') }} as s
      where s.business_date = d.date_day
  )
