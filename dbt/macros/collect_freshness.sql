{#-
  Freshness is measured in simulated time: `snapshotted_at` is the sim clock, not the wall clock.

  dbt resolves `collect_freshness` through adapter.dispatch('collect_freshness', 'dbt'), which looks for
  `postgres__collect_freshness` (root project first, then the adapter's own macros) before falling back to
  `default__collect_freshness` in dbt-adapters. Defining the postgres__ variant here is therefore picked up
  in preference to the default.
-#}
{% macro postgres__collect_freshness(source, loaded_at_field, filter) %}
  {% call statement('collect_freshness', fetch_result=True, auto_begin=False) -%}
    select
      max({{ loaded_at_field }}) as max_loaded_at,
      control.sim_now() as snapshotted_at
    from {{ source }}
    {% if filter %}
    where {{ filter }}
    {% endif %}
  {% endcall %}
  {{ return(load_result('collect_freshness')) }}
{% endmacro %}
