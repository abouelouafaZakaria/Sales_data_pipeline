select
    c.region,
    sum(f.revenue) as revenue
from {{ ref('fct_sales') }} f
left join {{ ref('stg_customers') }} c
on f.customer_id = c.customer_id
group by c.region
