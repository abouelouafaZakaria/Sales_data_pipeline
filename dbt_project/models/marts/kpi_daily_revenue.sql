select
    order_date,
    sum(revenue) as total_revenue,
    count(*) as total_orders
from {{ ref('fct_sales') }}
group by order_date
