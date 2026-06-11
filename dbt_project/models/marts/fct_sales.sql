{{ config(
    materialized='incremental',
    unique_key='order_id'
) }}

select
    o.order_id,
    o.customer_id,
    o.product_id,
    o.quantity,
    p.price,
    o.order_date,

    o.quantity * p.price as revenue

from {{ ref('stg_orders') }} o
left join {{ ref('stg_products') }} p
on o.product_id = p.product_id

{% if is_incremental() %}
where o.order_id > (select max(order_id) from {{ this }})
{% endif %}
