{{ config(materialized='view') }}

select

    data:order_id::int as order_id,

    data:customer_id::int as customer_id,

    data:product_id::int as product_id,

    case
        when data:quantity::int <= 0 then null
        when data:quantity::int > 1000 then null
        else data:quantity::int
    end as quantity,

    case
        when try_to_timestamp(data:order_date::string) is null
        then null
        else try_to_timestamp(data:order_date::string)
    end as order_date

from {{ ref('raw_orders') }}
