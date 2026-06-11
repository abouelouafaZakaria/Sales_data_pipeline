{{ config(materialized='view') }}

select

    data:product_id::int as product_id,

    trim(data:product_name::string) as product_name,

    data:category::string as category,

    case
        when data:price::float <= 0 then null
        else data:price::float
    end as price,

    case
        when data:price::float <= 0 then false
        else true
    end as valid_price

from {{ ref('raw_products') }}
