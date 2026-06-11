{{ config(materialized='view') }}

select

    data:customer_id::int as customer_id,

    trim(data:name::string) as name,

    case
        when data:region::string in
        (
            'Europe',
            'North America',
            'Asia',
            'Africa'
        )
        then data:region::string

        else 'Unknown'
    end as region

from {{ ref('raw_customers') }}
