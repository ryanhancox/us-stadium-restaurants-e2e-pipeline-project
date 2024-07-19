with int_restaurant_categories as (

    select * from {{ ref('int_restaurant_categories')}}
),

final as (
-- Simply retrieve distinct restaurant categories

    select distinct category_name
    from int_restaurant_categories
)

select

    {{dbt_utils.generate_surrogate_key([
        'category_name'
    ])}} as category_id,

    *
from final