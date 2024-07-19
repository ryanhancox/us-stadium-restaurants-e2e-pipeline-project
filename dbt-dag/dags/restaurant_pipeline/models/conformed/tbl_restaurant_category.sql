with int_restaurant_categories as (

    select * from {{ ref('int_restaurant_categories')}}
),

tbl_category as (

    select * from {{ ref('tbl_category')}}
),

final as (
-- Link table between restaurants and categories

    select

        int_restaurant_categories.restaurant_id as fk_restaurant_id,
        tbl_category.category_id as fk_category_id

    from int_restaurant_categories
    inner join tbl_category
        on int_restaurant_categories.category_name = tbl_category.category_name
)

select

    {{dbt_utils.generate_surrogate_key([
        'fk_restaurant_id', 'fk_category_id'
    ])}} as restaurant_category_id,

    *
from final