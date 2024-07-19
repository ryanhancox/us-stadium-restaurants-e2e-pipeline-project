with tbl_restaurant_clean as (
    
    select * from {{ ref('tbl_restaurant_clean')}}
),

final as (
-- Flatten clean category array so there is a restaurant record per category

    select distinct

        restaurant_id,
        replace(categories.value, '"', '') as category_name

    from tbl_restaurant_clean,
    table(flatten(restaurant_categories)) categories
)

select * from final