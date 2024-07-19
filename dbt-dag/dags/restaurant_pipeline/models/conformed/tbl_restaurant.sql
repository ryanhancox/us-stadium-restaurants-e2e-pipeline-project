with tbl_restaurant_clean as (

    select * from {{ ref('tbl_restaurant_clean')}}
),

final as (
-- TBL_RESTAURANT_CLEAN contains duplicate restaurants with slighlty varying data in some fields

    select distinct

        restaurant_id,
        restaurant_name,
        is_closed,
        num_reviews,
        restaurant_rating,
        price_bracket,
        phone_number,
        display_phone,
        address_line_1,
        address_line_2,
        address_line_3,
        zip_code,
        restaurant_city,
        restaurant_state,
        display_address,
        longitude,
        latitude,
    
    from tbl_restaurant_clean
)

select * from final