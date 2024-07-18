with tbl_restaurant_clean as (

    select * from {{ ref('tbl_restaurant_clean')}}
),

final as (
-- TBL_RESTAURANT_CLEAN contains duplicate restaurants with slighlty varying data in some fields

    select distinct

        restaurant_id,
        restaurant_name,
        is_closed,
        -- duplicate restaurants with differing reviews/ratings
        last_value(num_reviews) over (partition by restaurant_id order by num_reviews) as num_reviews,
        last_value(restaurant_rating) over (partition by restaurant_id order by num_reviews) as restaurant_rating,

        case 
            when price_bracket is not null then price_bracket
            else 'Not Available'
        end as price_bracket,

        -- duplicate restaurants where one doesn't contain phone numbers
        max(phone_number) over (partition by restaurant_id) as phone_number,
        max(display_phone) over (partition by restaurant_id) as display_phone,
        address_line_1,
        address_line_2,
        address_line_3,
        zip_code,
        restaurant_city,
        restaurant_state,
        display_address,
        -- duplicate restaurants with slightly different lat/longs
        max(restaurant_longitude) over (partition by restaurant_id) as longitude,
        max(restaurant_latitude) over (partition by restaurant_id) as latitude,
    
    from tbl_restaurant_clean
)

select * from final