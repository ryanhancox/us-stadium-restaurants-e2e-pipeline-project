with raw_restaurants as (
    
    select * from {{ source('raw', 'raw_stadium_restaurants') }}
),

int_stadium_state_clean as (

    select * from {{ ref('int_stadium_state_clean') }}
),

unnested_restaurants as (
-- Unnest the restaurant data that belongs to each stadium.
-- Restaurant 'category' data is still nested as a list of dictionaries at this point.
    
    select

        raw_restaurants.stadium::text as stadium,
        b.value:id::text as restaurant_id,
        b.value:name::text as restaurant_name,
        b.value:is_closed::boolean as is_closed,
        b.value:url::text as restaurant_url,
        b.value:review_count::float as num_reviews,
        b.value:rating::float as restaurant_rating,
        b.value:price::text as price_bracket,
        b.value:phone::text as phone_number,
        b.value:display_phone::text as display_phone,
        b.value:location:address1::text as address_line_1,

        case 
            when b.value:location:address2::text = '' then null 
            else b.value:location:address2::text
        end as address_line_2,

        case 
            when b.value:location:address3::text = '' then null 
            else b.value:location:address3::text
        end as address_line_3,

        b.value:location:zip_code::text as zip_code,

        -- cleans address data which is a list of strings
        replace(substr(b.value:location:display_address::text, 3, len(b.value:location:display_address::text)-4), '","', ', ') as display_address,
        b.value:location:city::text as restaurant_city,
        int_stadium_state_clean.state_name as restaurant_state,
        b.value:coordinates:longitude::decimal(7,4) as restaurant_longitude,
        b.value:coordinates:latitude::decimal(7,3) as restaurant_latitude,
        b.value:distance::float as distance_from_stadium,
        b.value:categories::array as raw_category_array
    from raw_restaurants
    inner join int_stadium_state_clean
        on raw_restaurants.stadium = int_stadium_state_clean.stadium,
    lateral flatten (input => businesses) b
),

clean_categories as (
-- Un-nest the category data for each restaurant and group back into a clean array

    select

        stadium,
        restaurant_id,
        restaurant_name,
        is_closed,
        restaurant_url,
        num_reviews,
        restaurant_rating,
        price_bracket,
        phone_number,
        display_phone,
        address_line_1,
        address_line_2,
        address_line_3,
        zip_code,
        display_address,
        restaurant_city,
        restaurant_state,
        restaurant_longitude,
        restaurant_latitude,
        distance_from_stadium,
        array_agg(raw_cat.value:title::text) within group (order by raw_cat.value:title::text) as restaurant_categories

    from unnested_restaurants,
    lateral flatten (input => raw_category_array) raw_cat
    group by all
),

final as (
-- Remove duplicate restaurant entries

    select distinct

        stadium,
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
        max(distance_from_stadium) over (partition by restaurant_id) as distance_from_stadium,
        restaurant_categories

    from clean_categories

)

select * from final


 
