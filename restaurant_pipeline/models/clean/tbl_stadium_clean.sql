with raw_restaurants as (

    select * from {{ source('raw', 'raw_stadium_restaurants') }}
),

int_stadium_state_clean as (

    select * from {{ ref('int_stadium_state_clean') }}
),

final as (
-- Obtain stadium data with cleaned stadium state name 
    select

        raw_restaurants.league,
        raw_restaurants.team,
        raw_restaurants.stadium,
        raw_restaurants.city,
        int_stadium_state_clean.state_name as state,
        raw_restaurants.stadium_longitude,
        raw_restaurants.stadium_latitude

    from raw_restaurants
    inner join int_stadium_state_clean 
        on raw_restaurants.stadium = int_stadium_state_clean.stadium
)

select * from final
