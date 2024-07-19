with raw_restaurants as (

    select * from {{ source('raw', 'raw_stadium_restaurants') }}
),

ref_state_codes_code as (

    select * from {{ ref('ref_state_codes') }}
),

ref_state_codes_state as (

    select * from {{ ref('ref_state_codes') }}
),

ref_state_codes_abbrev as (

    select * from {{ ref('ref_state_codes') }}
),

final as (
-- State data is inconsistent in source (mixture of codes/names/abbrev) so this 
-- int table ensures that state names are correct and correct for each stadium
    
    select distinct

        raw_restaurants.stadium,
        case
            when ref_state_codes_code.state is not null then ref_state_codes_code.state
            when ref_state_codes_state.state is not null then ref_state_codes_state.state
            when ref_state_codes_abbrev.state is not null then ref_state_codes_abbrev.state
            when raw_restaurants.state = 'Minnessota' then 'Minnesota'
            else raw_restaurants.state
        end as state_name

    from raw_restaurants
    left join ref_state_codes_code 
        on raw_restaurants.state = ref_state_codes_code.code
    left join ref_state_codes_state 
        on raw_restaurants.state = ref_state_codes_state.state
    left join ref_state_codes_abbrev 
        on raw_restaurants.state = ref_state_codes_abbrev.abbreviation
)

select * from final



