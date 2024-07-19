with raw_state_codes as (
    select * from {{ source('raw', 'raw_state_codes') }} 
),

final as (
    
    select
        code,
        state,
        abbrev as abbreviation
    from raw_state_codes
)

select * from final