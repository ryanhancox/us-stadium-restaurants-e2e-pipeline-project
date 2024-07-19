with tbl_stadium_clean as (

    select * from {{ ref('tbl_stadium_clean')}}
),

final as (
-- Conformed stadium data. Max values taken due to duplicates.

    select
        
        stadium as stadium_name,
        max(city) as city,
        state,
        max(stadium_longitude) as longitude,
        max(stadium_latitude) as latitude

    from tbl_stadium_clean
    group by stadium, state 
)

select 

    {{ dbt_utils.generate_surrogate_key([
            'stadium_name',
            'city',
            'state'
    ])}} as stadium_id,

    * 
from final
