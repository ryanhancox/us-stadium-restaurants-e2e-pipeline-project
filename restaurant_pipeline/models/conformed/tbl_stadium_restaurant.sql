with tbl_stadium as (

    select * from {{ ref('tbl_stadium')}}
),

tbl_restaurant_clean as (

    select * from {{ ref('tbl_restaurant_clean')}}
),

final as (

    select

        tbl_stadium.stadium_id as fk_stadium_id,
        tbl_restaurant_clean.restaurant_id as fk_restaurant_id

    from tbl_stadium
    inner join tbl_restaurant_clean
        on tbl_stadium.stadium_name = tbl_restaurant_clean.stadium
)

select

    {{dbt_utils.generate_surrogate_key([
        'fk_stadium_id', 'fk_restaurant_id'
    ])}} as stadium_restaurant_id,

    *
from final
