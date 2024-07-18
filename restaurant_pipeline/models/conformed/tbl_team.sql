with tbl_stadium_clean as (

    select * from {{ ref('tbl_stadium_clean')}}
),

tbl_stadium_conformed as (

    select * from {{ ref('tbl_stadium')}}
),

final as (
-- Take only the team information from the stadium data

    select

        tbl_stadium_conformed.stadium_id as fk_stadium_id,
        tbl_stadium_clean.team as team_name,
        tbl_stadium_clean.league

    from tbl_stadium_clean
    inner join tbl_stadium_conformed
        on tbl_stadium_clean.stadium = tbl_stadium_conformed.stadium_name
)

select 

    {{dbt_utils.generate_surrogate_key([
        'team_name', 'league'
    ])}} as team_id,

    *
from final