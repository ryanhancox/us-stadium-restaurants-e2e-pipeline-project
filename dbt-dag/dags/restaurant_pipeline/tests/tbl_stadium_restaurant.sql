select 
    *
from {{ ref('tbl_stadium_restaurant') }}
where distance_from_stadium < 0