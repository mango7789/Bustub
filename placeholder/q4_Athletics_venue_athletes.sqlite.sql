with codes as (
    select participant_code 
    from results
    where venue in (
        select venue from venues
        where disciplines like "%Athletics%"
    )
),
dists as (
    select c1.code as cod1, c2.code as cod2,
    ((c1.Latitude - c2.Latitude) * (c1.Latitude - c2.Latitude) + (c1.Longitude - c2.Longitude) * (c1.Longitude - c2.Longitude)) as distance
    from countries c1
    join countries c2 on c1.code != c2.code
)

select name, country_code, nationality_code 
from (
    select name, country_code, nationality_code, (
        select distance from dists
        where cod1 = country_code and cod2 = nationality_code
    ) as distance
    from athletes inner join codes
    on athletes.code = codes.participant_code
    union
    select name, country_code, nationality_code, (
        select distance from dists
        where cod1 = country_code and cod2 = nationality_code
    ) as distance
    from athletes
    where code in (
        select athletes_code 
        from teams inner join codes
        on teams.code = codes.participant_code 
    )
)
order by distance desc, name;