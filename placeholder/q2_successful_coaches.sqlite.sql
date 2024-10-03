select name, count(*) as number 
from coaches as c
join (
    select discipline, country_code from
    medals as m join (
        select code, country_code from athletes
        union
        select code, country_code from teams
    ) as a
    on m.winner_code = a.code
) as t
on c.country_code = t.country_code and c.discipline = t.discipline
group by name
order by number desc, name;