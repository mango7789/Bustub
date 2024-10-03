select name, count(*) as number 
from athletes as a
join (
    select athletes_code as code from teams
    join medals on teams.code = medals.winner_code
    where medals.discipline = 'Judo'
    union all
    select athletes.code as code from athletes
    join medals on athletes.code = medals.winner_code
    where medals.discipline = 'Judo'
) as t
on a.code = t.code
group by name
order by number desc, name;