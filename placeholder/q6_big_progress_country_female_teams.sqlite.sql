with all_athletes as (
    select code, country_code
    from athletes
    union
    select code, country_code
    from teams
),
current_medal as (
    select country_code, count(*) as curr_medal
    from medals
    join all_athletes
    on medals.winner_code = all_athletes.code
    where medal_code = 1
    group by country_code
),
difference as (
    select 
        current_medal.country_code as country_code, 
        (curr_medal - gold_medal) as increased_gold_medal_number,
        rank() over (order by (curr_medal - gold_medal) desc) as pos
    from current_medal
    join tokyo_medals
    on current_medal.country_code = tokyo_medals.country_code
),
top_5_country as (
    select country_code, increased_gold_medal_number 
    from difference
    where pos <= 5
), 
country_team as (
    select 
        teams.country_code as country_code, 
        increased_gold_medal_number, 
        teams.code as team_code,
        count(case when gender = 1 then 1 end) as female_count,
        count(*) as total_count
    from teams
    join top_5_country
    on teams.country_code = top_5_country.country_code 
    join athletes
    on teams.athletes_code = athletes.code
    group by team_code
)

select country_code, increased_gold_medal_number, team_code
from country_team
where female_count = total_count
order by increased_gold_medal_number desc, country_code, team_code;
