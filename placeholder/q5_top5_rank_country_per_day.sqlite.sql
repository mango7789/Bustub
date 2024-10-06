with top5_countries as (
    select date, country_code, count(*) as appearances
    from results
    join (
        select code, country_code
        from athletes
        union
        select code, country_code
        from teams
    ) as tmp
    on results.participant_code = tmp.code
    where rank <= 5
    group by date, country_code
),
most_appearances as (
    select date, country_code, appearances, rank() over (partition by date order by appearances desc) as rank_within_day
    from top5_countries
),
best_country_per_day as (
    select date, country_code, appearances
    from most_appearances
    where rank_within_day = 1
),
country_rank as (
    select code, 
    rank() over (order by "GDP ($ per capita)" desc) as gdp_rank, 
    rank() over (order by Population desc) as population_rank
    from countries
)

select date, country_code, appearances, gdp_rank, population_rank
from best_country_per_day
join country_rank
on country_code = code;