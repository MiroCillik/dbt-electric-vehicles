with vehicles as (
    select * from {{ ref('stg_electric_vehicles') }}
),

make_counts as (
    select
        county,
        city,
        make,
        count(*) as make_count,
        row_number() over (partition by county, city order by count(*) desc) as rn
    from vehicles
    group by county, city, make
),

top_makes as (
    select
        county,
        city,
        make as top_make
    from make_counts
    where rn = 1
),

by_geography as (
    select
        v.county,
        v.city,
        count(*) as vehicle_count,
        t.top_make,
        round(avg(v.electric_range), 1) as avg_electric_range
    from vehicles v
    left join top_makes t
        on v.county = t.county
        and v.city = t.city
    group by v.county, v.city, t.top_make
)

select * from by_geography
order by vehicle_count desc
