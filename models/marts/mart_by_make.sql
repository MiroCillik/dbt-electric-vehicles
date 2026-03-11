with vehicles as (
    select * from {{ ref('stg_electric_vehicles') }}
),

by_make as (
    select
        make,
        count(*) as vehicle_count,
        round(avg(electric_range), 1) as avg_electric_range,
        count(case when electric_vehicle_type = 'Battery Electric Vehicle (BEV)' then 1 end) as bev_count,
        count(case when electric_vehicle_type = 'Plug-in Hybrid Electric Vehicle (PHEV)' then 1 end) as phev_count,
        round(
            100.0 * count(case when electric_vehicle_type = 'Battery Electric Vehicle (BEV)' then 1 end)
            / count(*),
            1
        ) as bev_percentage
    from vehicles
    group by make
)

select * from by_make
order by vehicle_count desc
