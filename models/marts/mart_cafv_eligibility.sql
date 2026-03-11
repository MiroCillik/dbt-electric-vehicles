with vehicles as (
    select * from {{ ref('stg_electric_vehicles') }}
),

by_eligibility as (
    select
        cafv_eligibility,
        model_year,
        electric_vehicle_type,
        count(*) as vehicle_count,
        round(avg(electric_range), 1) as avg_electric_range,
        round(avg(case when base_msrp > 0 then base_msrp end), 0) as avg_base_msrp
    from vehicles
    group by cafv_eligibility, model_year, electric_vehicle_type
)

select * from by_eligibility
order by cafv_eligibility, model_year desc, electric_vehicle_type
