with source as (
    select * from {{ source('out.c-dbt', 'Electric_Vehicle_Population_Data') }}
),

renamed as (
    select
        "VIN (1-10)"                                         as vin,
        "County"                                             as county,
        "City"                                               as city,
        "State"                                              as state,
        "Postal Code"                                        as postal_code,
        cast("Model Year" as integer)                        as model_year,
        "Make"                                               as make,
        "Model"                                              as model,
        "Electric Vehicle Type"                              as electric_vehicle_type,
        "Clean Alternative Fuel Vehicle (CAFV) Eligibility"  as cafv_eligibility,
        cast("Electric Range" as integer)                    as electric_range,
        cast("Base MSRP" as integer)                         as base_msrp,
        "Legislative District"                               as legislative_district,
        "DOL Vehicle ID"                                     as dol_vehicle_id,
        "Vehicle Location"                                   as vehicle_location,
        "Electric Utility"                                   as electric_utility,
        "2020 Census Tract"                                  as census_tract_2020
    from source
)

select * from renamed
