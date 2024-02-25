with source as (
      select * from {{ source('staging', 'fhv_tripdata') }}
),
renamed as (
    select
        {{ adapter.quote("dispatching_base_num") }},
        {{ adapter.quote("pickup_datetime") }} ,
        {{ adapter.quote("dropOff_datetime") }} as `dropoff_datetime`,
        {{ adapter.quote("PUlocationID") }} as `pickup_location_id`,
        {{ adapter.quote("DOlocationID") }}as `dropoff_location_id`,
        {{ adapter.quote("SR_Flag") }} as `sr_flag`,
        {{ adapter.quote("Affiliated_base_number") }} as `affiliated_base_number`

    from source
)
select * from renamed
  