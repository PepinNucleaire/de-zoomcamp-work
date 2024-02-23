with source as (
      select * from {{ source('staging', 'green_tripdata') }}
),
renamed as (
    select
        {{ adapter.quote("vendor_id") }},
        {{ adapter.quote("pickup_datetime") }},
        {{ adapter.quote("dropoff_datetime") }},
        {{ adapter.quote("store_and_fwd_flag") }},
        {{ adapter.quote("rate_code") }},
        {{ adapter.quote("passenger_count") }},
        {{ adapter.quote("trip_distance") }},
        {{ adapter.quote("fare_amount") }},
        {{ adapter.quote("extra") }},
        {{ adapter.quote("mta_tax") }},
        {{ adapter.quote("tip_amount") }},
        {{ adapter.quote("tolls_amount") }},
        {{ adapter.quote("ehail_fee") }},
        {{ adapter.quote("airport_fee") }},
        {{ adapter.quote("total_amount") }},
        {{ adapter.quote("payment_type") }},
        {{ adapter.quote("distance_between_service") }},
        {{ adapter.quote("time_between_service") }},
        {{ adapter.quote("trip_type") }},
        {{ adapter.quote("imp_surcharge") }},
        {{ adapter.quote("pickup_location_id") }},
        {{ adapter.quote("dropoff_location_id") }},
        {{ adapter.quote("data_file_year") }},
        {{ adapter.quote("data_file_month") }}

    from source
)
select * from renamed
  