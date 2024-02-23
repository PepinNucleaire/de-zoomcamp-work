with source as (
      select * from {{ source('staging', 'yellow_tripdata') }}
),
renamed as (
    select
        {{ adapter.quote("vendor_id") }},
        {{ adapter.quote("pickup_datetime") }},
        {{ adapter.quote("dropoff_datetime") }},
        {{ adapter.quote("passenger_count") }},
        {{ adapter.quote("trip_distance") }},
        {{ adapter.quote("rate_code") }},
        {{ adapter.quote("store_and_fwd_flag") }},
        {{ adapter.quote("payment_type") }},
        {{ get_payment_type_description(adapter.quote("payment_type")) }} 
        as {{adapter.quote("payment_type_description")}},
        {{ adapter.quote("fare_amount") }},
        {{ adapter.quote("extra") }},
        {{ adapter.quote("mta_tax") }},
        {{ adapter.quote("tip_amount") }},
        {{ adapter.quote("tolls_amount") }},
        {{ adapter.quote("imp_surcharge") }},
        {{ adapter.quote("airport_fee") }},
        {{ adapter.quote("total_amount") }},
        {{ adapter.quote("pickup_location_id") }},
        {{ adapter.quote("dropoff_location_id") }},
        {{ adapter.quote("data_file_year") }},
        {{ adapter.quote("data_file_month") }}

    from source
)
select * from renamed
  