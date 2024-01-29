import io
import pandas as pd
import requests
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


base_url = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/'
taxi_dtypes = {
    'VendorID': "Int64",
    'passenger_count': "Int64",
    'trip_distance': float,
    'RatecodeID':"Int64",
    'store_and_fwd_flag':str,
    'PULocationID':"Int64",
    'DOLocationID':"Int64",
    'payment_type': "Int64",
    'fare_amount': float,
    'extra':float,
    'mta_tax':float,
    'tip_amount':float,
    'tolls_amount':float,
    'improvement_surcharge':float,
    'total_amount':float,
    'congestion_surcharge':float
}
parse_dates = ['lpep_pickup_datetime','lpep_dropoff_datetime']

date_months=["10","11","12"]

@data_loader
def load_data_from_api(*args, **kwargs):
    """
    Template for loading data from API
    """
    year ="2020"
    df_months = []

    for month in date_months:
        url_to_get = base_url + f"green_tripdata_{year}-{month}.csv.gz"
        print(f"Getting Green cab file from {year}-{month}")
        df_months.append(pd.read_csv(url_to_get, sep=',', compression="gzip",dtype=taxi_dtypes,parse_dates=parse_dates))
        
    return pd.concat(df_months)




# @test
# def test_output(output, *args) -> None:
#     """
#     Template code for testing the output of the block.
#     """
#     assert output is not None, 'The output is undefined'
