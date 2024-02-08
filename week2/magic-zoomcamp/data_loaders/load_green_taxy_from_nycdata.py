import io
import pandas as pd
import pyarrow.parquet as pq

import requests
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


base_url = 'https://d37ci6vzurychx.cloudfront.net/trip-data/'
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


@data_loader
def load_data_from_api(*args, **kwargs):
    """
    Template for loading data from API
    """
    date_to_get = kwargs['execution_date']
    year= date_to_get.strftime("%Y")
    month= date_to_get.strftime("%m")
    # year= "2022"
    # month= "01"
    

    url_to_get = base_url + f"green_tripdata_{year}-{month}.parquet"

    print(f"Getting Green cab file from {year}-{month}")
    return pd.read_parquet(url_to_get)    
    

        





# @test
# def test_output(output, *args) -> None:
#     """
#     Template code for testing the output of the block.
#     """
#     assert output is not None, 'The output is undefined'
