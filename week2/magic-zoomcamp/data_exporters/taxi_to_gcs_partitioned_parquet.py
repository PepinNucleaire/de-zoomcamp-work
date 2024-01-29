import pyarrow as pa
import pyarrow.parquet as pq
import os

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/home/src/personal-gcp.json/eighth-veld-411323-8a0d542a0598.json'

# config_path = path.join(get_repo_path(), 'io_config.yaml')
# config_profile = 'dev'

bucket_name = 'eighth-veld-411323-bucket'
project_id = 'eighth-veld-411323-8a0d542a0598'

table_name= "nyc_taxi_data"

root_path = f'{bucket_name}/{table_name}'

@data_exporter
def export_data(data, *args, **kwargs):
    data['tpep_pickup_date'] = data['tpep_pickup_datetime'].dt.date

    table = pa.Table.from_pandas(data)

    gcs = pa.fs.GcsFileSystem()

    pq.write_to_dataset(
        table,
        root_path=root_path,
        partition_cols=["tpep_pickup_date"],
        filesystem=gcs
    )

