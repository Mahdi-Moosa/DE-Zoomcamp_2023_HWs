import os
import pandas as pd
from pathlib import Path
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket

dtype_dict = {'dispatching_base_num': str,
 'pickup_datetime': str,
 'dropOff_datetime': str,
 'PUlocationID': 'float64',
 'DOlocationID': 'float64',
 'DOLocationID': 'float64',
 'SR_Flag': 'float64',
 'Affiliated_base_number': str,
 'VendorID': 'float64',
 'tpep_pickup_datetime': str,
 'tpep_dropoff_datetime': str,
 'passenger_count': 'float64',
 'trip_distance': 'float64',
 'RatecodeID': 'float64',
 'store_and_fwd_flag': str,
 'PULocationID': 'float64',
 'DOLocationID': 'float64',
 'payment_type': 'float64',
 'fare_amount': 'float64',
 'extra': 'float64',
 'mta_tax': 'float64',
 'tip_amount': 'float64',
 'tolls_amount': 'float64',
 'improvement_surcharge': 'float64',
 'total_amount': 'float64',
 'congestion_surcharge': 'float64'
 }

# TODO: Should convert all column headers to lower-case. df.columns.str.lower()

@task(retries=3, log_prints=True)
def fetch(dataset_url: str)-> pd.DataFrame:
    df = pd.read_csv(dataset_url, dtype= dtype_dict, encoding='unicode_escape', low_memory=False)
    if 'DOLocationID' in df.columns:
        df.rename({'DOLocationID' : 'DOlocationID'}, axis=1, inplace=True)
    print(f'Dataframe loaded for: {dataset_url}')
    return df

@task()
def write_local(df: pd.DataFrame, taxi_data_tag: str, dataset_file: str) -> Path:
    """Write DataFrame out locally as parquet file"""
    import os
    folder_path = f"data/{taxi_data_tag}"
    if not os.path.exists(folder_path):
        print(f"Destination path : {folder_path} does not exist. Creating folder.")
        os.makedirs(folder_path)
        print(f'Folder (with sub-folder) created: {folder_path}')

    path = Path(f"{folder_path}/{dataset_file}_snappy.parquet")
    df.to_parquet(path, compression="snappy")
    return path

@task(log_prints=True)
def write_gcs(path: str) -> None:
    """Upload local parquet file to GCS"""
    gcs_block = GcsBucket.load("de-zoomcamp-gcs")
    gcs_block.upload_from_path(from_path=path, to_path=path)
    return

@flow(log_prints=True)
def fetch_from_web_to_gcs(taxi_data_tag : str, year: int, month: int, save_dir : str = 'data/') -> None:
    """The main ETL function"""
    dataset_file = f"{taxi_data_tag}_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{taxi_data_tag}/{dataset_file}.csv.gz"
    # save_dir_with_subfolder =f'{save_dir}{taxi_data_tag}/'
    df = fetch(dataset_url=dataset_url)
    path = write_local(df=df, taxi_data_tag=taxi_data_tag, dataset_file=dataset_file) # Path(f'{save_dir_with_subfolder}{dataset_file}.csv.gz')
    write_gcs(path = path)
    print(f"GCS write successful for {dataset_file} snappy.")

@flow(log_prints=True, name = 'snappy_compression_test')
def parent_fetch_flow(months: list[int], year: int, taxi_data_tag : str):
    for month in months:
        fetch_from_web_to_gcs(taxi_data_tag=taxi_data_tag, month=month, year=year, save_dir = 'data/')
        print(f'Finished data EL for month number: {month}')

if __name__ == "__main__":
    months = list(range(8,13,1))
    year = 2019
    taxi_data_tag = 'yellow'
    parent_fetch_flow(months=months, year= year, taxi_data_tag= taxi_data_tag)