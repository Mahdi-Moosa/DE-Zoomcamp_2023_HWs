import os
from pathlib import Path
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket

@task(retries=3, log_prints=True)
def fetch(dataset_url: str, save_dir : str = 'data/' )-> None:
    os.system(f'wget {dataset_url} -P {save_dir}')
    print(f'Download domplete for: {dataset_url}')
    return

@task(log_prints=True)
def write_gcs(path: str) -> None:
    """Upload local parquet file to GCS"""
    gcs_block = GcsBucket.load("de-zoomcamp-gcs")
    gcs_block.upload_from_path(from_path=path, to_path=path)
    return

@flow()
def fetch_from_web_to_gcs(taxi_data_tag : str, year: int, month: int, save_dir : str = 'data/') -> None:
    """The main ETL function"""
    dataset_file = f"{taxi_data_tag}_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{taxi_data_tag}/{dataset_file}.csv.gz"
    save_dir_with_subfolder =f'{save_dir}{taxi_data_tag}/'
    fetch(dataset_url=dataset_url, save_dir = save_dir_with_subfolder)
    path = Path(f'{save_dir_with_subfolder}{dataset_file}.csv.gz')
    write_gcs(path = path)

@flow(log_prints=True)
def parent_fetch_flow(months: list[int] = list(range(1,13,1)), year: int = 2019, taxi_data_tag : str = 'fhv'):
    for month in months:
        fetch_from_web_to_gcs(taxi_data_tag=taxi_data_tag, month=month, year=year, save_dir = 'data/')

if __name__ == "__main__":
    parent_fetch_flow()