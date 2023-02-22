from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials


@task(retries=3)
def extract_from_gcs(color: str, year: int, month: int) -> Path:
    """Download trip data from GCS"""
    gcs_path = f"data/{color}/{color}_tripdata_{year}-{month:02}_snappy.parquet"
    gcs_block = GcsBucket.load("de-zoomcamp-gcs")
    gcs_block.get_directory(from_path=gcs_path, local_path=f"../data/")
    return Path(f"../data/{gcs_path}")


@task()
def load_data(path: Path) -> pd.DataFrame:
    """Data cleaning example"""
    df = pd.read_parquet(path)
    # print(f"pre: missing passenger count: {df['passenger_count'].isna().sum()}")
    # df["passenger_count"].fillna(0, inplace=True)
    # print(f"post: missing passenger count: {df['passenger_count'].isna().sum()}")
    return df


@task()
def write_bq(df: pd.DataFrame, t_name : str) -> None:
    """Write DataFrame to BiqQuery"""

    gcp_credentials_block = GcpCredentials.load("mmm-gcs")

    df.to_gbq(
        destination_table=f"dezoomcamp_ny_taxi.{t_name}",
        project_id="de-zoomcamp-mmm",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists="append",
    )


@flow()
def el_gcs_to_bq(color : str, year : int, month: int):
    """Main ETL flow to load data into Big Query"""
    # color = "yellow"
    # year = 2021
    # month = 1

    path = extract_from_gcs(color, year, month)
    df = load_data(path)
    write_bq(df, t_name = color + '_snappy')

@flow()
def parent_el_gcs_to_bq(color : str = 'green', year : int = 2020, months: list[int] = [11]):
    for month in months:
        el_gcs_to_bq(color=color, year=year, month=month)

if __name__ == "__main__":
    color = "yellow"
    year = 2020
    months = list(range(1,13,1))  #[2,3]
    parent_el_gcs_to_bq(color=color, year=year, months=months)
