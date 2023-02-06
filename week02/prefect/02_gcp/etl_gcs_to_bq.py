from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials 


@task(retries=3)
def extract_from_gcs(colour: str, year: int, month: int)-> Path:
    """Download trip data from gcs"""
    gcs_path = f"{colour}/{colour}_tripdata_{year}-{month:02}.parquet"
    gcs_block = GcsBucket.load("zoom-gcs")
    gcs_block.get_directory(from_path = gcs_path, local_path = f"./data/")
    return Path( f"./data/{gcs_path}")

@task()
def transform_data(path: Path) -> pd.DataFrame:
    """Transform the data"""
    df = pd.read_parquet(path)
    print(f"Pre missing passenger count {df['passenger_count'].isna().sum()}")
    df['passenger_count'] = df['passenger_count'].fillna(0)
    print(f"Post missing passenger count {df['passenger_count'].isna().sum()}")
    return df

@task()
def write_bq(df: pd.DataFrame) -> None:
    """Write DataFrame to BiqQuery"""

    gcp_credentials_block = GcpCredentials.load("zoom-gcp-creds")

    df.to_gbq(
        destination_table="dezoomcamp.trips",
        project_id="zoomcamp-376919",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists="append",
    )

@flow()
def etl_etl_gcs_to_bq() -> None:
    """The main etl flow to load data into big query"""
    colour = "yellow"
    year = 2021
    month = 1

    path = extract_from_gcs(colour, year, month) 
    df = transform_data(path)
    write_bq(df)


if __name__ == '__main__':
    etl_etl_gcs_to_bq()

