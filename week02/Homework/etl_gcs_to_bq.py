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
    print(f"Pre missing passenger count {len(df.index)}")

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

@flow(log_prints=True)
def etl_etl_gcs_to_bq(colour,year, month):
    """The main etl flow to load data into big query"""


    path = extract_from_gcs(colour, year, month) 
    df = pd.read_parquet(path)
    print(f"Number of rows processesd: {len(df.index)}")
    write_bq(df)



@flow()
def etl_parent_flow(
    months: list[int] = [1, 2], year: int = 2020, colour: str = "green"
):
    for month in months:
        etl_etl_gcs_to_bq(colour,year, month)


if __name__ == "__main__":
    colour = "yellow"
    months = [2,3]
    year = 2019
    etl_parent_flow(months, year, colour)
