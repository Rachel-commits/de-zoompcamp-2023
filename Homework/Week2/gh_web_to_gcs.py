from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket




@task(retries=3)
def fetch(dataset_url: str) -> pd.DataFrame:
    """Read taxi data from web into pandas dataframe"""

    df = pd.read_csv(dataset_url)
    return df

@task(log_prints=True)
def clean(df = pd.DataFrame) -> pd.DataFrame:
    """Fix some datatype issues."""
    df['tpep_pickup_datetime']  = pd.to_datetime(df['tpep_pickup_datetime'])
    df['tpep_dropoff_datetime']  = pd.to_datetime(df['tpep_dropoff_datetime'])
    print(df.head(2))
    print("logging")
    print(f"rows: {len(df)}")
    return df

@task()
def write_local(df: pd.DataFrame, colour: str, dataset_file: str) -> Path:
    """Write the dataframe out as a parquet file"""
    path = Path(f"data/{colour}/{dataset_file}.parquet") 
    df.to_parquet(path, compression="gzip")
    return path

@task()
def write_gcs(path: Path) -> None:
    """Uploading to GCS"""
    gcs_block = GcsBucket.load("zoom-gcs")
    gcs_block.upload_from_path(
        from_path=path, to_path=path)
    return
_
@flow()
def etl_web_to_gcs() -> None:
    """The main etl function"""
    colour = "greem"
    year = 2020
    month = 11
    dataset_file = f"{colour}_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{colour}/{dataset_file}.csv.gz"

    df = fetch(dataset_url)
    df_clean = clean(df)
    path = write_local(df_clean, colour, dataset_file)
    write_gcs(path)

if __name__ == '__main__':
    etl_web_to_gcs()

