from pathlib import Path
import pandas as pd
from random import randint
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect.filesystems import GitHub



@task(retries=3)
def fetch(dataset_url: str) -> pd.DataFrame:
    """Read taxi data from web into pandas DataFrame"""
    # if randint(0, 1) > 0:
    #     raise Exception

    df = pd.read_csv(dataset_url)
    print(f"heading of csv is {df.head(2)}")
    print(f"columns: {df.dtypes}")
    return df


@task(log_prints=True)
def clean(df: pd.DataFrame, color:str) -> pd.DataFrame:
    """Fix dtype issues"""
    if color.startswith('yellow'): 
        df["tpep_pickup_datetime"] = pd.to_datetime(df["tpep_pickup_datetime"])
        df["tpep_dropoff_datetime"] = pd.to_datetime(df["tpep_dropoff_datetime"])
    else:
        df["lpep_pickup_datetime"] = pd.to_datetime(df["lpep_pickup_datetime"])
        df["lpep_dropoff_datetime"] = pd.to_datetime(df["lpep_dropoff_datetime"])
    print(df.head(2))
    print(f"\ncolumns: {df.dtypes}")
    print(f"\nrows: {len(df)}")
    return df


@task()
def write_local(df: pd.DataFrame, color: str, dataset_file: str) -> Path:
    """Write DataFrame out locally as parquet file"""
    path = Path(f"data/{color}/{dataset_file}.parquet")
    df.to_parquet(path, compression="gzip")
    return path


@task()
def write_github(path: Path) -> None:
    """Upload local parquet file to github"""
    # gcs_block = GcsBucket.load("zoom-gcs")
    # gcs_block.upload_from_path(from_path=path, to_path=path)
    github_block = GitHub.load("de-zoomcamp")
    # block = GitHub(
    #     repository="https://github.com/my-repo/",
    #     # access_token=<my_access_token> # only required for private repos
    # )
    github_block.get_directory("") # specify a subfolder of repo
    # block.save("dev")
    return


@flow()
def etl_web_to_gcs() -> None:
    """The main ETL function"""
    color = "green"
    year = 2020
    month = 11
    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"

    df = fetch(dataset_url)
    df_clean = clean(df, color)
    path = write_local(df_clean, color, dataset_file)
    write_github(path)


if __name__ == "__main__":
    etl_web_to_gcs()