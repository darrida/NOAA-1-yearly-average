from io import StringIO
from pathlib import Path
from pprint import pprint
from prefect import task, get_run_logger
import pandas as pd
from support import set_station_as_index


@task(retries=5, retry_delay_seconds=5)
def calc_yearly_avg(obj: bytes, filename) -> bytes:
    logger = get_run_logger()

    # Convert bytes string to file-like object
    data = StringIO(str(obj, 'utf-8'))

    logger.info(f'BEGIN calculating averages for {filename}')
    # Read data and prep for calculations
    df = pd.read_csv(data)
    df = set_station_as_index(df)
    # Calculate yearly averages
    avg_df = df.groupby('STATION')[['TEMP', 'DEWP', 'STP', 'MIN', 'MAX', 'PRCP']].mean()
    # Pull spatial data and rejoin with calculated averages
    spatial_df = df.groupby('STATION')[['LATITUDE', 'LONGITUDE', 'ELEVATION', 'SOURCE_FILE']].first()
    num_avg, num_spatial = len(avg_df), len(spatial_df)
    df = spatial_df.join(avg_df)
    if num_avg != len(df) or num_spatial != len(df):  # Confirm counts match final dataframe
        raise ValueError('Number of grouped averages doesn\'t match number of grouped spatial records')
    # Export dataframe to in-memory file stream
    textStream = StringIO()
    df.to_csv(textStream)

    filename = Path(filename).name
    logger.info(f"COMPLETED calculating averages for {filename}")

    # Convert return value back into bytes
    return bytes(textStream.getvalue(), 'utf-8')


# @task(retries=5, retry_delay_seconds=5)
# def database_insert(bucket_name, region_name, files_l: list):
#     sqlalchemy_credentials = DatabaseCredentials(
#         driver=AsyncDriver.POSTGRESQL_ASYNCPG,
#         username="prefect",
#         password="prefect_password",
#         database="postgres",
#     )
#     sqlalchemy_execute(
#         "abligatory insert statement",
#         sqlalchemy_credentials,
#     )