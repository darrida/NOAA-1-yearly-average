from io import StringIO
from pathlib import Path
from prefect import task, get_run_logger
import pandas as pd
from src.support import initialize_s3_client, aws_all_data_files, set_station_as_index, remove_missing_spatial


@task(retries=5, retry_delay_seconds=5)
def aws_get_to_calc(bucket, region, prefix):
    s3_client = initialize_s3_client(region)
    files_l = aws_all_data_files(s3_client, bucket, prefix=prefix)
    return [f"{prefix}/{x}" for x in files_l if '_full.csv' in x]


@task(retries=5, retry_delay_seconds=5)
def calc_yearly_avg(bucket_name, region_name, files_l: list, calc_all: bool=False):
    logger = get_run_logger()
    s3_client = initialize_s3_client(region_name)

    for f in files_l:
        logger.info(f'BEGIN calculating averages for {f}')
        obj = s3_client.get_object(Bucket=bucket_name, Key=f)
        data = obj['Body']
        # Read data and prep for calculations
        df = pd.read_csv(data)
        df = set_station_as_index(df)
        # df = remove_missing_spatial(df)
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
        # Upload results back to AWS s3
        filename = Path(f).name
        s3_client.put_object(Body=textStream.getvalue(), Bucket=bucket_name, Key=f"year_average/{filename[:4]}_averages.csv")
        logger.info(f'COMPLETED calculating averages for {f}; stored as "year_average/{filename[:4]}_averages.csv"')