import boto3
import pandas as pd


def initialize_s3_client(region_name: str) -> boto3.client:
    return boto3.client("s3", region_name=region_name)


def aws_all_data_files(s3_client, bucket, prefix):
    paginator = s3_client.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=bucket, Prefix=prefix)
    files_l = []
    for page in pages:
        list_all_keys = page["Contents"]
        # item arrives in format of 'year/filename'; this removes 'year/'
        temp_l = [x["Key"].split("/")[1] for x in list_all_keys]
        files_l += temp_l
    return files_l


# @task()
def set_station_as_index(df: pd.DataFrame) -> pd.DataFrame:
    df[['LATITUDE', 'LONGITUDE', 'ELEVATION']] = df[['LATITUDE', 'LONGITUDE', 'ELEVATION']].fillna('missing')
    return df.set_index('STATION')


# @task()
def remove_missing_spatial(df: pd.DataFrame) -> pd.DataFrame:
    """
    Removes Records with Missing Spatial Data
    - If 'NaN' exists, replaces with 'missing'
    - Returns dataframe with all 'missing' spatial elements removed
    - Why: This is 100x (guestimate) faster than saving a separate csv with this information removed. Quicker to just
           run this function when a dataframe with 100% clean spatial data is required
    """
    df[['LATITUDE', 'LONGITUDE', 'ELEVATION']] = df[['LATITUDE', 'LONGITUDE', 'ELEVATION']].fillna('missing')
    return df[(df['LATITUDE'] != 'missing') & (df['LONGITUDE'] != 'missing') & (df['ELEVATION'] != 'missing')]