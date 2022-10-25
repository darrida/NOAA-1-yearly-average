from io import StringIO
from pathlib import Path
from pprint import pprint
from typing import Tuple
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
    df.to_csv("temp_records.csv")

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


# @task(retries=5, retry_delay_seconds=5)
# def insert_records(filename, db_name: str, user: str, host: str, port: str, bucket_name, region_name):
#     ic(filename)
#     year = filename.strip("year_average/avg_")
#     year = year.strip(".csv")
#     # Retrieve file data from AWS S3
#     s3_client = initialize_s3_client(region_name)
#     obj = s3_client.get_object(Bucket=bucket_name, Key=filename)
#     data = obj["Body"]
#     csv_df = pd.read_csv(data)
#     csv_df["SITE_NUMBER"] = csv_df["SITE_NUMBER"].str.strip("]")
#     csv_df["SITE_NUMBER"] = csv_df["SITE_NUMBER"].str.strip("[")
#     csv_df["LATITUDE"] = csv_df["LATITUDE"].str.strip("]")
#     csv_df["LATITUDE"] = csv_df["LATITUDE"].str.strip("[")
#     csv_df["LONGITUDE"] = csv_df["LONGITUDE"].str.strip("]")
#     csv_df["LONGITUDE"] = csv_df["LONGITUDE"].str.strip("[")
#     csv_df["ELEVATION"] = csv_df["ELEVATION"].str.strip("]")
#     csv_df["ELEVATION"] = csv_df["ELEVATION"].str.strip("[")

#     conn_info = {
#         "user": user,
#         "password": PrefectSecret("HEROKU_DB_PW").run(),
#         "host": host,
#         "dbname": db_name,
#         "port": port,
#     }

#     time.sleep(10)
#     with database(**conn_info) as conn:
#         commit_count = 0
#         for i in tqdm(csv_df.index):
#             vals = [csv_df.at[i, col] for col in list(csv_df.columns)]
#             station = vals[0]
#             # df_if_two_one cleans a few issues left over from the data cleaning and calc steps
#             station = df_if_two_one(station)
#             latitude = vals[1]
#             latitude = df_if_two_one(latitude)
#             longitude = vals[2]
#             longitude = df_if_two_one(longitude)
#             if latitude not in ("nan", "") and longitude not in ("nan", ""):
#                 try:
#                     cursor = conn.cursor()
#                     # val = cursor.callproc('ST_GeomFromText', ((f'POINT({longitude} {latitude})'), 4326))
#                     cursor.callproc("ST_GeomFromText", ((f"POINT({longitude} {latitude})"), 4326))
#                     geom = cursor.fetchone()[0]
#                     insert_str = """
#                         insert into climate.noaa_year_averages 
#                             (year, station, latitude, longitude, elevation, temp, dewp, stp, max, min, prcp, geom)
#                         values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
#                     """
#                     conn.execute_insert(
#                         insert_str,
#                         (
#                             year,
#                             vals[0],
#                             vals[1],
#                             vals[2],
#                             vals[3],
#                             vals[4],
#                             vals[5],
#                             vals[6],
#                             vals[7],
#                             vals[8],
#                             vals[9],
#                             geom,
#                         ),
#                     )
#                     commit_count += 1
#                 except UniqueViolation as e:
#                     # Record already exists
#                     pass
#                 except InFailedSqlTransaction as e:
#                     # Record exists, so transaction with "geom" is removed
#                     pass
#                 except Exception as e:
#                     if "parse error - invalid geometry" in str(e):
#                         # Error in spatial data
#                         ic(latitude, longitude)
#                     print(e)
#                     ic(vals[0], year)
#                     raise Exception(e)
#                 if commit_count >= 100:
#                     conn.commit()
#                     commit_count = 0
#     try:
#         PostgresExecute(db_name=db_name, user=user, host=host, port=port,).run(
#             query="""
#             insert into climate.csv_checker 
#                 (year, date_create, date_update)
#             values (%s, CURRENT_DATE, CURRENT_DATE)
#             """,
#             data=(year,),
#             commit=True,
#             password=PrefectSecret("HEROKU_DB_PW").run(),
#         )
#     except UniqueViolation:
#         pass
#     except TypeError as e:
#         ic(vals[0], year)
#         ic(e)
#     return
