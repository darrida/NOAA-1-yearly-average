from datetime import timedelta
from io import StringIO
from pathlib import Path
from typing import List

import pandas as pd
from prefect import get_run_logger, task
from prefect.tasks import task_input_hash
from prefect_sqlalchemy import DatabaseCredentials
from psycopg2.errors import UniqueViolation
from support import database, not_missing_check, set_station_as_index
from tqdm import tqdm


@task(retries=5, retry_delay_seconds=5,
      cache_key_fn=task_input_hash, 
      cache_expiration=timedelta(minutes=300))
def calc_yearly_avg(obj: bytes, filename) -> bytes:
    logger = get_run_logger()

    # Convert bytes string to file-like object
    data = StringIO(str(obj, 'utf-8'))

    logger.info(f'BEGIN calculating averages for {filename}')
    # Read data and prep for calculations
    df = pd.read_csv(data)
    logger.info(f"Number of records: {len(df)}")
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
    logger.info(f"Number of records: {len(df)}")
    textStream = StringIO()
    df.to_csv(textStream)
    df.to_csv("temp_records.csv")

    filename = Path(filename).name
    logger.info(f"COMPLETED calculating averages for {filename}")

    # Convert return value back into bytes
    return bytes(textStream.getvalue(), 'utf-8')


@task(cache_key_fn=task_input_hash, 
      cache_expiration=timedelta(minutes=300))
def prep_records(data, db_creds: DatabaseCredentials) -> List[list]:
    logger = get_run_logger()

    data = StringIO(str(data, 'utf-8'))    
    csv_df = pd.read_csv(data)

    prepped_l = []
    for i in tqdm(csv_df.index):
        vals = [csv_df.at[i, col] for col in list(csv_df.columns)]
        latitude, longitude, elevation = vals[1], vals[2], vals[3]
        if (
            latitude in ("nan", "", "missing") 
            or longitude in ("nan", "", "missing") 
            or elevation in ("nan", "", "missing")
        ):
            logger.info(f"Missing spatial data from station {vals[0]} | {vals[4]}")
            continue
        prepped_l.append(vals)
    
    return prepped_l


@task()
def delete_csv_checker(year, db_creds: DatabaseCredentials):
    
    conn_info = {
        "user": db_creds.username,
        "password": db_creds.password.get_secret_value(),
        "host": db_creds.host,
        "dbname": db_creds.database,
        "port": db_creds.port,
    }
    
    with database(**conn_info) as conn:
        conn.execute_insert(
            """
            delete from climate.csv_checker
            where year = %s
            """,
            params=(year,)
        )


@task()
def insert_records(data: List[list], year: str, db_creds: DatabaseCredentials):
    logger = get_run_logger()
    
    conn_info = {
        "user": db_creds.username,
        "password": db_creds.password.get_secret_value(),
        "host": db_creds.host,
        "dbname": db_creds.database,
        "port": db_creds.port,
    }

    # time.sleep(5)
    with database(**conn_info) as conn:
        year_4_digits = year #[1][:4]
        print(year_4_digits)
        for vals in tqdm(data, desc=f"Inserting {year}"):
            record_d = {
                "year": year_4_digits,
                "station": str(vals[0]),
                "latitude": not_missing_check(vals[1]),
                "longitude": not_missing_check(vals[2]),
                "elevation": not_missing_check(vals[3]),
                # "source_file": vals[4],
                "temp": float(vals[5]),
                "dewp": float(vals[6]),
                "stp": float(vals[7]),
                "max": float(vals[8]),
                "min": float(vals[9]),
                "prcp": float(vals[10]),
                "geom": f"POINT({vals[2]} {vals[1]})"
            }

            try:
                conn.execute_insert(
                    """
                    delete from climate.noaa_year_averages
                    where year = %s
                    and station = %s
                    """, 
                    (record_d["year"], record_d["station"])
                )
                insert_str = """
                    insert into climate.noaa_year_averages 
                        (year, station, latitude, longitude, elevation, temp, dewp, 
                        stp, max, min, prcp, geom)
                    values (%(year)s, %(station)s, %(latitude)s, %(longitude)s, %(elevation)s, %(temp)s, %(dewp)s, 
                            %(stp)s, %(max)s, %(min)s, %(prcp)s, ST_GeomFromText(%(geom)s, 4326))
                """
                conn.execute_insert(insert_str, record_d)
            except UniqueViolation as e:
                # Record already exists
                logger.info(f"Record for {year}-{record_d['station']} already exists | executing UPDATE")
                update_str = """
                    update climate.noaa_year_averages
                    set (latitude, longitude, elevation, temp, dewp, 
                         stp, max, min, prcp, geom)
                      = (%(latitude)s, %(longitude)s, %(elevation)s, %(temp)s, %(dewp)s, 
                         %(stp)s, %(max)s, %(min)s, %(prcp)s, ST_GeomFromText(%(geom)s, 4326))
                    where year = %(year)s
                    and station = %(station)s
                """
                conn.execute_insert(update_str, record_d)
        conn.commit()
    return


@task()
def update_csv_checker(year: str, s3_date, db_creds: DatabaseCredentials):
    logger = get_run_logger()
    logger.info('CALLED')
    
    conn_info = {
        "user": db_creds.username,
        "password": db_creds.password.get_secret_value(),
        "host": db_creds.host,
        "dbname": db_creds.database,
        "port": db_creds.port,
    }

    with database(**conn_info) as conn:
        try:
            conn.execute_insert(
                """
                insert into climate.csv_checker 
                    (year, date_create, date_update)
                values (%s, %s, %s)
                """,
                params=(year, s3_date, s3_date)
            )
            conn.commit()
        except UniqueViolation:
            pass
        except TypeError as e:
            logger.error(year)
            logger.error(e)
