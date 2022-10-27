# import sys
# import pandas as pd
# import psycopg2

import sys
sys.settrace
import traceback
import pandas as pd
import boto3
import psycopg2
from psycopg2.errors import SyntaxError, InFailedSqlTransaction


def set_station_as_index(df: pd.DataFrame) -> pd.DataFrame:
    df[['LATITUDE', 'LONGITUDE', 'ELEVATION']] = df[['LATITUDE', 'LONGITUDE', 'ELEVATION']].fillna('missing')
    return df.set_index('STATION')


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


########################
# SUPPORTING FUNCTIONS #
########################
def initialize_s3_client(region_name: str) -> boto3.client:
    return boto3.client("s3", region_name=region_name)


def df_if_two_one(value):
    """Final Data Cleaning Function
    - This is run against station, latitude, longitude, and elevation for indidividual records
      - Many of these records have usable data, so don't want to just throw them out.
      - Example issues:
        - Instead of a station of '000248532' a value may contain '000248532 000248532'
          - Both are equal - function returns the first one
        - Instead of a latitude of '29.583' a value may contain '29.583 29.58333333'
          - This is from raw csv data files where they changed the number of decimal points userd
            part of the way through a year.
          - Function converts both to integers, which rounds up to the nearest whole number. If both
            whole numbers are equal, then the function returns the first value from the original pair.
    - exception handler:
      - ValueError -> str of '': Looks like some empty strings for latitude and/or longitude slipped
        through data cleaning. This handles those.

    Args:
        value (str): value to check and clean if needed

    Returns: str
    """
    try:
        split = value.split(" ")
        if len(split) > 1:
            if "." in split[0]:
                if int(float(split[0])) == int(float(split[1])):
                    return split[0]
            elif split[0] == split[1]:
                return split[0]
        return value
    except ValueError as e:
        if "could not convert string to float: ''" not in str(e):
            raise ValueError(e)
        return value


class database:
    def __init__(self, user, password, port, dbname, host):
        try:
            self.__db_connection = psycopg2.connect(
                user=user, password=password, port=port, database=dbname, host=host, sslmode="require"
            )
            self.cursor = self.__db_connection.cursor
            self.commit = self.__db_connection.commit
            self.rollback = self.__db_connection.rollback
        except psycopg2.OperationalError as e:
            if str(e).startswith("FATAL:"):
                sys.exit()
            if str(e).startswith("could not connect to server: Connection refused"):
                sys.exit()
            if str(e).startswith("could not translate host name"):
                sys.exit()
            raise psycopg2.OperationalError(e)
        except psycopg2.Error as e:
            exit()

    def __enter__(self):
        return self

    def __del__(self):
        try:
            self.__db_connection.close()
        except AttributeError as e:
            if str(e) != "'database' object has no attribute '_database__db_connection'":
                raise AttributeError(e)

    def __exit__(self, ext_type, exc_value, traceback):
        if isinstance(exc_value, Exception) or ext_type is not None:
            self.rollback()
        else:
            self.commit()
        self.__db_connection.close()

    def execute_insert(self, sql: str, params=None):
        try:
            cursor = self.cursor()
            if params:
                cursor.execute(sql, params)
                return True
            cursor.execute(sql)
            return True
        except SyntaxError as e:
            self.rollback()
            traceback.print_exc()
            sys.exit()
        except InFailedSqlTransaction as e:
            self.rollback()
            if not str(e).startswith("current transaction is aborted"):
                raise InFailedSqlTransaction(e)
            traceback.print_exc()
            sys.exit()

    def execute_query(self, sql: str, params=None):
        try:
            cursor = self.cursor()
            if params:
                cursor.execute(sql, params)
            else:
                cursor.execute(sql)
            return cursor.fetchall()
        except SyntaxError as e:
            self.rollback()
            traceback.print_exc()
            sys.exit()
        except InFailedSqlTransaction as e:
            self.rollback()
            if not str(e).startswith("current transaction is aborted"):
                raise InFailedSqlTransaction(e)
            traceback.print_exc()
            sys.exit()