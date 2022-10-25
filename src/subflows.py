from io import StringIO
from pprint import pprint
from typing import Any
from prefect import flow
from prefect_sqlalchemy import DatabaseCredentials
from prefect_sqlalchemy.database import sqlalchemy_execute, sqlalchemy_query
import pandas as pd
# from tasks import find_difference


@flow()
def insert_records(new_records: bytes, year: int):
    db_creds = DatabaseCredentials.load("heroku-postgres")
    
    data = StringIO(str(new_records, 'utf-8'))
    new_df = pd.read_csv(data)
    print(new_df)
    
    # pprint(new_records)
    sql_query = """select * from climate.noaa_year_averages where year = :year"""
    with open("sql/insert.sql") as f:
        sql_insert = f.read()
    # task1: query csv from S3
    db_existing = sqlalchemy_query(sql_query, db_creds, params={"year": year})
    print(len(db_existing))
    for record in db_existing[:10]:
        print(record)
    return
    diff_l = find_difference(new_records, db_existing)
    inserted = sqlalchemy_execute(sql_insert, db_creds, params={"data_l": diff_l, "year": year})