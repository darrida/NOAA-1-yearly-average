from prefect import flow
from prefect_sqlalchemy import DatabaseCredentials
from prefect_sqlalchemy.database import sqlalchemy_execute, sqlalchemy_query
from tasks import find_difference


@flow()
def insert_records(new_records, sql_insert: str, db_creds: DatabaseCredentials, year: int):
    sql_query = """select * noaa_year_averages where year = :year"""
    with open("sql/insert.sql") as f:
        sql_insert = f.read()
    # task1: query csv from S3
    db_existing = sqlalchemy_query(sql_query, db_creds, params={"year": year})
    diff_l = find_difference(new_records, db_existing)
    inserted = sqlalchemy_execute(sql_insert, db_creds, params={"data_l": diff_l, "year": year})