# from prefect import flow
# from prefect_sqlalchemy import DatabaseCredentials

# from tasks import insert_records1, prep_records


# @flow()
# def insert_records(new_records: bytes, year: int):
#     db_creds = DatabaseCredentials.load("heroku-postgres")
    
#     # data = StringIO(str(new_records, 'utf-8'))
#     # new_df = pd.read_csv(data)
#     # # print(new_df)
#     # new_d = new_df.to_dict("records")
#     # pprint(new_d[:5])
    
#     # TODO: (1) Figure out how to transform new data to match existing data queried from db
#     # TODO: (2) See if I can setup a set difference
#     # TODO: (3) Insert/Update differences in database
#     # - See if it's possible to do "insert many" with prefect-sqlalchemy

#     prepped_l = prep_records(new_records)
#     inserted = insert_records1(prepped_l, year, db_creds)