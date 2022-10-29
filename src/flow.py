from pprint import pprint
from datetime import datetime, timedelta
from prefect.tasks import task_input_hash
from prefect import flow, get_run_logger
from prefect.task_runners import SequentialTaskRunner
from prefect_aws import AwsCredentials
from prefect_aws.s3 import s3_list_objects, s3_download, s3_upload
from prefect_sqlalchemy import DatabaseCredentials
from prefect_sqlalchemy.database import sqlalchemy_query
from tasks import calc_yearly_avg, insert_records, prep_records


@flow(name="NOAA-1-yearly-average", task_runner=SequentialTaskRunner())
def main():
    logger = get_run_logger()
    
    bucket = "noaa-temperature-data"
    aws_creds = AwsCredentials.load("aws-creds")
    db_creds = DatabaseCredentials.load("heroku-postgres")

    # FIND DIFFERENCE BETWEEN S3 AND DB (if S3 date > db date, update year records)
    s3_objects = s3_list_objects(bucket=bucket, prefix="data", aws_credentials=aws_creds)
    files_l = [x["Key"] for x in s3_objects]
    # pprint(files_l)

    # pull completed files (includes NOAA csv date)
    # - transform to match queried db format
    last_updated_s3_l = [x["Key"] for x in s3_objects if x["Key"].endswith('___complete')]
    last_updated_s3_l = [
       (x.split("/")[1][:4], datetime.strptime(x.split("_")[2], '%Y%m%d').date()) 
        for x in last_updated_s3_l
    ]
    # pprint(last_updated_s3_l)

    # Query year update dates from db
    # - transform into dictionary with year for keys (quick searching)
    last_updated_db_l = sqlalchemy_query(
        """
        select year, date_update from climate.csv_checker
        order by year
        """,
        sqlalchemy_credentials=db_creds,
    )
    # pprint(last_updated_db_l)
    last_updated_db_d = {}
    for obj in last_updated_db_l:
        year, date_ = obj[0], obj[1]
        last_updated_db_d[year] = date_
    
    # Identify years for data upload/insert/update
    update_l = []
    for i in last_updated_s3_l:
        year = i[0]
        s3_date = i[1]
        try:
            db_date = last_updated_db_d[year]
        except KeyError:
            # If year not found in db, add year to update/insert list
            update_l.append(year)
            continue
        if s3_date > db_date:
            # If s3 date is more than db date, add year to update/insert list
            update_l.append(year)
    pprint(update_l)
    # exit()
    
    # get list of files with data to insert
    csv_l = [x for x in files_l if x.endswith('_full.csv') and x.split("/")[1][:4] in update_l]
    print(csv_l)
    # exit()

    # TODO: Decide if there is a need to replace the current/old records
    #       (if they exist to be replaced)
    for filename in csv_l:
        # if '1980' not in filename:
        #     continue
        print(filename)
        year_obj = s3_download(bucket=bucket, key=filename, aws_credentials=aws_creds)
        avg_obj = calc_yearly_avg(year_obj, filename)
        key = s3_upload(bucket=bucket, key=f"year_average/{filename.split('/')[1][:4]}_averages.csv", # example "filename": "data/YYYY_full.csv"
                        data=avg_obj, aws_credentials=aws_creds)
        logger.info(f"Stored as '{key}'")
        # continue
        logger.info(f"Start Database Insert Process for {key}")
        # insert_records(dataframe, "1995")
        # insert_records(avg_obj, "1995")
        prepped_l = prep_records(avg_obj, db_creds)
        pprint(prepped_l)
        # return
        year = filename.split("/")[:4]
        inserted = insert_records(prepped_l, year, db_creds)


if __name__ == "__main__":
    main()
