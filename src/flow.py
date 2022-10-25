import asyncio
from pprint import pprint
from prefect import flow, get_run_logger
from prefect.task_runners import SequentialTaskRunner
from prefect_aws import AwsCredentials
from prefect_aws.s3 import s3_list_objects, s3_download, s3_upload
from prefect_sqlalchemy import DatabaseCredentials
from subflows import insert_records
from tasks import calc_yearly_avg


@flow(name="NOAA-1-yearly-average", task_runner=SequentialTaskRunner())
def main():
    logger = get_run_logger()
    
    bucket = "noaa-temperature-data"
    aws_creds = AwsCredentials.load("aws-creds")

    s3_objects = s3_list_objects(bucket=bucket, prefix="data", aws_credentials=aws_creds)
    files_l = [x["Key"] for x in s3_objects]
    files_l = [x for x in files_l if x.endswith('_full.csv')]

    for filename in files_l[:1]:
        # if '1980' not in filename:
        #     continue
        print(filename)
        year_obj = s3_download(bucket=bucket, key=filename, aws_credentials=aws_creds)
        avg_obj = calc_yearly_avg(year_obj, filename)
        key = s3_upload(bucket=bucket, key=f"year_average/{filename.split('/')[1][:4]}_averages.csv", # example "filename": "data/YYYY_full.csv"
                        data=avg_obj, aws_credentials=aws_creds)
        logger.info(f"Stored as '{key}'")
        logger.info(f"Start Database Insert Process for {key}")
        # insert_records(dataframe, "1995")
        insert_records(avg_obj, "1995")


if __name__ == "__main__":
    main()
