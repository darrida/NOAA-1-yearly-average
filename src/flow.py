from prefect import flow, get_run_logger
from prefect.task_runners import SequentialTaskRunner
from prefect.filesystems import S3
from prefect_aws import AwsCredentials
from prefect_aws.s3 import s3_list_objects, s3_download, s3_upload
from src.tasks import calc_yearly_avg


s3_block = S3.load("noaa-data")


@flow(name="NOAA-1-yearly-average", task_runner=SequentialTaskRunner())
def main():
    logger = get_run_logger()
    
    region = "us-east-1"
    bucket = "noaa-temperature-data"
    aws_credentials = AwsCredentials(aws_access_key_id=s3_block.aws_access_key_id.get_secret_value(), 
                                     aws_secret_access_key=s3_block.aws_secret_access_key.get_secret_value())

    s3_objects = s3_list_objects(bucket=bucket, prefix="data", aws_credentials=aws_credentials)
    files_l = [x["Key"] for x in s3_objects]
    files_l = [x for x in files_l if '_full.csv' in x]

    for filename in files_l:
        print(filename)
        year_obj = s3_download(bucket=bucket, key=filename, aws_credentials=aws_credentials)
        avg_obj = calc_yearly_avg(year_obj, filename)
        key = s3_upload(bucket=bucket, key=f"year_average/{filename.split('/')[1][:4]}_averages.csv", # example "filename": "data/YYYY_full.csv"
                        data=avg_obj, aws_credentials=aws_credentials)
        logger.info(f"Stored as '{key}'")


if __name__ == "__main__":
    main()
