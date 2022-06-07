from prefect import flow
from prefect.task_runners import SequentialTaskRunner
from src.tasks import aws_get_to_calc, calc_yearly_avg


@flow(name="NOAA-1-yearly-average", task_runner=SequentialTaskRunner())
def main():
    region = "us-east-1"
    bucket = "noaa-temperature-data"

    files_l = aws_get_to_calc(bucket, region, prefix='data')
    calc_yearly_avg(bucket, region, files_l)


if __name__ == "__main__":
    main()
