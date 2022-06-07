##############################################################################
# Author: Ben Hammond
#
# REQUIREMENTS
# - Pip: use requirements.txt
# - Poetry: use pyproject.toml
#
# DESCRIPTION
#
##############################################################################
from sys import prefix
from prefect import flow, get_run_logger
from prefect.task_runners import SequentialTaskRunner
from pathlib import Path
from src.tasks import aws_get_to_calc, calc_yearly_avg
# from src.tasks.download import query_cloud_archives, query_local_archives, archives_difference, download_and_merge, save_to_file
# from src.tasks.clean import set_station_as_index, find_missing_lat_long, find_missing_elevation, confirm_consistent_spatial


@flow(name="NOAA-1-yearly-average", task_runner=SequentialTaskRunner())
def main():
    logger = get_run_logger()
    data_dir = str(Path("./local_data/global-summary-of-the-day-archive"))
    region = "us-east-1"
    bucket = "noaa-temperature-data"

    files_l = aws_get_to_calc(bucket, region, prefix='data')
    calc_yearly_avg(bucket, region, files_l)
    # exit()
    # for file_ in diff_l.wait().result():
    #     year = Path(file_[1]).name[:4]
    #     tarfile_name = file_[0]
        
    #     # DOWNLOAD DATA FILE
    #     logger.info(f'Calculate Yearly Average for {year}')
    #     s4_download_df = download_and_merge(file_, base_url, data_dir)
        

    #     # SAVE DATA
    #     s6_save = save_to_file(final_data_df.result(), year, data_dir, tarfile_name)
    #     logger.info(f'Completed Yearly Average for {year}')


if __name__ == "__main__":
    main()
