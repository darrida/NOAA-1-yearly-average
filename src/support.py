import pandas as pd


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