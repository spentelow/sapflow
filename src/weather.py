"""
Create data tables storing weather data for the closest/chosen NOAA weather 
station to each sap data collection site for each year that measurements were 
collected.

author: Steffen Pentelow
date: 2021-03-25

Usage: src/weather.py
"""

import os
from ftplib import FTP
import gzip
import io
import pandas as pd
import numpy as np


def main():

    processed_path = os.path.join("data","processed","stinson2019","norm_tables")
    raw_path = os.path.join("data","raw")
    raw_path = "data/raw/stinson2019"

    if not os.path.exists(processed_path):
        os.makedirs(processed_path)
    
    record_range = create_stn_year_range(processed_path)
    weather = get_weather_data(record_range)

    weather.to_pickle(os.path.join(processed_path, 'weather'))

    return


def create_stn_year_range(processed_path):
    """Create table containing the range of years for which measurements were 
    collected at the sap data collection site associated with each weather
    station.

    Returns
    -------
    pandas.DataFrame
        Data frame with site, weather station id, and first and last years of 
        recorded sap data at that site.

     Examples
    --------
    >>> create_stn_year_range(os.path.join("data","processed","stinson2019","norm_tables"))
    """
    # Load required data tables
    weather_stn = pd.read_pickle(os.path.join(processed_path,'weather_stn'))
    closest_weather_stn = pd.read_pickle(os.path.join(processed_path,'closest_weather_stn'))
    tree_site = pd.read_pickle(os.path.join(processed_path,'tree_site'))
    tap_tree = pd.read_pickle(os.path.join(processed_path,'tap_tree'))
    tap_records = pd.read_pickle(os.path.join(processed_path,'tap_records'))
    dates = pd.read_pickle(os.path.join(processed_path,'dates'))

    # Create dataframe with all required info to connect weather station id to the number of years of sap measurements
    df = (
        weather_stn.merge(closest_weather_stn.reset_index(), on="stn_id", how="right")
        .merge(tree_site.reset_index(), on="site", how="right")
        .merge(tap_tree.reset_index(), on="tree", how="right")
        .merge(tap_records.reset_index(), on="tap_id", how="right")
        .merge(dates.reset_index(), on="record_id", how="right")
    )

    # Create an populate table relating weather station ID with associated years of sap measurements
    record_range = weather_stn.merge(closest_weather_stn.reset_index(), on="stn_id", how="right").set_index('site')
    record_range['first_year'] = int()
    record_range['last_year'] = int()

    for site in closest_weather_stn.index:
        first_year = df[df.site==site].date.min().year
        last_year = df[df.site==site].date.max().year
        
        if first_year < record_range.loc[site,'start'].year:
            print(f"Earliest measurements at site {site} occured before weather station ID {record_range.loc[site,'stn_id']} began collecting data.")
            break
        else:
            record_range.loc[site,'first_year'] = first_year
            
        if first_year < record_range.loc[site,'start'].year:
            print(f"Latest measurements at site {site} occured after weather station ID {record_range.loc[site,'stn_id']} stopped collecting data.")
            break
        else:
            record_range.loc[site,'last_year'] = last_year

    return record_range.loc[:, ['stn_id', 'first_year', 'last_year']]


def get_weather_data(record_range):
    """Create weather data frame containing air temperature measurements
    for the NOAA weather stations closest to/chosen as representative of each
    sap data collection site for each year that measurements were taken
    
    Parameters
    ----------
    record_range : pandas.DataFrame
        Data frame with site, weather station id, and first and last years of 
        recorded sap data at that site.

    Returns
    -------
    pandas.DataFrame
        Data frame with air temperature measurements from each selected NOAA
        weather station for each required year.
    """

    weather = pd.DataFrame(columns = ["stn_id", "datetime","air_temp"])

    noaa_ftp = FTP("ftp.ncei.noaa.gov")
    noaa_ftp.login()

    for _, stn in record_range.iterrows():
        for year in range(stn.first_year,stn.last_year+1):

            # Generate filename based on selected station number and year and download
            # data from NOAA FTP site.
            filepath = "pub/data/noaa/" + str(year) + "/"
            filename = stn.stn_id + "-" + str(year) + ".gz"

            compressed_data = io.BytesIO()

            try:
                noaa_ftp.retrbinary("RETR " + filepath + filename, compressed_data.write)
            except error_perm as e_message:
                print(f"Error generated from NOAA FTP site for station {stn.stn_id} and year {year}: \n", e_message)
                noaa_ftp.quit()
                break

            # Unzip and process data line by line and extract variables of interest
            # The raw data file format is described here:
            # ftp://ftp.ncei.noaa.gov/pub/data/noaa/isd-format-document.pdf
            compressed_data.seek(0)
            stn_year_df = pd.DataFrame(
                columns=[
                    "stn_id",
                    "datetime",
                    "air_temp",
                ]
            )
            with gzip.open(compressed_data, mode="rt") as stn_data:
                stn_data_df = pd.read_csv(stn_data, names=['data'])
                stn_year_df["datetime"] = pd.to_datetime(stn_data_df.data.str.slice(15,27))
                stn_year_df["air_temp"] = pd.to_numeric(stn_data_df.data.str.slice(87,92)) /10

            # Replace missing value indicators with NaNs
            stn_year_df = stn_year_df.replace(
                [999, 999.9, 9999.9], [np.nan, np.nan, np.nan]
            )

            stn_year_df.loc[:, "stn_id"] = stn.stn_id
            weather = weather.append(stn_year_df)
            
            
    noaa_ftp.quit()

    weather = weather.set_index("stn_id")
    return weather

if __name__ == "__main__":
    main()
