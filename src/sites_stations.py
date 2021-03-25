"""
Create data tables for site location information and closest NOAA weather
station information (table names: 'location', 'closest_weather_stn',
'weather_stn')

author: Steffen Pentelow
date: 2021-03-15

Usage: src/sites_stations.py
"""

import os
from ftplib import FTP, error_perm
import pandas as pd


def main():

    processed_path = os.path.join("data","processed","stinson2019","norm_tables")
    raw_path = os.path.join("data","raw")

    if not os.path.exists(processed_path):
        os.makedirs(processed_path)

    # Load location information data
    location = pd.read_csv(os.path.join(raw_path, "stinson2019", "ACERnet_LatLon.csv"))

    location_table = create_loc_table(location)
    closest_weather_station = create_closest_stn_tbl()
    weather_stn = create_wstn_tables(raw_path, closest_weather_station)

    location_table.to_pickle(os.path.join(processed_path, "location"))
    closest_weather_station.to_pickle(os.path.join(processed_path, "closest_weather_stn"))
    weather_stn.to_pickle(os.path.join(processed_path, "weather_stn"))

    return


def create_loc_table(location):
    """Create table of information on each data collection site from raw file.

    Parameters
    ----------
    data : str
        Raw site location information .csv file name.

    Returns
    -------
    pandas.DataFrame
        Data frame with parsed site location information

     Examples
    --------
    >>> create_loc_table(os.path.join('data','raw','stinson2019','ACERnet_LatLon.csv'))
    """

    # Load location names/info
    location = location.rename(
        columns={"Site": "site", "Loc": "state_province"}
        )
    location = location.set_index("site")

    # Fix inconsistency in acronym for Quebec site between data tables
    if "QB" in location.index:
        location = location.rename({"QB": "QC"}, axis="index")
        location.loc["QC", :] = location.loc["QC", :].replace(
            regex=[r"^QB$"], value="QC"
        )

    location = location[
        ["lat", "lon", "short_name", "long_name", "state_province"]
        ]

    return location


def create_closest_stn_tbl():
    """Create table with data collection site and the station ID for the
    closest NOAA station to that site.  This implementation requires manually
    inputting the station ID closest to each data collection site.

    Returns
    -------
    pandas.DataFrame
        Data frame with entries for each site location.
    """
    # Create DF of station IDs

    stn_ids = [
        ["INDU", "726358-00384"],
        ["SMM", "724115-93757"],
        ["DR", "724117-63802"],
        ["QC", "716170-99999"],
        ["HF", "725085-54756"],
        ["DOF", "726116-94765"],
    ]

    closest_weather_station = pd.DataFrame(
        stn_ids, columns=["site", "stn_id"]
    ).set_index("site")
    return closest_weather_station


def create_wstn_tables(raw_path, closest_weather_station):
    """For relevant NOAA weather stations, this function downloads, parses raw
    data, and creates a local file containing info on the stations.

    Parameters
    ----------
    raw_path : str
        List of normalized dataframes from `normalized_tables` function.
    closest_weather_station : pandas.DataFrame
        Dataframe with columns 'site' and 'stn_id'. Created by
        closest_weather_station() function.

    Returns
    -------
    pandas.DataFrame
        Dataframe containing information on each relevant NOAA weather station.
    """

    # This chunk downloads and creates a local file containing the weather
    # station information

    # Get station info file from the ftp site
    noaa_ftp = FTP("ftp.ncei.noaa.gov")
    noaa_ftp.login()  # Log in (no user name or password required)
    noaa_ftp.cwd("pub/data/noaa/")

    stn_history_file = "isd-history.txt"

    if not os.path.exists(os.path.join(raw_path,"NOAA")):
        os.makedirs(os.path.join(raw_path, "NOAA"))

    # Open the station history file and save it locally
    try:
        with open(os.path.join(raw_path, "NOAA", stn_history_file), "wb+") as stn_hist:
            noaa_ftp.retrbinary("RETR " + stn_history_file, stn_hist.write)
    except error_perm as e_message:
        print("Error generated from NOAA FTP site: \n", e_message)
        noaa_ftp.quit()
        return
    else:
        noaa_ftp.quit()

    weather_stn = pd.DataFrame(
        columns=[
            "stn_name",
            "lat",
            "lon",
            "elevation_m",
            "country",
            "state",
            "start",
            "end",
        ],
        index=closest_weather_station["stn_id"],
    )

    # Parse station history file line by line
    with open(os.path.join(raw_path, "NOAA",stn_history_file), mode="rt") as stn_hist:
        for line in stn_hist:
            stn = line[0:6] + "-" + line[7:12]
            if stn in weather_stn.index.tolist():
                weather_stn.loc[stn] = [
                    line[13:43].strip(),
                    line[57:65].strip(),
                    line[65:74].strip(),
                    line[74:82].strip(),
                    line[43:48].strip(),
                    line[48:51].strip(),
                    line[82:91].strip(),
                    line[91:].strip(),
                ]
    weather_stn.start = pd.to_datetime(weather_stn.start)
    weather_stn.end = pd.to_datetime(weather_stn.end)
    weather_stn.elevation_m = weather_stn.elevation_m.astype(float)
    weather_stn.lat = weather_stn.lat.astype(float)
    weather_stn.lon = weather_stn.lon.astype(float)

    return weather_stn


if __name__ == "__main__":
    main()
