"""
Create growing degree days (GDD) and freeze-thaw cycle counts from weather station
data and save pickled dataframe with daily counts of both.

author: Steffen Pentelow
date: 2021-02-19

Usage: src/create_GDD-frthw.py [--gdd_tbase=<gdd_tbase>] [--ft_threshold=<ft_threshold>]

Options:
--gdd_tbase=<gdd_tbase>     Base temperature for growing degree days [default: 5]
--ft_threshold=<ft_threshold>     Base temperature for growing degree days [default: 3]
"""

import numpy as np
import pandas as pd
import os
from docopt import docopt

opt = docopt(__doc__)

def main(opt):
    tbase = float(opt['--gdd_tbase'])  # Growing degree day base temperature
    threshold = float(opt['--ft_threshold'])  # Freeze-thaw temperature threshold

    raw_path = "data/raw/HF_weather"
    processed_path = "data/processed/HF_weather"

    if not os.path.exists(processed_path):
        os.makedirs(processed_path)

    # Load weather station data set
    HF_weather = pd.read_csv(raw_path + '/hf001-08-hourly-m.csv', parse_dates=['datetime'])
    HF_stn_id = 'HF001'
    HF_datetime = 'datetime' #name of datetime column in HF station data
    HF_airt = 'airt' #name of air temperature column in HF station data
    
    gdd = get_gdd(HF_weather, HF_stn_id, tbase=tbase, datetime=HF_datetime, airtemp = HF_airt)
    frzthw = get_frthw(HF_weather, HF_stn_id, threshold=threshold, datetime=HF_datetime, airtemp = HF_airt)

    gdd_frthw = pd.concat([gdd, frzthw.reset_index()["frthw"]], axis=1)
    gdd_frthw.to_pickle(processed_path + '/gdd_frthw')

    return

def get_gdd(data, stn_id, tbase, datetime="datetime", airtemp="airt"):
    """Calculate cumulative growing degree days (GDD) from weather station data

    Parameters
    ----------
    data : DataFrame
        Weather station data.
    stn_id : str
        Name of weather station where data is from.
    tbase : int, optional
        GDD base temperature, by default 5
    datetime : str, optional
        Name of datetime column in 'data', by default "datetime"
    airtemp : str, optional
        Name of air temperature column in 'data', by default "airt"

    Returns
    -------
    DataFrame
        Table containing mean daily air temperature and cumulative GDD (resets
        on January 1st each year).  One row per day.
    """
    gdd_df = (
        data[[datetime, airtemp]]
        .groupby(pd.Grouper(key=datetime, freq="1D"))
        .mean()
        .reset_index()
    )
    gdd_df.rename(columns={airtemp: "mean_airt"}, inplace=True)
    gdd_df["GDD"] = gdd_df[["mean_airt"]].applymap(
        lambda x: 0 if x <= tbase else x - tbase
    )

    data_datetime = pd.DatetimeIndex(gdd_df[datetime])

    for year in data_datetime.year.unique():
        gdd_df.loc[(data_datetime.year == year), "cumGDD"] = gdd_df[
            data_datetime.year == year
        ]["GDD"].cumsum()

    gdd_df["STN"] = stn_id

    return gdd_df

def get_frthw(data, stn_id, threshold, datetime="datetime", airtemp="airt"):
    """Calculate cumulative freeze-thaw cycles from weather station data.

    Parameters
    ----------
    data : DataFrame
        Weather station data.
    stn_id : str
        Name of weather station where data is from.
    threshold : int, optional
        Freeze-thaw temperature threshold, by default 0
    datetime : str, optional
        Name of datetime column in 'data', by default "datetime"
    airtemp : str, optional
        Name of air temperature column in 'data', by default "airt"

    Returns
    -------
    DataFrame
        Table containing cumulative freeze-thaw cycles (resets on
        January 1st each year).  One row per day.
    """

    # Initialize summary dataframe
    frthw_df = pd.DataFrame()

    data_datetime = pd.DatetimeIndex(data[datetime])
    data["frthw"] = np.nan

    for year in data_datetime.year.unique():

        year_data = data.loc[
            (data_datetime.year == year), [datetime, airtemp, "frthw"]
        ]
        frthw = (
            0.5 if year_data.iloc[0, 1] > threshold else 0
        )  # If first temp reading is above threshold, start frthw at 0.5

        for obs in range(year_data.shape[0]):
            if frthw % 1 == 0:
                if year_data.iloc[obs, 1] >= threshold:
                    frthw += 0.5
            else:
                if year_data.iloc[obs, 1] < threshold:
                    frthw += 0.5
            year_data.iloc[obs, 2] = frthw

        year_data = year_data.drop(columns=[airtemp])
        year_data = year_data.set_index(datetime).resample("1D").last()
        frthw_df = frthw_df.append(year_data)

    frthw_df["STN"] = stn_id

    return frthw_df

if __name__ == "__main__":
    main(opt)
