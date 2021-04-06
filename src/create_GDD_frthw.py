"""
Create growing degree days (GDD) and freeze-thaw cycle counts from weather station
data and save pickled dataframe with daily counts of both.

author: Steffen Pentelow
date: 2021-02-19

Usage: src/create_GDD_frthw.py [--gdd_tbase=<gdd_tbase>] [--ft_threshold=<ft_threshold>]

Options:
--gdd_tbase=<gdd_tbase>     Base temperature for growing degree days [default: 5]
--ft_threshold=<ft_threshold>     Base temperature for growing degree days [default: 3]
"""

import numpy as np
import pandas as pd
import os
from docopt import docopt


def main(tbase = 5, threshold = 3):
    tbase = float(tbase)
    threshold = float(threshold)

    processed_path = os.path.join("data","processed", "stinson2019", "norm_tables")

    if not os.path.exists(processed_path):
        os.makedirs(processed_path)
   
    weather = pd.read_pickle(os.path.join(processed_path, 'weather'))  # Load weather station data set

    gdd_frthw = pd.DataFrame(columns = ['stn_id', 'datetime', 'mean_airt', 'GDD', 'cumGDD', 'frthw'])
    
    for station in weather.index.unique().to_list():
        datetime = 'datetime'  # name of datetime column in HF station data
        airt = 'air_temp'  # name of air temperature column in HF station data
        gdd = get_gdd(weather[weather.index == station], station, tbase=tbase, datetime=datetime, airtemp = airt)
        frzthw = get_frthw(weather[weather.index == station], station, threshold=threshold, datetime=datetime, airtemp = airt)
        gdd_frthw = gdd_frthw.append(pd.concat([gdd, frzthw.reset_index()["frthw"]], axis=1))
    
    gdd_frthw.to_pickle(os.path.join(processed_path, 'gdd_frthw'))

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
    gdd_df = gdd_df.dropna()

    dates_index = pd.date_range(gdd_df.datetime.min(), gdd_df.datetime.max())

    # **ASSUMPTION**
    # If there are missing mean daily temperature values, forward fill from the
    # last valid measurement.
    # **ASSUMPTION**
    
    if (len(dates_index) - len(gdd_df)) > 0:
        print(
            f'''
            Warning: Some days within the data collection window do not have
            associated temperature readings from weather station {stn_id}.
            A total of {len(dates_index) - len(gdd_df):.0f} days are missing temperature
            readings and have been filled with values from the previous day
            with temperature readings.
            '''
            )
        gdd_df.set_index('datetime', inplace = True)
        gdd_df.index = pd.DatetimeIndex(gdd_df.index)
        gdd_df = gdd_df.reindex(dates_index, method='ffill')
        gdd_df = gdd_df.reset_index()
        gdd_df.rename(columns={'index':'datetime'}, inplace=True)


    gdd_df["GDD"] = gdd_df[["mean_airt"]].applymap(
        lambda x: 0 if x <= tbase else x - tbase
    )

    data_datetime = pd.DatetimeIndex(gdd_df[datetime])

    for year in data_datetime.year.unique():
        gdd_df.loc[(data_datetime.year == year), "cumGDD"] = gdd_df[
            data_datetime.year == year
        ]["GDD"].cumsum()

    gdd_df["stn_id"] = stn_id

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
    pd.set_option("mode.chained_assignment", None)
    data["frthw"] = np.nan
    pd.set_option("mode.chained_assignment", "warn")

    for year in data_datetime.year.unique():

        year_data = data.loc[
            (data_datetime.year == year), [datetime, airtemp, "frthw"]
        ]
        frthw = (
            0.5 if year_data.iloc[0, 1] > threshold else 0
        )  
        # **ASSUMPTION**
        # If first temp reading of the year is above the freeze-thaw threshold 
        # value, start frthw variable at 0.5 (half a freeze-thaw cycle)
        # **ASSUMPTION**

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
        
        year_data = year_data.dropna()
        dates_index = pd.date_range(year_data.index.min(), year_data.index.max())
        
        # **ASSUMPTION**
        # If there are missing mean daily temperature values, forward fill from the
        # last valid measurement.
        # **ASSUMPTION**
        
        if (len(dates_index) - len(year_data)) > 0:
            year_data = year_data.reindex(dates_index, method='ffill')
            print(year_data.columns)

        frthw_df = frthw_df.append(year_data)

    frthw_df["stn_id"] = stn_id

    return frthw_df

if __name__ == "__main__":

    opt = docopt(__doc__)
    tbase = opt["--gdd_tbase"]
    threshold = opt["--ft_threshold"]
    main(tbase = tbase, threshold=threshold)
