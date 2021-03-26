"""
Download maple tree sap flow data from various sources

author: Steffen Pentelow
date: 2021-02-18

Usage: python src/data_download_sap.py
"""

import sciencebasepy
import os
import pandas as pd
import requests


def main():
    stinson2019()
    return

def stinson2019():
    """Downloads all files from ScienceBase item number "5d67eacae4b0c4f70cf15be3"

    ## Data website ##
    https://www.sciencebase.gov/catalog/item/5d67eacae4b0c4f70cf15be3

    ## Citation ##
    Stinson, K., Rapp, J., Ahmed, S., Lutz, D., Huish, R., Dufour, B., and Morelli, T.L., 2019,
    Sap Quantity at Study Sites in the Northeast: U.S. Geological Survey data release, https://doi.org/10.5066/P9H65YCC.
    """


    sb = sciencebasepy.SbSession()
    raw_path = os.path.join("data","raw","stinson2019")
    processed_path = os.path.join("data","processed","stinson2019")

    if not os.path.exists(raw_path):
        os.makedirs(raw_path)	

    if not os.path.exists(processed_path):
        os.makedirs(processed_path)

    item_json = sb.get_item("5d67eacae4b0c4f70cf15be3")
    sb.get_item_files(item_json, raw_path)

    # Convert data csv files to dataframes and pickle
    df = pd.read_csv(os.path.join(raw_path, 'ACERnet_sap_2012_2017_ID.csv'),
                    parse_dates=['Date', 'Year'])
    df.columns=[x.lower().replace('.','_') for x in list(df.columns)]
    df.to_pickle(os.path.join(processed_path, 'stinson2019_df'))

    locations = pd.read_csv(os.path.join(raw_path, 'ACERnet_LatLon.csv'))
    locations.to_pickle(os.path.join(processed_path, 'stinson2019_locations'))

if __name__ == "__main__":
    main()
