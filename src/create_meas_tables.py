"""
Create normalized data tables for sap measurement data.

author: Steffen Pentelow
date: 2021-02-19

Usage: src/create_meas_tables.py
"""

import numpy as np
import pandas as pd
import copy
import os


def main():

    processed_path = os.path.join("data", "processed", "stinson2019")

    if not os.path.exists(os.path.join(processed_path, "norm_tables")):
        os.makedirs(os.path.join(processed_path, "norm_tables"))

    # Load sap flow data
    stinson2019 = pd.read_pickle(os.path.join(processed_path, "stinson2019_df"))

    data = add_ids(stinson2019)  # Add unique record id column to dataframe
    normalized_data = normalized_tables(data)  # Move data to normalized tables
    
    # Save normalized tables
    for table in normalized_data.keys():
        normalized_data[table].to_pickle(os.path.join(processed_path, "norm_tables", table))

    return


def add_ids(data):
    """Add unique record ids for each entry in sap dataframe

    Parameters
    ----------
    data : DataFrame
        Dataframe containing sap records

    Returns
    -------
    DataFrame
        Dataframe same as input but with new column containing a
        unique record id for each entry of the form:

        '<TreeID>_<TapID>_<RecordYear>_<ID#>'

        where ID# is 0 for the first record for a given tap in <RecordYear>,
        1 is the second record of the year, etc.
    """

    id_df = data.sort_values(["site_id", "tree", "tap", "date"])

    # Create unique record ids for each entry in the following form:
    # "<TreeID>_<TapID>_<RecordYear>_<ID#>" where ID# is 0 for the first
    # record for a given tap in <RecordYear>, 1 is the second ...

    # First create "<TreeID>_<TapID>_<RecordYear>_" label for each record_id
    id_df["record_id"] = (
        +id_df["tree"]
        + "_"
        + id_df["tap"]
        + "_"
        + pd.DatetimeIndex(id_df["year"]).year.astype(str)
        + "_"
    )

    # Add "<ID#>" to each record_id
    for tapyear in id_df["record_id"].unique():
        id_df.loc[id_df["record_id"] == tapyear, "record_id"] += [
            str(i) for i in range(id_df[id_df["record_id"] == tapyear].shape[0])
        ]

    id_df["tap_id"] = id_df["tree"] + id_df["tap"]

    return id_df


def normalized_tables(data):
    """Create normalized tables from wide dataframe of sap measurements

    Parameters
    ----------
    data : DataFrame
        Wide dataframe of sap measurements

    Returns
    -------
    dict
        Dict of 7 normalized dataframes: tap_records, sap, sugar, dates, tap_tree, tree_species, site
    """
    df = {}

    df["tap_records"] = data[["record_id", "tap_id"]].set_index("record_id")
    df["sap"] = (
        data[["record_id", "sap_wt"]]
        .rename(columns={"sap_wt": "sap"})
        .set_index("record_id")
    )
    df["sugar"] = data[["record_id", "sugar"]].set_index("record_id")
    df["dates"] = data[["record_id", "date"]].set_index("record_id")
    df["dates"].loc[:, "date"] = pd.to_datetime(df["dates"]["date"])
    df["tap_tree"] = data[["tap_id", "tree"]].drop_duplicates().set_index("tap_id")
    df["tree_species"] = data[["tree", "species"]].drop_duplicates().set_index("tree")
    df["tree_site"] = data[["tree", "site_id"]].drop_duplicates().set_index("tree")
    df["tree_site"]["site_id"] = df["tree_site"]["site_id"].str.upper()
    df["tree_site"] = df["tree_site"].rename(columns={"site_id": "site"})

    return df


if __name__ == "__main__":
    
    main()
