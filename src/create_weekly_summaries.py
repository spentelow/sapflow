"""
Create DataFrame with daily summaries of weekly sap and sugar production based on sap flow records.

author: Steffen Pentelow
date: 2021-02-19

Usage: src/create_weekly_summaries.py [--location=<location>] [--tree=<tree>] [--tap=<tap>] [--years=<years>] [--species=<species>]

Options:
--location=<location>   Name of locations (sites) to be included in data table [default: all]
--tree=<tree>           ID of trees to be included in data table [default: all]
--tap=<tap>             ID of taps to be included in data table [default: all]
--years=<years>         Years to be included in data table [default: all]
--species=<species>     Species to be included in data table [default: ACSA]
"""

import numpy as np
import pandas as pd
import copy
import os
from docopt import docopt


def main(loc = 'all', tre = 'all', tp = 'all', yrs = 'all', spec = 'ACSA'):

    processed_path = os.path.join("data", "processed", "stinson2019", "norm_tables")

    full_df = get_weekly_data(
       processed_path, location=loc, tree=tre, tap_id=tp, years=yrs, species=spec
    )  # Calculate weekly summary parameters
    
    tree_site = pd.read_pickle(os.path.join(processed_path,'tree_site'))
    full_df = full_df.reset_index().merge(
        tree_site, on="tree", how="left"
    )
    
    sap_sugar_df = full_df.loc[
        :, ["tap_id", "date_from", "date_to", "weekly_sugarwt", "weekly_sap", "site"]
    ]

    full_df.to_pickle(os.path.join(processed_path, "full_weekly_summary"))
    sap_sugar_df.to_pickle(os.path.join(processed_path, "sap_sugar_weekly_summary"))

    return

def get_weekly_data(
    processed_path,
    location=["all"],
    tree="all",
    tap_id="all",
    years="all",
    species="ACSA",
):
    """Generate data table containing cumulative weekly sap and sugar amounts

    Parameters
    ----------
    location : str or list of str, optional
        Name of locations (sites) to be included in data table, by default 'all'
    tree : str or list of str, optional
        ID of trees to be included in data table, by default 'all'
    tap_id : str or list of str, optional
        ID of taps to be included in data table, by default 'all'
    years : int, list of ints, or 'all' , optional
        Years to be included in data table, by default 'all'
    species : str, list of str, or 'all' , optional
        Species to be included in data table, by default 'ACSA' (sugar maple)

    Returns
    -------
    pd.DataFrame
        Table with weekly summaries for all taps specified in arguments.  Includes
        cumulative sap and sugar weight, and weekly sap and sugar weight.
    """

    # Unpack normalized DataFrames
    tap_records = pd.read_pickle(os.path.join(processed_path,"tap_records"))
    sap = pd.read_pickle(os.path.join(processed_path,'sap'))
    sugar = pd.read_pickle(os.path.join(processed_path,'sugar'))
    dates = pd.read_pickle(os.path.join(processed_path,'dates'))
    tap_tree = pd.read_pickle(os.path.join(processed_path,'tap_tree'))
    tree_species = pd.read_pickle(os.path.join(processed_path,'tree_species'))
    site = pd.read_pickle(os.path.join(processed_path,'tree_site'))

    # Check and clean location argument
    if type(location) != list:
        location = [location]
    location = [x.upper() for x in location]
    if location == ["ALL"]:
        location = site["site"].unique().tolist()

    # Check and clean tap_id argument
    if type(tap_id) != list:
        tap_id = [tap_id]
    tap_id = [x.upper() for x in tap_id]
    if tap_id == ["ALL"]:
        tap_id = tap_tree.index.tolist()

    # Check and clean tree argument
    if type(tree) != list:
        tree = [tree]
    tree = [x.upper() for x in tree]
    if tree == ["ALL"]:
        tree = tap_tree["tree"].unique().tolist()

    # Check and clean years argument
    if type(years) != list:
        years = [years]
    if type(years[0]) == str:
        years[0] = years[0].upper()
        if years == ["ALL"]:
            years = pd.DatetimeIndex(dates["date"]).year.unique().tolist()

    # Check and clean species argument
    if type(species) != list:
        species = [species]
    species = [x.upper() for x in species]
    if species == ["ALL"]:
        species = tree_species["species"].unique().tolist()

    tap_id = (
        tap_tree[tap_tree.index.isin(tap_id)]
        .join(site, how="left", on="tree")
        .reset_index()
        .merge(tree_species, how="left", on="tree")
        .set_index("tap_id")
    )
    tap_id = tap_id[
        (tap_id["tree"].isin(tree))
        & (tap_id["site"].isin(location))
        & (tap_id["species"].isin(species))
    ].index.tolist()

    # Initialize summary dataframe
    weekly_df = pd.DataFrame()

    # Create weekly summaries, iterating through all taps
    for tap in tap_id:
        #         print("tap:  ", tap)
        # Create joint dataframe will all required info for current tap
        df = (
            tap_records.join(tap_tree[tap_tree.index == tap], how="right", on="tap_id")
            .join(sap, how="left")
            .join(sugar, how="left")
            .join(dates[pd.DatetimeIndex(dates["date"]).year.isin(years)], how="inner")
        )
        df["year"] = pd.DatetimeIndex(df["date"]).year
        df["jd"] = pd.DatetimeIndex(df["date"]).dayofyear

        for year in df["year"].unique():
            #             print('     year: ', year)
            df_year = df[df["year"] == year]

            # Deal with multiple entries per day.  Sap taken as sum of measurements, sugar content as weighted average.
            if not df_year["jd"].is_unique:
                df_year_temp = copy.copy(df_year)
                df_year_temp["product"] = df_year_temp.sap * df_year_temp.sugar.fillna(
                    value=df_year_temp.sugar.mean()
                )
                df_year_temp = df_year_temp.groupby(by="jd").sum().reset_index()
                df_year_temp["sugar"] = df_year_temp["product"] / df_year_temp["sap"]
                df_year = df_year.drop_duplicates(subset="jd")
                df_year = (
                    df_year.reset_index()
                    .merge(
                        df_year_temp[["jd", "sap", "sugar"]],
                        on="jd",
                        how="right",
                        suffixes=["", "_sum"],
                    )
                    .set_index("record_id")
                )
                df_year["sap"] = df_year["sap_sum"]
                df_year["sugar"] = df_year["sugar_sum"]
                df_year = df_year.drop(columns=["sap_sum", "sugar_sum"])

            # Add entry for every day of year from first day with recorded flow to last
            df_year = (
                df_year.reset_index()
                .merge(
                    pd.date_range(
                        start=df_year["date"].min(), end=df_year["date"].max()
                    ).to_frame(name="date"),
                    how="right",
                    on="date",
                )
                .set_index("date", drop=False)
            )

            # Assumption: missing sugar content should be filled with mean sugar content
            df_year["sugarwt"] = (
                df_year.sap * df_year.sugar.fillna(value=df_year.sugar.mean()) / 100
            )

            # Assumption: missing sap values should be replaced with zeros
            df_year["cum_sap"] = df_year.sap.fillna(value=0).cumsum()
            df_year["cum_sugarwt"] = df_year.sugarwt.fillna(value=0).cumsum()
            df_year["tap_id"] = df_year.tap_id.fillna(value=tap)
            df_year["tree"] = df_year.tree.fillna(value=df_year.tree[0])
            df_year["year"] = pd.DatetimeIndex(df_year["date"]).year
            df_year["jd"] = pd.DatetimeIndex(df_year["date"]).dayofyear

            if df_year.shape[0] > 7:
                df_year["weekly_sap"] = df_year.loc[
                    :, "cum_sap"
                ].to_numpy() - np.concatenate(
                    (np.zeros(7), df_year.iloc[:-7]["cum_sap"].to_numpy()), axis=0
                )
                df_year["weekly_sugarwt"] = df_year.loc[
                    :, "cum_sugarwt"
                ].to_numpy() - np.concatenate(
                    (np.zeros(7), df_year.iloc[:-7]["cum_sugarwt"].to_numpy()), axis=0
                )
            else:
                df_year["weekly_sap"] = df_year["cum_sap"]
                df_year["weekly_sugarwt"] = df_year["cum_sugarwt"]

            df_year["cum_syrupLitres"] = df_year["cum_sugarwt"] / 1.33
            df_year["weekly_syrupLitres"] = df_year["weekly_sugarwt"] / 1.33

            df_year["date_from"] = df_year["date"] - pd.to_timedelta(6, unit="D")
            df_year["date_to"] = df_year["date"]
            df_year["jd_from"] = df_year["jd"] - 6
            df_year["jd_to"] = df_year["jd"]
            df_year = df_year.drop(columns=["date", "jd", "record_id"])

            weekly_df = weekly_df.append(df_year)

    return weekly_df


if __name__ == "__main__":
    opt = docopt(__doc__)
    loc = opt["--location"]
    tre = opt["--tree"]
    tp = opt["--tap"]
    yrs = opt["--years"]
    spec = opt["--species"]

    main(loc=loc, tre=tre, tp=tp, yrs=yrs, spec=spec)
