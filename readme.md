`sapflow`: An analysis of weekly sap prediction in maple trees
================

# About

It is widely accepted that maple syrup is the most delicious substance
known to humanity. There is a large and growing body of research related
to the biological, chemical, and climatic factors affecting the
production of maple syrup. As more and more data are collected these
areas, the application of data science techniques provides value in
organizing and visualizing the data as well as in inference and
prediction tasks. This repository draws together sap production data
made available by Stinson et al. (2019), publicly available weather data
from the US National Oceanic and Atmospheric Administration weather
stations, and a prediction model proposed by Houle et al. (2015).

## Jump to highlights

  - [Data
    visualizations](https://spentelow.github.io/sapflow/notebooks/data_vis.html)
  - [Analysis](https://spentelow.github.io/sapflow/notebooks/houle_analysis_comparison.html)

## Purpose

This repository has been created with three primary purposes:

1.  Apply the weekly sap prediction model proposed by Houle et al.
    (2015) to data presented in Stinson et al. (2019) to see if the
    model produced predictions of similar precision to that reported by
    Houle et al. (2015) on the original data set.*Note that the model
    referred to here is a logistic regression model predicting ‘yes’ or
    ‘no’ in response the question ‘will sap be produced?’ in a given
    week.*
2.  Develop my own skill in working with diverse data sets, organizing
    data from multiple sources, and creating analysis pipelines.
3.  Create a model to predict the volume of sap produced in a given week
    the data from Stinson et al. (2019). *Note that this model would
    produce a numeric prediction in response to the question ‘how much
    sap will be produced?’ in a given week.*

Significant work has been completed to date related to the first two
purposes and work related to the third is anticipated in the near
future.

## Organization

This repository consists of:

1.  Scripts to download and clean;
2.  Scripts to create a series of database-like normalized tables; and
3.  An interactive coding notebook that walks through the most
    significant analyses.

### Pipeline Overview

Sap flow data provided by Stinson et al. (2019) for 6 measurement
locations (‘Sites’) is downloaded from the USGS ScienceBase-Catelogue.
These data are then normalized into a series of smaller tables in the
style of a relational database (see entity relationship diagram below).
Central to the overall analysis is pairing the sap flow data with local
weather data. To do so, a NOAA weather station nearby each data
collection site has been manually identified. Pairing these weather
stations with the available period of measurements for the nearby Sites
(extracted from the normalized sap flow data), historical weather data
from the selected weather stations are downloaded for the appropriate
periods.

![Chart showing normalized data table
organization.](documentation/img/norm_tables.svg) *Entity-Relationship
diagram for normalized data tables.*

Several derived features important to the remainder of the analysis are
generated from the raw sap and weather data: Growing Degree Days (GDD),
Freeze-Thaw Cycles (frthw), weekly sap flow, and weekly sugar. These
features are calculated are from the previously noted tables.

Using the tables described above, the prediction model proposed by Houle
et al. (2015) is tested on data from the Sites investigated by Stinson
et al. (2019). A [Jupyter
Notebook](notebooks/houle_analysis_comparison.ipynb) is used to perform
this analysis and include annotations on each of the key steps.

The flow chart below illustrates the pipeline comprising this analysis
at a high level.

![Flow chart showing repo
organization.](documentation/img/sapflow_org.svg)

## Usage

  - Install the packages listed in requirements.txt
      - For convenience, a conda environment file has been included in
        this repository and can be used instead of installing each
        package individually. To create the conda environment run the
        following terminal command from the root of this repository:
    <!-- end list -->
    ``` bash
    conda env create -f sapflow.yml
    ```
      - Switch into the new conda environment:
    <!-- end list -->
    ``` bash
     conda activate sapflow
    ```
  - Download and process the raw data locally by running the following
    terminal command from the root of the repository (should take \<5
    minutes on most machines)

<!-- end list -->

``` bash
python src/master.py
```

  - The Jupyter Notebook
    [houle\_analysis\_comparison.ipynb](notebooks/houle_analysis_comparison.ipynb)
    located in the documentation directory can then be run to explore
    the analysis of the model by Houle et al. (2015) using the data from
    Stinson et al. (2019). An .html version of this notebook can be
    viewed at [this
    link](https://spentelow.github.io/sapflow/notebooks/houle_analysis_comparison.html).

## References

<div id="refs" class="references hanging-indent">

<div id="ref-houle">

Houle, Daniel, Alain Paquette, Benoı̂t Côté, Travis Logan, Hugues Power,
Isabelle Charron, and Louis Duchesne. 2015. “Impacts of Climate Change
on the Timing of the Production Season of Maple Syrup in Eastern
Canada.” *PLoS One* 10 (12): e0144844.

</div>

<div id="ref-noaa">

National Centers for Environmental Information: National Oceanic and
Atmospheric Administration. n.d. “Integrated Surface Hourly Data Base.”
<ftp://ftp.ncdc.noaa.gov/pub/data/noaa/>. Accessed March 28, 2021.

</div>

<div id="ref-stinson">

Stinson, Kristina, Joshua Rapp, Selena Ahmed, David Lutz, Ryan Huish,
Boris Dufour, and Toni Lyn Morelli. 2019. “Sap Quantity at Study Sites
in the Northeast.”

</div>

</div>
