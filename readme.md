`sapflow`: An analysis of weekly maple sap prediction
================
Steffen Pentelow

# About

It is widely accepted that maple syrup is the most delicious substance
known to humanity
([Pentelow, 2021](https://github.com/spentelow/sapflow)). There is a
large and growing body of research related to the biological, chemical,
and climatic factors affecting the production of maple syrup. As more
and more data are collected these areas, the application of data science
techniques provides value in organizing and visualizing the data as well
as in inference and prediction tasks. This repository draws together sap
production data made available by Stinson et al. (2019), publicly
available weather data from the US National Oceanic and Atmospheric
Administration weather stations, and a prediction model proposed by
Houle et al. (2015).

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

1.  Various scripts which download, clean, and analyze the data; and
2.  An interactive coding notebook that walks through the most
    significant analysis.

The first item begins with downloading sap flow data provided for 6
measurement locations (‘Sites’) by Stinson et al. (2019) from the USGS
ScienceBase-Catelogue. The data are then normalized into a series of
smaller tables for ease-of-use. Central to the overall analysis is
pairing the sap flow data with local weather data. To do so, a NOAA
weather station nearby each data collection site has been manually
identified. Pairing these weather stations with the available period of
measurements for the nearby Sites (extracted from the normalized sap
flow data), historical weather data from the selected weather stations
are downloaded for the appropriate periods. Several derived parameters
were generated from the raw sap and weather data are important to the
remainder of the analysis: Growing Degree Days (GDD), Freeze-Thaw Cycles
(frthw), weekly sap flow, and weekly sugar.

Using the tables described above, the prediction model proposed by Houle
et al. (2015) was tested on data from the Sites investigated by Stinson
et al. (2019). A [Jupyter Notebook](documentation/weekly_analysis.ipynb)
was used to perform this analysis and include annotations on each of the
key steps.

The flow chart below illustrates the pipeline comprising this analysis
at a high level.

![Flow chart showing repo
organization.](documentation/img/Sapflow_org.svg)

The following image illustrates the structure and connection between the
various normalized sap flow and weather data tables. Note that derived
parameter tables are not included.

![Chart showing normalized data table
organization.](documentation/img/norm_tables.svg)

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
[ftp.ncdc.noaa.gov/pub/data/noaa/](ftp.ncdc.noaa.gov/pub/data/noaa/).
Accessed March 28, 2021.

</div>

<div id="ref-stinson">

Stinson, Kristina, Joshua Rapp, Selena Ahmed, David Lutz, Ryan Huish,
Boris Dufour, and Toni Lyn Morelli. 2019. “Sap Quantity at Study Sites
in the Northeast.”

</div>

</div>
