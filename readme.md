`sapflow`: An analysis of weekly maple sap prediction
================
Steffen Pentelow

# About

Maple syrup is the most delicious substance known to humanity
([Pentelow, 2021](https://github.com/spentelow/sapflow)). There is a
large and growing body of research related to the biological, chemical,
and practical factors affecting the production of maple syrup. As more
and more data are collected in all of these areas, the application of
data science techniques provides value in organizing and visualizing the
data as well as in modelling and prediction tasks. This repository draws
together sap production data made available by Stinson et al. (2019),
publically available weather data from the US National Oceanic and
Atmospheric Administration weather stations, and a prediction model
proposed by Houle et al. (2015).

## Purpose

This repository has been created with three primary purposes which have
(generally) been in concert with one another. These are:

1.  Apply the weekly sap prediction model proposed by Houle et
    al. (2015) to data presented in Stinson et al. (2019) to see if the
    model produced predictions of similar precision to that reported by
    Houle et al. (2015) on the original data set. *Note that the model
    referred to here is a logistic regression model predicting ‘yes’ or
    ‘no’ in response the question ‘will sap be produced?’ in a given
    week.*
2.  Develop my own skill in working with diverse data sets, organizing
    data from multiple sources, and creating analysis pipelines
3.  Create a model to predict the volume of sap produced in a given week
    the data from Stinson et al. (2019). *Note that this model would
    produce a numeric prediction in response to the question ‘how much
    sap will be produced?’ in a given week.*

Significant work has been completed to date related to the first two
purposes and work related to the third is anticipated in the near
future.

#