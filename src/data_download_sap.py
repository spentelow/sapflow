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
    templer2020()
    rapp2016()
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

def templer2020():
    """ Loads data from study listed below and creates pandas dataframes containing the data.  Dataframes are saved to .csv.

    ## Data website ##
    https://portal.edirepository.org/nis/mapbrowse?packageid=knb-lter-hfr.338.1

    ## Citation ##
    Templer, P. 2020. Sap Flow in Red Maple and Red Oak in the Harvard Forest Snow Removal Study 2011 ver 1. 
    Environmental Data Initiative. https://doi.org/10.6073/pasta/f4e8ef6675dff989baae075b476e9f13 (Accessed 2021-01-04).

    # The code below has been adapted from: 'https://portal.edirepository.org/nis/codeGeneration?packageId=knb-lter-hfr.338.1&statisticalFileType=py'

    Package ID: knb-lter-hfr.338.1 Cataloging System:https://pasta.edirepository.org.
    Data set title: Sap Flow in Red Maple and Red Oak in the Harvard Forest Snow Removal Study 2011.
    Data set creator:  Pamela Templer -  
    Contact:  Information Manager -  Harvard Forest  - hf-im@lists.fas.harvard.edu
    Stylesheet v1.0 for metadata conversion into program: John H. Porter, Univ. Virginia, jporter@virginia.edu      
    
    This program creates numbered PANDA dataframes named dt1,dt2,dt3...,
    one for each data table in the dataset. It also provides some basic
    summaries of their contents. NumPy and Pandas modules need to be installed
    for the program to run
    """


    raw_path = os.path.join("data","raw","templer2020")
    processed_path = os.path.join("data","processed", "templer2020")

    if not os.path.exists(raw_path):
        os.makedirs(raw_path)	

    if not os.path.exists(processed_path):
        os.makedirs(processed_path)


    infile1  ="https://pasta.lternet.edu/package/data/eml/knb-lter-hfr/338/1/a34fde7eee187f323b8d0eea1dc74823".strip() 
    infile1  = infile1.replace("https://","http://")
                    
    dt1 =pd.read_csv(infile1 
            ,skiprows=1
                ,sep=","  
            , names=[
                        "datetime",     
                        "doy",     
                        "year",     
                        "time",     
                        "plott",     
                        "treatment",     
                        "tree",     
                        "species",     
                        "instant_sap"    ]
            ,parse_dates=[
                            'datetime',
                            'year',
                    ] 
                ,na_values={
                    'datetime':[
                            'NA',],
                    'doy':[
                            'NA',],
                    'year':[
                            'NA',],
                    'time':[
                            'NA',],
                    'plot':[
                            'NA',],
                    'tree':[
                            'NA',],
                    'species':[
                            'NA',],
                    'instant_sap':[
                            'NA',],} 
                
        )

    # Coerce the data into the types specified in the metadata 
    # Since date conversions are tricky, the coerced dates will go into a new column with _datetime appended
    # This new column is added to the dataframe but does not show up in automated summaries below. 
    dt1=dt1.assign(datetime_datetime=pd.to_datetime(dt1.datetime,errors='coerce')) 
    dt1.doy=pd.to_numeric(dt1.doy,errors='coerce',downcast='integer') 
    # Since date conversions are tricky, the coerced dates will go into a new column with _datetime appended
    # This new column is added to the dataframe but does not show up in automated summaries below. 
    dt1=dt1.assign(year_datetime=pd.to_datetime(dt1.year,errors='coerce')) 
    dt1.time=pd.to_numeric(dt1.time,errors='coerce',downcast='integer')  
    dt1.plott=dt1.plott.astype('category')  
    dt1.treatment=dt1.treatment.astype('category')  
    dt1.tree=dt1.tree.astype('category')  
    dt1.species=dt1.species.astype('category') 
    dt1.instant_sap=pd.to_numeric(dt1.instant_sap,errors='coerce') 

        
    dt1.to_pickle(os.path.join(processed_path, 'templer2020_df'))


    # Download raw data and save as csv
    raw_data = requests.get(infile1)

    with open(os.path.join(raw_path, 'templer2020_raw1.csv'), 'wb+') as data_file:
        for chunk in raw_data:
                data_file.write(chunk)

    # Download metadata and save
    metadata_url = 'https://pasta.lternet.edu/package/metadata/eml/knb-lter-hfr/338/1'
    metadata_xml = requests.get(metadata_url)

    with open(os.path.join(raw_path, 'templer2020_metadata.xml'), 'w+') as metadata_file:
        metadata_file.write(metadata_xml.text)

def rapp2016():
    """ Loads data from study listed below and creates pandas dataframes containing the data.  Dataframes are saved to .csv.

    ## Data website ##
    https://portal.edirepository.org/nis/mapbrowse?packageid=knb-lter-hfr.285.3

    ## Citation ##
    Rapp, J., E. Crone, and K. Stinson. 2020. Maple Reproduction and Sap Flow at Harvard Forest since 2011 ver 3. Environmental Data Initiative. 
    https://doi.org/10.6073/pasta/3b8b9a83188b910bb3d27d3be59bdd4e (Accessed 2021-01-04).

    # The code below has been adapted from: 'https://portal.edirepository.org/nis/codeGeneration?packageId=knb-lter-hfr.285.3&statisticalFileType=py'

    Package ID: knb-lter-hfr.285.3 Cataloging System:https://pasta.edirepository.org.
    Data set title: Maple Reproduction and Sap Flow at Harvard Forest since 2011.
    Data set creator:  Joshua Rapp -  
    Data set creator:  Elizabeth Crone -  
    Data set creator:  Kristina Stinson -  
    Contact:  Joshua Rapp -  Harvard Forest  - rapp@fas.harvard.edu
    Stylesheet v1.0 for metadata conversion into program: John H. Porter, Univ. Virginia, jporter@virginia.edu      
    
    This program creates numbered PANDA dataframes named dt1,dt2,dt3...,
    one for each data table in the dataset. It also provides some basic
    summaries of their contents. NumPy and Pandas modules need to be installed
    for the program to run.
    """

    raw_path = os.path.join("data","raw","rapp2016")
    processed_path = os.path.join("data","processed","rapp2016")

    if not os.path.exists(raw_path):
        os.makedirs(raw_path)	

    if not os.path.exists(processed_path):
        os.makedirs(processed_path)

    infile1  ="https://pasta.lternet.edu/package/data/eml/knb-lter-hfr/285/3/70673164176668f2f0450115e59f31bd".strip() 
    infile1  = infile1.replace("https://","http://")
                    
    dt1 =pd.read_csv(infile1 
            ,skiprows=1
                ,sep=","  
            , names=[
                        "date",     
                        "tree",     
                        "tap",     
                        "species",     
                        "dbh",     
                        "tap_bearing",     
                        "tap_height"    ]
            ,parse_dates=[
                            'date',
                    ] 
                ,na_values={
                    'date':[
                            'NA',],
                    'tree':[
                            'NA',],
                    'tap':[
                            'NA',],
                    'species':[
                            'NA',],
                    'dbh':[
                            'NA',],
                    'tap_bearing':[
                            'NA',],
                    'tap_height':[
                            'NA',],} 
                
        )
    # Coerce the data into the types specified in the metadata 
    # Since date conversions are tricky, the coerced dates will go into a new column with _datetime appended
    # This new column is added to the dataframe but does not show up in automated summaries below. 
    dt1=dt1.assign(date_datetime=pd.to_datetime(dt1.date,errors='coerce'))  
    dt1.tree=dt1.tree.astype('category')  
    dt1.tap=dt1.tap.astype('category')  
    dt1.species=dt1.species.astype('category') 
    dt1.dbh=pd.to_numeric(dt1.dbh,errors='coerce') 
    dt1.tap_bearing=pd.to_numeric(dt1.tap_bearing,errors='coerce',downcast='integer') 
    dt1.tap_height=pd.to_numeric(dt1.tap_height,errors='coerce',downcast='integer') 
        
    infile2  ="https://pasta.lternet.edu/package/data/eml/knb-lter-hfr/285/3/c1f4585bebbf0c76d32352d254bc8e64".strip() 
    infile2  = infile2.replace("https://","http://")
                    
    dt2 =pd.read_csv(infile2 
            ,skiprows=1
                ,sep=","  
            , names=[
                        "date",     
                        "tree",     
                        "tap",     
                        "time",     
                        "datetime",     
                        "sugar",     
                        "species",     
                        "sap_wt"    ]
            ,parse_dates=[
                            'date',
                            'time',
                            'datetime',
                    ] 
                ,na_values={
                    'date':[
                            'NA',],
                    'tree':[
                            'NA',],
                    'tap':[
                            'NA',],
                    'time':[
                            'NA',],
                    'datetime':[
                            'NA',],
                    'sugar':[
                            'NA',],
                    'species':[
                            'NA',],
                    'sap_wt':[
                            'NA',],} 
                
        )
    # Coerce the data into the types specified in the metadata 
    # Since date conversions are tricky, the coerced dates will go into a new column with _datetime appended
    # This new column is added to the dataframe but does not show up in automated summaries below. 
    dt2=dt2.assign(date_datetime=pd.to_datetime(dt2.date,errors='coerce'))  
    dt2.tree=dt2.tree.astype('category')  
    dt2.tap=dt2.tap.astype('category') 
    # Since date conversions are tricky, the coerced dates will go into a new column with _datetime appended
    # This new column is added to the dataframe but does not show up in automated summaries below. 
    dt2=dt2.assign(time_datetime=pd.to_datetime(dt2.time,errors='coerce')) 
    # Since date conversions are tricky, the coerced dates will go into a new column with _datetime appended
    # This new column is added to the dataframe but does not show up in automated summaries below. 
    dt2=dt2.assign(datetime_datetime=pd.to_datetime(dt2.datetime,errors='coerce')) 
    dt2.sugar=pd.to_numeric(dt2.sugar,errors='coerce')  
    dt2.species=dt2.species.astype('category') 
    dt2.sap_wt=pd.to_numeric(dt2.sap_wt,errors='coerce') 
        
        
    infile3  ="https://pasta.lternet.edu/package/data/eml/knb-lter-hfr/285/3/316c7943e741a678ecfd8495e24ec55a".strip() 
    infile3  = infile3.replace("https://","http://")
                    
    dt3 =pd.read_csv(infile3 
            ,skiprows=1
                ,sep=","  
            , names=[
                        "year",     
                        "tree_num",     
                        "first_sex",     
                        "flowering_intensity"    ]
            ,parse_dates=[
                            'year',
                    ] 
                ,na_values={
                    'year':[
                            'NA',],
                    'tree_num':[
                            'NA',],
                    'first_sex':[
                            'NA',],
                    'flowering_intensity':[
                            'NA',],} 
                
        )
    # Coerce the data into the types specified in the metadata 
    # Since date conversions are tricky, the coerced dates will go into a new column with _datetime appended
    # This new column is added to the dataframe but does not show up in automated summaries below. 
    dt3=dt3.assign(year_datetime=pd.to_datetime(dt3.year,errors='coerce'))  
    dt3.tree_num=dt3.tree_num.astype('category')  
    dt3.first_sex=dt3.first_sex.astype('category')  
    dt3.flowering_intensity=dt3.flowering_intensity.astype('category') 
        

    infile4  ="https://pasta.lternet.edu/package/data/eml/knb-lter-hfr/285/3/46badf5c80db2c78d2ecb5370747ca95".strip() 
    infile4  = infile4.replace("https://","http://")
                    
    dt4 =pd.read_csv(infile4 
            ,skiprows=1
                ,sep=","  
            , names=[
                        "date",     
                        "tree",     
                        "branch",     
                        "canopy_strata",     
                        "internode_year",     
                        "internode_num",     
                        "bud_num",     
                        "bud_pos",     
                        "num_male",     
                        "num_female",     
                        "num_unknown",     
                        "leaves",     
                        "num_leaves"    ]
            ,parse_dates=[
                            'date',
                    ] 
                ,na_values={
                    'date':[
                            'NA',],
                    'tree':[
                            'NA',],
                    'branch':[
                            'NA',],
                    'canopy_strata':[
                            'NA',],
                    'internode_year':[
                            'NA',],
                    'internode_num':[
                            'NA',],
                    'bud_num':[
                            'NA',],
                    'bud_pos':[
                            'NA',],
                    'num_male':[
                            'NA',],
                    'num_female':[
                            'NA',],
                    'num_unknown':[
                            'NA',],
                    'leaves':[
                            'NA',],
                    'num_leaves':[
                            'NA',],} 
                
        )
    # Coerce the data into the types specified in the metadata 
    # Since date conversions are tricky, the coerced dates will go into a new column with _datetime appended
    # This new column is added to the dataframe but does not show up in automated summaries below. 
    dt4=dt4.assign(date_datetime=pd.to_datetime(dt4.date,errors='coerce'))  
    dt4.tree=dt4.tree.astype('category')  
    dt4.branch=dt4.branch.astype('category')  
    dt4.canopy_strata=dt4.canopy_strata.astype('category') 
    dt4.internode_year=pd.to_numeric(dt4.internode_year,errors='coerce',downcast='integer')  
    dt4.internode_num=dt4.internode_num.astype('category')  
    dt4.bud_num=dt4.bud_num.astype('category')  
    dt4.bud_pos=dt4.bud_pos.astype('category') 
    dt4.num_male=pd.to_numeric(dt4.num_male,errors='coerce',downcast='integer') 
    dt4.num_female=pd.to_numeric(dt4.num_female,errors='coerce',downcast='integer') 
    dt4.num_unknown=pd.to_numeric(dt4.num_unknown,errors='coerce',downcast='integer')  
    dt4.leaves=dt4.leaves.astype('category') 
    dt4.num_leaves=pd.to_numeric(dt4.num_leaves,errors='coerce',downcast='integer') 
        

    infile5  ="https://pasta.lternet.edu/package/data/eml/knb-lter-hfr/285/3/150309317a285c00cd44058f65701af7".strip() 
    infile5  = infile5.replace("https://","http://")
                    
    dt5 =pd.read_csv(infile5 
            ,skiprows=1
                ,sep=","  
            , names=[
                        "year",     
                        "tree",     
                        "branch",     
                        "internode_year",     
                        "internode_num",     
                        "internode_length",     
                        "num_buds"    ]
            ,parse_dates=[
                            'year',
                    ] 
                ,na_values={
                    'year':[
                            'NA',],
                    'tree':[
                            'NA',],
                    'branch':[
                            'NA',],
                    'internode_year':[
                            'NA',],
                    'internode_num':[
                            'NA',],
                    'internode_length':[
                            'NA',],
                    'num_buds':[
                            'NA',],} 
                
        )
    # Coerce the data into the types specified in the metadata 
    # Since date conversions are tricky, the coerced dates will go into a new column with _datetime appended
    # This new column is added to the dataframe but does not show up in automated summaries below. 
    dt5=dt5.assign(year_datetime=pd.to_datetime(dt5.year,errors='coerce'))  
    dt5.tree=dt5.tree.astype('category')  
    dt5.branch=dt5.branch.astype('category') 
    dt5.internode_year=pd.to_numeric(dt5.internode_year,errors='coerce',downcast='integer')  
    dt5.internode_num=dt5.internode_num.astype('category') 
    dt5.internode_length=pd.to_numeric(dt5.internode_length,errors='coerce') 
    dt5.num_buds=pd.to_numeric(dt5.num_buds,errors='coerce',downcast='integer') 

    infile6  ="https://pasta.lternet.edu/package/data/eml/knb-lter-hfr/285/3/1ccb7c40e71c99f0746d6c51207530dc".strip() 
    infile6  = infile6.replace("https://","http://")
                    
    dt6 =pd.read_csv(infile6 
            ,skiprows=1
                ,sep=","  
            , names=[
                        "date",     
                        "tree",     
                        "branch",     
                        "treatment",     
                        "internode_year",     
                        "internode_num",     
                        "internode_length",     
                        "leaves",     
                        "leaves_missing",     
                        "leaf_area",     
                        "num_samaras"    ]
            ,parse_dates=[
                            'date',
                    ] 
                ,na_values={
                    'date':[
                            'NA',],
                    'tree':[
                            'NA',],
                    'branch':[
                            'NA',],
                    'treatment':[
                            'NA',],
                    'internode_year':[
                            'NA',],
                    'internode_num':[
                            'NA',],
                    'internode_length':[
                            'NA',],
                    'leaves':[
                            'NA',],
                    'leaves_missing':[
                            'NA',],
                    'leaf_area':[
                            'NA',],
                    'num_samaras':[
                            'NA',],} 
                
        )
    # Coerce the data into the types specified in the metadata 
    # Since date conversions are tricky, the coerced dates will go into a new column with _datetime appended
    # This new column is added to the dataframe but does not show up in automated summaries below. 
    dt6=dt6.assign(date_datetime=pd.to_datetime(dt6.date,errors='coerce'))  
    dt6.tree=dt6.tree.astype('category')  
    dt6.branch=dt6.branch.astype('category')  
    dt6.treatment=dt6.treatment.astype('category') 
    dt6.internode_year=pd.to_numeric(dt6.internode_year,errors='coerce',downcast='integer')  
    dt6.internode_num=dt6.internode_num.astype('category') 
    dt6.internode_length=pd.to_numeric(dt6.internode_length,errors='coerce',downcast='integer') 
    dt6.leaves=pd.to_numeric(dt6.leaves,errors='coerce',downcast='integer') 
    dt6.leaves_missing=pd.to_numeric(dt6.leaves_missing,errors='coerce',downcast='integer') 
    dt6.leaf_area=pd.to_numeric(dt6.leaf_area,errors='coerce') 
    dt6.num_samaras=pd.to_numeric(dt6.num_samaras,errors='coerce',downcast='integer') 
        

    infile7  ="https://pasta.lternet.edu/package/data/eml/knb-lter-hfr/285/3/53adcdd59f172b36e5d805789abd53b3".strip() 
    infile7  = infile7.replace("https://","http://")
                    
    dt7 =pd.read_csv(infile7 
            ,skiprows=1
                ,sep=","  
            , names=[
                        "year",     
                        "tree",     
                        "branch",     
                        "num_filled",     
                        "num_empty",     
                        "num_grub",     
                        "num_wrinkle",     
                        "num_small",     
                        "num_moldy",     
                        "num_exit"    ]
            ,parse_dates=[
                            'year',
                    ] 
                ,na_values={
                    'year':[
                            'NA',],
                    'tree':[
                            'NA',],
                    'branch':[
                            'NA',],
                    'num_filled':[
                            'NA',],
                    'num_empty':[
                            'NA',],
                    'num_grub':[
                            'NA',],
                    'num_wrinkle':[
                            'NA',],
                    'num_small':[
                            'NA',],
                    'num_moldy':[
                            'NA',],
                    'num_exit':[
                            'NA',],} 
                
        )
    # Coerce the data into the types specified in the metadata 
    # Since date conversions are tricky, the coerced dates will go into a new column with _datetime appended
    # This new column is added to the dataframe but does not show up in automated summaries below. 
    dt7=dt7.assign(year_datetime=pd.to_datetime(dt7.year,errors='coerce'))  
    dt7.tree=dt7.tree.astype('category')  
    dt7.branch=dt7.branch.astype('category') 
    dt7.num_filled=pd.to_numeric(dt7.num_filled,errors='coerce',downcast='integer') 
    dt7.num_empty=pd.to_numeric(dt7.num_empty,errors='coerce',downcast='integer') 
    dt7.num_grub=pd.to_numeric(dt7.num_grub,errors='coerce',downcast='integer') 
    dt7.num_wrinkle=pd.to_numeric(dt7.num_wrinkle,errors='coerce',downcast='integer') 
    dt7.num_small=pd.to_numeric(dt7.num_small,errors='coerce',downcast='integer') 
    dt7.num_moldy=pd.to_numeric(dt7.num_moldy,errors='coerce',downcast='integer') 
    dt7.num_exit=pd.to_numeric(dt7.num_exit,errors='coerce',downcast='integer') 
        
    infile8  ="https://pasta.lternet.edu/package/data/eml/knb-lter-hfr/285/3/a7652807020c976092ca8f762d1a9b97".strip() 
    infile8  = infile8.replace("https://","http://")
                    
    dt8 =pd.read_csv(infile8 
            ,skiprows=1
                ,sep=","  
            , names=[
                        "year",     
                        "tree",     
                        "branch",     
                        "treatment",     
                        "num_filled",     
                        "num_empty",     
                        "num_grub",     
                        "num_wrinkle",     
                        "num_small",     
                        "num_moldy",     
                        "num_exit",     
                        "num_missing"    ]
            ,parse_dates=[
                            'year',
                    ] 
                ,na_values={
                    'year':[
                            'NA',],
                    'tree':[
                            'NA',],
                    'branch':[
                            'NA',],
                    'treatment':[
                            'NA',],
                    'num_filled':[
                            'NA',],
                    'num_empty':[
                            'NA',],
                    'num_grub':[
                            'NA',],
                    'num_wrinkle':[
                            'NA',],
                    'num_small':[
                            'NA',],
                    'num_moldy':[
                            'NA',],
                    'num_exit':[
                            'NA',],
                    'num_missing':[
                            'NA',],} 
                
        )
    # Coerce the data into the types specified in the metadata 
    # Since date conversions are tricky, the coerced dates will go into a new column with _datetime appended
    # This new column is added to the dataframe but does not show up in automated summaries below. 
    dt8=dt8.assign(year_datetime=pd.to_datetime(dt8.year,errors='coerce'))  
    dt8.tree=dt8.tree.astype('category')  
    dt8.branch=dt8.branch.astype('category')  
    dt8.treatment=dt8.treatment.astype('category') 
    dt8.num_filled=pd.to_numeric(dt8.num_filled,errors='coerce',downcast='integer') 
    dt8.num_empty=pd.to_numeric(dt8.num_empty,errors='coerce',downcast='integer') 
    dt8.num_grub=pd.to_numeric(dt8.num_grub,errors='coerce',downcast='integer') 
    dt8.num_wrinkle=pd.to_numeric(dt8.num_wrinkle,errors='coerce',downcast='integer') 
    dt8.num_small=pd.to_numeric(dt8.num_small,errors='coerce',downcast='integer') 
    dt8.num_moldy=pd.to_numeric(dt8.num_moldy,errors='coerce',downcast='integer') 
    dt8.num_exit=pd.to_numeric(dt8.num_exit,errors='coerce',downcast='integer') 
    dt8.num_missing=pd.to_numeric(dt8.num_missing,errors='coerce',downcast='integer') 
        

    infile9  ="https://pasta.lternet.edu/package/data/eml/knb-lter-hfr/285/3/420b180629ecc044e44f03edcd687bcb".strip() 
    infile9  = infile9.replace("https://","http://")
                    
    dt9 =pd.read_csv(infile9 
            ,skiprows=1
                ,sep=","  
            , names=[
                        "tree",     
                        "date",     
                        "ec_count",     
                        "jr_count",     
                        "total_count"    ]
            ,parse_dates=[
                            'date',
                    ] 
                ,na_values={
                    'tree':[
                            'NA',],
                    'date':[
                            'NA',],
                    'ec_count':[
                            'NA',],
                    'jr_count':[
                            'NA',],
                    'total_count':[
                            'NA',],} 
                
        )
    # Coerce the data into the types specified in the metadata  
    dt9.tree=dt9.tree.astype('category') 
    # Since date conversions are tricky, the coerced dates will go into a new column with _datetime appended
    # This new column is added to the dataframe but does not show up in automated summaries below. 
    dt9=dt9.assign(date_datetime=pd.to_datetime(dt9.date,errors='coerce')) 
    dt9.ec_count=pd.to_numeric(dt9.ec_count,errors='coerce',downcast='integer') 
    dt9.jr_count=pd.to_numeric(dt9.jr_count,errors='coerce',downcast='integer') 
    dt9.total_count=pd.to_numeric(dt9.total_count,errors='coerce',downcast='integer') 
        

    infile10  ="https://pasta.lternet.edu/package/data/eml/knb-lter-hfr/285/3/8dd0ed00b10ed71a56ab6d35b5b434de".strip() 
    infile10  = infile10.replace("https://","http://")
                    
    dt10 =pd.read_csv(infile10 
            ,skiprows=1
                ,sep=","  
            , names=[
                        "year",     
                        "tree",     
                        "branch",     
                        "treatment",     
                        "num_flowering",     
                        "num_notflowering",     
                        "num_buds_w_samaras",     
                        "num_samaras",     
                        "num_petioles"    ]
            ,parse_dates=[
                            'year',
                    ] 
                ,na_values={
                    'year':[
                            'NA',],
                    'tree':[
                            'NA',],
                    'branch':[
                            'NA',],
                    'treatment':[
                            'NA',],
                    'num_flowering':[
                            'NA',],
                    'num_notflowering':[
                            'NA',],
                    'num_buds_w_samaras':[
                            'NA',],
                    'num_samaras':[
                            'NA',],
                    'num_petioles':[
                            'NA',],} 
                
        )
    # Coerce the data into the types specified in the metadata 
    # Since date conversions are tricky, the coerced dates will go into a new column with _datetime appended
    # This new column is added to the dataframe but does not show up in automated summaries below. 
    dt10=dt10.assign(year_datetime=pd.to_datetime(dt10.year,errors='coerce'))  
    dt10.tree=dt10.tree.astype('category')  
    dt10.branch=dt10.branch.astype('category')  
    dt10.treatment=dt10.treatment.astype('category') 
    dt10.num_flowering=pd.to_numeric(dt10.num_flowering,errors='coerce',downcast='integer') 
    dt10.num_notflowering=pd.to_numeric(dt10.num_notflowering,errors='coerce',downcast='integer') 
    dt10.num_buds_w_samaras=pd.to_numeric(dt10.num_buds_w_samaras,errors='coerce',downcast='integer') 
    dt10.num_samaras=pd.to_numeric(dt10.num_samaras,errors='coerce',downcast='integer') 
    dt10.num_petioles=pd.to_numeric(dt10.num_petioles,errors='coerce',downcast='integer') 
        
                    

    infile11  ="https://pasta.lternet.edu/package/data/eml/knb-lter-hfr/285/3/733485773d21cc6404b640de62c34e69".strip() 
    infile11  = infile11.replace("https://","http://")
                    
    dt11 =pd.read_csv(infile11 
            ,skiprows=1
                ,sep=","  
            , names=[
                        "tree",     
                        "branch",     
                        "treatment",     
                        "growth_year",     
                        "internode_length"    ]
            ,parse_dates=[
                            'growth_year',
                    ] 
                ,na_values={
                    'tree':[
                            'NA',],
                    'branch':[
                            'NA',],
                    'treatment':[
                            'NA',],
                    'growth_year':[
                            'NA',],
                    'internode_length':[
                            'NA',],} 
                
        )
    # Coerce the data into the types specified in the metadata  
    dt11.tree=dt11.tree.astype('category')  
    dt11.branch=dt11.branch.astype('category')  
    dt11.treatment=dt11.treatment.astype('category') 
    # Since date conversions are tricky, the coerced dates will go into a new column with _datetime appended
    # This new column is added to the dataframe but does not show up in automated summaries below. 
    dt11=dt11.assign(growth_year_datetime=pd.to_datetime(dt11.growth_year,errors='coerce')) 
    dt11.internode_length=pd.to_numeric(dt11.internode_length,errors='coerce') 
        
                

    infile12  ="https://pasta.lternet.edu/package/data/eml/knb-lter-hfr/285/3/deac87be90dc1f4f8ad11e01e0cceca4".strip() 
    infile12  = infile12.replace("https://","http://")
                    
    dt12 =pd.read_csv(infile12 
            ,skiprows=1
                ,sep=","  
            , names=[
                        "year",     
                        "tree",     
                        "branch",     
                        "treatment",     
                        "num_flowering",     
                        "num_notflowering",     
                        "num_buds_w_samaras",     
                        "num_samaras",     
                        "num_petioles"    ]
            ,parse_dates=[
                            'year',
                    ] 
                ,na_values={
                    'year':[
                            'NA',],
                    'tree':[
                            'NA',],
                    'branch':[
                            'NA',],
                    'treatment':[
                            'NA',],
                    'num_flowering':[
                            'NA',],
                    'num_notflowering':[
                            'NA',],
                    'num_buds_w_samaras':[
                            'NA',],
                    'num_samaras':[
                            'NA',],
                    'num_petioles':[
                            'NA',],} 
                
        )
    # Coerce the data into the types specified in the metadata 
    # Since date conversions are tricky, the coerced dates will go into a new column with _datetime appended
    # This new column is added to the dataframe but does not show up in automated summaries below. 
    dt12=dt12.assign(year_datetime=pd.to_datetime(dt12.year,errors='coerce'))  
    dt12.tree=dt12.tree.astype('category')  
    dt12.branch=dt12.branch.astype('category')  
    dt12.treatment=dt12.treatment.astype('category') 
    dt12.num_flowering=pd.to_numeric(dt12.num_flowering,errors='coerce',downcast='integer') 
    dt12.num_notflowering=pd.to_numeric(dt12.num_notflowering,errors='coerce',downcast='integer') 
    dt12.num_buds_w_samaras=pd.to_numeric(dt12.num_buds_w_samaras,errors='coerce',downcast='integer') 
    dt12.num_samaras=pd.to_numeric(dt12.num_samaras,errors='coerce',downcast='integer') 
    dt12.num_petioles=pd.to_numeric(dt12.num_petioles,errors='coerce',downcast='integer') 
        
            

    infile13  ="https://pasta.lternet.edu/package/data/eml/knb-lter-hfr/285/3/e8f6a405cd20955005b066b524ed09c3".strip() 
    infile13  = infile13.replace("https://","http://")
                    
    dt13 =pd.read_csv(infile13 
            ,skiprows=1
                ,sep=","  
            , names=[
                        "tree",     
                        "branch",     
                        "treatment",     
                        "growth_year",     
                        "internode_length"    ]
            ,parse_dates=[
                            'growth_year',
                    ] 
                ,na_values={
                    'tree':[
                            'NA',],
                    'branch':[
                            'NA',],
                    'treatment':[
                            'NA',],
                    'growth_year':[
                            'NA',],
                    'internode_length':[
                            'NA',],} 
                
        )
    # Coerce the data into the types specified in the metadata  
    dt13.tree=dt13.tree.astype('category')  
    dt13.branch=dt13.branch.astype('category')  
    dt13.treatment=dt13.treatment.astype('category') 
    # Since date conversions are tricky, the coerced dates will go into a new column with _datetime appended
    # This new column is added to the dataframe but does not show up in automated summaries below. 
    dt13=dt13.assign(growth_year_datetime=pd.to_datetime(dt13.growth_year,errors='coerce')) 
    dt13.internode_length=pd.to_numeric(dt13.internode_length,errors='coerce') 
        
                

    infile14  ="https://pasta.lternet.edu/package/data/eml/knb-lter-hfr/285/3/bd5c44a085ab8b90ce758e1d1f1419e2".strip() 
    infile14  = infile14.replace("https://","http://")
                    
    dt14 =pd.read_csv(infile14 
            ,skiprows=1
                ,sep=","  
            , names=[
                        "year",     
                        "tree",     
                        "branch",     
                        "num_flowering",     
                        "num_notflowering",     
                        "num_buds_w_samaras",     
                        "num_samaras",     
                        "num_petioles"    ]
            ,parse_dates=[
                            'year',
                    ] 
                ,na_values={
                    'year':[
                            'NA',],
                    'tree':[
                            'NA',],
                    'branch':[
                            'NA',],
                    'num_flowering':[
                            'NA',],
                    'num_notflowering':[
                            'NA',],
                    'num_buds_w_samaras':[
                            'NA',],
                    'num_samaras':[
                            'NA',],
                    'num_petioles':[
                            'NA',],} 
                
        )
    # Coerce the data into the types specified in the metadata 
    # Since date conversions are tricky, the coerced dates will go into a new column with _datetime appended
    # This new column is added to the dataframe but does not show up in automated summaries below. 
    dt14=dt14.assign(year_datetime=pd.to_datetime(dt14.year,errors='coerce'))  
    dt14.tree=dt14.tree.astype('category')  
    dt14.branch=dt14.branch.astype('category') 
    dt14.num_flowering=pd.to_numeric(dt14.num_flowering,errors='coerce',downcast='integer') 
    dt14.num_notflowering=pd.to_numeric(dt14.num_notflowering,errors='coerce',downcast='integer') 
    dt14.num_buds_w_samaras=pd.to_numeric(dt14.num_buds_w_samaras,errors='coerce',downcast='integer') 
    dt14.num_samaras=pd.to_numeric(dt14.num_samaras,errors='coerce',downcast='integer') 
    dt14.num_petioles=pd.to_numeric(dt14.num_petioles,errors='coerce',downcast='integer') 
        


    dt_nums = [str(x) for x in list(range(1,15))]

    for dt_num in dt_nums:
        locals()['dt' + dt_num].to_pickle(os.path.join(processed_path, 'dt' + dt_num.zfill(2)))
        
        
    # Download raw data and save as csv
    for dt_num in dt_nums:
        raw_data = requests.get(locals()['infile' + dt_num])
        with open(os.path.join(raw_path, 'rapp2016_raw' + dt_num.zfill(2) + '.csv'), 'wb+') as data_file:
            data_file.write(raw_data.content)

    # Download metadata and save
    metadata_url = 'https://pasta.lternet.edu/package/metadata/eml/knb-lter-hfr/285/3'
    metadata_xml = requests.get(metadata_url)

    with open(os.path.join(raw_path, 'rapp2016_metadata.xml'), 'w+') as metadata_file:
        metadata_file.write(metadata_xml.text)

if __name__ == "__main__":
    main()
