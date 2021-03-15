"""
Download weather data from various weather stations.

author: Steffen Pentelow
date: 2021-02-18

Usage: python src/data_download_weather.py
"""

import os
import requests

def main():
    hf001()

    return

def hf001():
    """ Downloads 15-min metric weather files from Fisher Meteorological Station at Harvard Forest since 2005

    ## Data website ##
    https://harvardforest1.fas.harvard.edu/exist/apps/datasets/showData.html?id=HF001

    ## Citation ##
    Boose E. 2018. Fisher Meteorological Station at Harvard Forest since 2001. Harvard Forest Data Archive: HF001.
    """

    raw_path = "data/raw/HF_weather"

    if not os.path.exists(raw_path):
        os.makedirs(raw_path)
        
    HF_weather_url = 'https://harvardforest.fas.harvard.edu/data/p00/hf001/hf001-10-15min-m.csv'
    HF_weather_csv = requests.get(HF_weather_url)

    with open(raw_path + '/hf001-08-hourly-m.csv', 'wb+') as raw_data:
        raw_data.write(HF_weather_csv.content)
            
    # Download metadata and save
    metadata_url = 'http://harvardforest.fas.harvard.edu/data/eml/hf001.xml'
    metadata_xml = requests.get(metadata_url)

    with open(raw_path + '/HF_metadata.xml', 'w+') as metadata_file:
        metadata_file.write(metadata_xml.text)
    
    return

if __name__ == "__main__":
    main()