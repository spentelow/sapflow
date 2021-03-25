"""
Run all required scripts for sapflow analysis in correct order

author: Steffen Pentelow
date: 2021-03-25

Usage: src/master.py
"""

def main():

    import data_download_sap
    import create_weekly_summaries
    
    import sites_stations
    import weather
    import data_download_weather
    
    import create_GDD_frthw

    data_download_sap.main()  # Download raw sap data from databases from 3 studies
    create_weekly_summaries.main()  # Create weekly summaries and normalized tables for data from Stinson2019 study
    
    sites_stations.main()
    weather.main()
    data_download_weather.main()

    create_GDD_frthw.main()


    return



if __name__ == "__main__":
    main()
