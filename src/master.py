"""
Run all required scripts for sapflow analysis in correct order

author: Steffen Pentelow
date: 2021-03-25

Usage: src/master.py [--downloads=<downloads>] [--tables=<tables>] [--derived=<derived>]

Options:
--downloads=<downloads>     Run data download scripts (True/False) [default: True]
--tables=<tables>       Run table generation scripts (True/False) [default: True]
--derived=<derived>     Run derived parameter table scripts (True/False) [default: True]
"""

from docopt import docopt

def main(downloads = True, tables = True, derived = True):

    if downloads: 
        run_downloads()
    elif tables: 
        # Only run run_tables() explicity if run_downloads() has not run since
        # run_tables() is run within run_downloads()
        run_tables()
    
    if derived: run_derived()
    
    return

def run_downloads():
    """Runs data download scripts
    """

    import download_sap
    import download_weather

    
    print('Downloading sap measurement data.')
    download_sap.main()  # Download raw sap data from databases from 3 studies
    
    # Tables must be created from sap measurement data prior to downloading 
    # weather data because they specify which weather stations to download data
    # for.
    run_tables()
    
    print('Downloading weather data.')
    download_weather.main()  # Download weather data

    return

def run_tables():
    """Creates normalized tables for all data.
    """
    
    import create_location_tables
    import create_meas_tables
    
    print('Creating normalized sap measurement tables.')
    create_meas_tables.main() # Create normalized weather tables
    
    print('Creating sap collection site / NOAA weather station association tables.')
    # Create tables with information on sap collection location and 
    # closest/associated NOAA weather station.
    create_location_tables.main()
    
    return

def run_derived():
    """Runs scripts which generate derived parameters
    """
    import create_GDD_frthw
    import create_weekly_summaries

    print('Calculating weekly sap flow values for all sites.')
    # Create weekly summaries and normalized tables for sap data
    create_weekly_summaries.main()
    
    print('Calculating cumulative growing degree days and cumulative freeze-thaw cycle values.')
    # Generate tables with growing degree days and freeze-thaw cycle 
    # information.
    create_GDD_frthw.main()

    return


if __name__ == "__main__":
    
    opt = docopt(__doc__)
    downloads = bool(opt['--downloads'])
    tables = bool(opt['--tables'])
    derived = bool(opt['--derived'])
    main(downloads = downloads, tables = tables, derived = derived)
