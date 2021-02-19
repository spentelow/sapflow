# Makefile for pipeline including downloading data, cleaning, analyzing.
#
# author: Steffen Pentelow
# date: 2021-02-18
#
# Example usage:
# make all
# make download_data
# make clean

all : data/processed/stinson2019/sap_sugar_weekly_summary data/processed/HF_weather/gdd_frthw

download_data : 
	python src/data_download_weather.py
	python src/data_download_sap.py

data/raw/HF_weather/hf001-08-hourly-m.csv : src/data_download_weather.py
	python src/data_download_weather.py

data/processed/stinson2019/stinson2019_df : src/data_download_sap.py
	python src/data_download_sap.py

data/processed/stinson2019/sap_sugar_weekly_summary : src/create_weekly_summaries.py data/processed/stinson2019/stinson2019_df
	python src/create_weekly_summaries.py

data/processed/HF_weather/gdd_frthw : src/create_GDD-frthw.py data/raw/HF_weather/hf001-08-hourly-m.csv
	python src/create_GDD-frthw.py

clean :
	rm -rf data/raw
	rm -rf data/processed