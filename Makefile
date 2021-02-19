# Makefile for pipeline including downloading data, cleaning, analyzing.
#
# author: Steffen Pentelow
# date: 2021-02-18
#
# Example usage:
# make all
# make download_data
# make clean

all :

download_data : 
	python src/data_download_weather.py
	python src/data_download_sap.py

data/raw/HF_weather/hf001-08-hourly-m.csv : src/data_download_weather.py
	python src/data_download_weather.py

data/processed/stinson2019/stinson2019_df : src/data_download_sap.py
	python src/data_download_sap.py

clean :
	rm -rf data/raw
	rm -rf data/processed
# 	rm data/raw/aliens.csv
# 	rm data/processed/aliens.csv
# 	rm -f results/*.png
# 	rm -f results/*.rds
# 	rm doc/*.pdf