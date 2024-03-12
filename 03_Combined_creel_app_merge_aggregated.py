# Import necessary libraries
import os
import vaex
import pyarrow.parquet as pq
import datetime
import pandas as pd
from datetime import datetime
import pathlib
import os, psutil  # to get to know required memory
import pyarrow as pa
import h5py
import matplotlib.pyplot as plt
import seaborn as sns

# Define file paths
inpath = pathlib.Path(r"C:\Azar_Drive\relationships-between-variables1\01_preprocessing\data")
outpath = pathlib.Path(r"C:\Azar_Drive\relationships-between-variables1\01_preprocessing\results")

# Read creel data and anglers atlas data
Creel_data_daily_agg = pd.read_csv(outpath / "01-Creel_app_data_daily_aggregated.csv")
Environment_App_Bow_Oldman_2018 = vaex.open(inpath / "Environment_App_Bow_Oldman_2018.hdf5")

# Explore the data
print(Environment_App_Bow_Oldman_2018["waterbody_id"].unique())
print(Environment_App_Bow_Oldman_2018.head())

# Clean and preprocess data
Environment_App_Bow_Oldman_2018["hours_out"] = Environment_App_Bow_Oldman_2018["hours_out"].fillna(0)

# Rename columns
Creel_data_daily_agg.rename(columns={"Date_FJ": "date"}, inplace=True)
Creel_data_daily_agg.rename(columns={'creel_fishing_duration': 'creel_fishing_duration'}, inplace=True)

# Convert date format
Environment_App_Bow_Oldman_2018 = Environment_App_Bow_Oldman_2018.to_pandas_df()
Environment_App_Bow_Oldman_2018['date'] = Environment_App_Bow_Oldman_2018['date'].dt.strftime('%Y-%m-%d')

# Exclude specific waterbodies
Environment_App_Bow_Oldman_2018 = Environment_App_Bow_Oldman_2018[Environment_App_Bow_Oldman_2018["waterbody_id"] != 87177]

# Check for duplicates
duplicates_AA = Environment_App_Bow_Oldman_2018.duplicated(subset=['waterbody_id', 'date'], keep=False)
print(duplicates_AA)

# Drop duplicates
AA_data = Environment_App_Bow_Oldman_2018.drop_duplicates(subset=['waterbody_id', 'date'], keep='last')

# Initialize and define columns
AA_data["app_fishing_duration"] = 0
AA_data["app_fishing_duration"] = AA_data["hours_out"] / AA_data["total_trips"]

# Merge datasets
Combined_Creel_app_data_daily_agg_merged = pd.merge(Creel_data_daily_agg, AA_data, on=["date"], how='inner')

# Fill null values with zero
Combined_Creel_app_data_daily_agg_merged.fillna(0, inplace=True)

# Rename columns
Combined_Creel_app_data_daily_agg_merged.rename(columns={'hours_out': 'app_hours_out',
                                                        'total_trips': 'app_number_of_trips',
                                                        'catch_rate_div_avg': 'app_catch_rate',
                                                        'unique_page_views': 'webpage_views',
                                                        'wind_speed_at_2_meters': 'wind_speed'}, inplace=True)

# Fill missing values with zero
Combined_Creel_app_data_daily_agg_merged["app_number_of_trips"] = Combined_Creel_app_data_daily_agg_merged["app_number_of_trips"].fillna(0)
Combined_Creel_app_data_daily_agg_merged["webpage_views"] = Combined_Creel_app_data_daily_agg_merged["webpage_views"].fillna(0)
Combined_Creel_app_data_daily_agg_merged["app_catch_rate"] = Combined_Creel_app_data_daily_agg_merged["app_catch_rate"].fillna(0)
Combined_Creel_app_data_daily_agg_merged["creel_catch_rate"] = Combined_Creel_app_data_daily_agg_merged["creel_catch_rate"].fillna(0)
Combined_Creel_app_data_daily_agg_merged["app_fishing_duration"] = Combined_Creel_app_data_daily_agg_merged["app_fishing_duration"].fillna(0)

# Convert boolean variables to numeric
Combined_Creel_app_data_daily_agg_merged['has_whirling_disease'] = Combined_Creel_app_data_daily_agg_merged['has_whirling_disease'].map({True: 1, False: 0})
Combined_Creel_app_data_daily_agg_merged['is_weekend'] = Combined_Creel_app_data_daily_agg_merged['is_weekend'].map({True: 1, False: 0})

# Aggregate data based on date
Combined_Creel_app_data_daily_agg_merged = Combined_Creel_app_data_daily_agg_merged.groupby(['date'], as_index=False).agg({
    "date": "first",
    "creel_catch_rate": "mean",
    "app_catch_rate": "mean",
    "creel_fishing_duration": "mean",
    "app_fishing_duration": "mean",
    "air_temperature": "mean",
    "total_precipitation": "mean",
    "relative_humidity": "mean",
    "solar_radiation": "mean",
    "wind_speed": "mean",
    "degree_days": "mean",
    "webpage_views": "mean",
    "is_weekend": "first"
})

# Save aggregated data
file_name = "Combined_Creel_app_data_daily_agg_merged.csv"
Combined_Creel_app_data_daily_agg_merged.to_csv(outpath / file_name)

# Analyze and visualize data
num_rows = Combined_Creel_app_data_daily_agg_merged.shape[0]

# Calculate zero percentages
zero_count_app_fishing_duration = (Combined_Creel_app_data_daily_agg_merged["app_fishing_duration"] == 0).sum()
zero_count_app_catch_rate = (Combined_Creel_app_data_daily_agg_merged["app_catch_rate"] == 0).sum()

print(zero_count_app_fishing_duration / num_rows)
print(zero_count_app_catch_rate / num_rows)

# Plot histograms
plt.hist(Combined_Creel_app_data_daily_agg_merged["app_catch_rate"], bins=10)
plt.xlabel("App catch rate")
plt.ylabel("Frequency")
plt.savefig("Combined_app_catch_rate.pdf", dpi=300, bbox_inches='tight')
plt.show()

plt.hist(Combined_Creel_app_data_daily_agg_merged["app_fishing_duration"], bins=10)
plt.xlabel("App fishing duration")
plt.ylabel("Frequency")
plt.savefig("Combined_app_fishing_duration.pdf", dpi=300, bbox_inches='tight')
plt.show()

plt.hist(Combined_Creel_app_data_daily_agg_merged["creel_catch_rate"], bins=10)
plt.xlabel("Creel catch rate")
plt.ylabel("Frequency")
plt.savefig("Combined_creel_catch_rate.pdf", dpi=300, bbox_inches='tight')
plt.show()

plt.hist(Combined_Creel_app_data_daily_agg_merged["creel_fishing_duration"], bins=10)
plt.xlabel("Creel fishing duration")
plt.ylabel("Frequency")
plt.savefig("Combined_creel_fishing_duration.pdf", dpi=300, bbox_inches='tight')
plt.show()

plt.hist(Combined_Creel_app_data_daily_agg_merged["air_temperature"], bins=10)
plt.xlabel("Air temperature")
plt.ylabel("Frequency")
plt.savefig("Combined_air_temperature.pdf", dpi=300, bbox_inches='tight')
plt.show()

plt.hist(Combined_Creel_app_data_daily_agg_merged["degree_days"], bins=10)
plt.xlabel("Degree days")
plt.ylabel("Frequency")
plt.savefig("Combined_degree_days.pdf", dpi=300, bbox_inches='tight')
plt.show()

plt.hist(Combined_Creel_app_data_daily_agg_merged["relative_humidity"], bins=10)
plt.xlabel("Relative humidity")
plt.ylabel("Frequency")
plt.savefig("Combined_relative_humidity.pdf", dpi=300, bbox_inches='tight')
plt.show()

plt.hist(Combined_Creel_app_data_daily_agg_merged["solar_radiation"], bins=10)
plt.xlabel("Solar radiation")
plt.ylabel("Frequency")
plt.savefig("Combined_solar_radiation.pdf", dpi=300, bbox_inches='tight')
plt.show()

plt.hist(Combined_Creel_app_data_daily_agg_merged["total_precipitation"], bins=10)
plt.xlabel("Total precipitation")
plt.ylabel("Frequency")
plt.savefig("Combined_total_precipitation.pdf", dpi=300, bbox_inches='tight')
plt.show()

plt.hist(Combined_Creel_app_data_daily_agg_merged["wind_speed"], bins=10)
plt.xlabel("Wind speed")
plt.ylabel("Frequency")
plt.savefig("Combined_wind_speed.pdf", dpi=300, bbox_inches='tight')
plt.show()

plt.hist(Combined_Creel_app_data_daily_agg_merged["webpage_views"], bins=10)
plt.xlabel("Webpage views")
plt.ylabel("Frequency")
plt.savefig("Combined_webpage_views.pdf", dpi=300, bbox_inches='tight')
plt.show()
