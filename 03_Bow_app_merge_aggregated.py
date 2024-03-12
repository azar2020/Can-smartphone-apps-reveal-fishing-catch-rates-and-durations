

# Import required packages
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
import seaborn as sns
import matplotlib.pyplot as plt


# Define onpath and outpath
inpath = pathlib.Path(r"C:\Azar_Drive\relationships-between-variables1\01_preprocessing\data")
outpath = pathlib.Path(r"C:\Azar_Drive\relationships-between-variables1\01_preprocessing\results")

# Read creel data and anglers atlas data
Creel_data_daily_agg = pd.read_csv(outpath / "01-Bow_creel_agg.csv")
Environment_App_Bow_Oldman_2018 = vaex.open(inpath / "Environment_App_Bow_Oldman_2018.hdf5")


Environment_App_Bow_Oldman_2018.column_names


# Fill null values with zero
Environment_App_Bow_Oldman_2018["hours_out"] = Environment_App_Bow_Oldman_2018["hours_out"].fillna(0)


# Print the waterbodies with ids=87178 & 87177, which are the sections of the Bow River
Environment_App_Bow_Oldman_2018[(Environment_App_Bow_Oldman_2018["waterbody_id"] == 87178) &
                                (Environment_App_Bow_Oldman_2018["waterbody_id"] == 87177)]


# Rename "Date_FJ" column to "date"
Creel_data_daily_agg.rename(columns={"Date_FJ": "date"}, inplace=True)


len(Environment_App_Bow_Oldman_2018["date"].unique())


# Convert Vaex DataFrame to Pandas DataFrame
Environment_App_Bow_Oldman_2018 = Environment_App_Bow_Oldman_2018.to_pandas_df()
Environment_App_Bow_Oldman_2018['date'] = Environment_App_Bow_Oldman_2018['date'].dt.strftime('%Y-%m-%d')


# Check duplicated data
duplicates_AA = Environment_App_Bow_Oldman_2018.duplicated(subset=['waterbody_id', 'date'], keep=False)
duplicates_AA


# Compute the number of duplicated data
sum(duplicates_AA)


# Drop the duplicated data
AA_data = Environment_App_Bow_Oldman_2018.drop_duplicates(subset=['waterbody_id', 'date'], keep='last')


AA_data["total_trips"].describe()


# Check again
duplicates_AA_check_again = AA_data.duplicated(subset=['waterbody_id', 'date'], keep=False)
duplicates_AA_check_again


sum(duplicates_AA_check_again)


# Initialize the column "app fishing duration" with zero
AA_data["app_fishing_duration"] = 0


# Define the column by dividing the time spent on fishing by the total trips
AA_data["app_fishing_duration"] = AA_data["hours_out"] / AA_data["total_trips"]


Creel_data_daily_agg.columns


# Select only the id 87178, as for the id 87177, we don't have data in our study time period
Creel_data_daily_agg["waterbody_id"] = 87178


Creel_data_daily_agg


# Merge creel data and app data based on waterbody id and date
Bow_Creel_app_data_daily_agg_merged = pd.merge(
    Creel_data_daily_agg, AA_data, on=["waterbody_id", "date"], how='inner')


Bow_Creel_app_data_daily_agg_merged


# Fill the null values with zero
Bow_Creel_app_data_daily_agg_merged.fillna(0)

Bow_Creel_app_data_daily_agg_merged["total_trips"].describe()


# Rename "hours_out" to "app_hours_out"
Bow_Creel_app_data_daily_agg_merged = Bow_Creel_app_data_daily_agg_merged.rename(
    columns={'hours_out': 'app_hours_out'})
Bow_Creel_app_data_daily_agg_merged = Bow_Creel_app_data_daily_agg_merged.rename(
    columns={'total_trips': 'app_number_of_trips'})
Bow_Creel_app_data_daily_agg_merged = Bow_Creel_app_data_daily_agg_merged.rename(
    columns={'catch_rate_div_avg': 'app_catch_rate'})
Bow_Creel_app_data_daily_agg_merged = Bow_Creel_app_data_daily_agg_merged.rename(
    columns={'unique_page_views': 'webpage_views'})
Bow_Creel_app_data_daily_agg_merged = Bow_Creel_app_data_daily_agg_merged.rename(
    columns={'wind_speed_at_2_meters': 'wind_speed'})


# Fill null values with zero
Bow_Creel_app_data_daily_agg_merged["app_number_of_trips"] = Bow_Creel_app_data_daily_agg_merged[
    "app_number_of_trips"].fillna(0)
Bow_Creel_app_data_daily_agg_merged["webpage_views"] = Bow_Creel_app_data_daily_agg_merged[
    "webpage_views"].fillna(0)
Bow_Creel_app_data_daily_agg_merged["app_catch_rate"] = Bow_Creel_app_data_daily_agg_merged[
    "app_catch_rate"].fillna(0)
Bow_Creel_app_data_daily_agg_merged["creel_catch_rate"] = Bow_Creel_app_data_daily_agg_merged[
    "creel_catch_rate"].fillna(0)
Bow_Creel_app_data_daily_agg_merged["app_fishing_duration"] = Bow_Creel_app_data_daily_agg_merged[
    "app_fishing_duration"].fillna(0)


# Check the number of waterbodies with id 87178
len(Bow_Creel_app_data_daily_agg_merged[Bow_Creel_app_data_daily_agg_merged["waterbody_id"] == 87178])


# Check the number of waterbodies with id 87177
len(Bow_Creel_app_data_daily_agg_merged[Bow_Creel_app_data_daily_agg_merged["waterbody_id"] == 87177])


len(Bow_Creel_app_data_daily_agg_merged["date"].unique())


# Change the value of boolean variables to numeric
Bow_Creel_app_data_daily_agg_merged['is_weekend'] = Bow_Creel_app_data_daily_agg_merged['is_weekend'].map({True: 1, False: 0})


Bow_Creel_app_data_daily_agg_merged.columns


# Aggregate creel data based on date and waterbody id
Bow_Creel_app_data_daily_agg_merged = Bow_Creel_app_data_daily_agg_merged.groupby(['date'], as_index=False).agg({
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
    "is_weekend": "first",
})


Bow_Creel_app_data_daily_agg_merged["date"].unique()


# Bow_Creel_app_data_daily_agg_merged["app_number_of_trips"].describe()


# Save daily aggregated merged creel data and app data
file_name = "Bow_Creel_app_data_daily_agg_merged.csv"
Bow_Creel_app_data_daily_agg_merged.to_csv(outpath / file_name)


# Find Pearson correlation matrix before discretization
corr_matrix = Bow_Creel_app_data_daily_agg_merged.corr()
plt.figure(figsize=(8, 6))  # Adjust the figure size as needed
sns.heatmap(corr_matrix, cmap='PuBu', annot=True, annot_kws={'size': 8})  # Adjust the 'size' value to change font size
plt.savefig('corr_matrix_before_discretization.pdf', format='pdf')
plt.show()


# Read the data after discretization to find the Pearson correlation after discretization
Bow_Discretized_data = pd.read_csv(outpath / "Discretized_Bow_Creel_app_data_daily_agg.csv")
Bow_Discretized_data = Bow_Discretized_data[["creel_catch_rate", "app_catch_rate",
                                             "creel_fishing_duration", "app_fishing_duration",
                                             "air_temperature", "total_precipitation",
                                             "relative_humidity", "solar_radiation",
                                             "wind_speed", "degree_days",
                                             "webpage_views", "is_weekend"]]


# Find Pearson correlation matrix after discretization
Bow_corr_matrix_discret = Bow_Discretized_data.corr()
plt.figure(figsize=(8, 6))  # Adjust the figure size as needed
sns.heatmap(Bow_corr_matrix_discret, cmap='PuBu', annot=True, annot_kws={'size': 8})  # Adjust the 'size' value to change font size
plt.savefig('corr_matrix_discrete.pdf', format='pdf')
plt.show()
