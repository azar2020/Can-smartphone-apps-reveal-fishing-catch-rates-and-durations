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
import seaborn as sns
import matplotlib.pyplot as plt

# Define onpath and outpath
inpath = pathlib.Path(r"C:\Azar_Drive\relationships-between-variables1\01_preprocessing\data")
outpath = pathlib.Path(r"C:\Azar_Drive\relationships-between-variables1\01_preprocessing\results")

# Read creel data and anglers atlas data
Creel_data_daily_agg = pd.read_csv(outpath / "01-Oldman_creel_agg.csv")
Environment_App_Bow_Oldman_2018 = vaex.open(inpath / "Environment_App_Bow_Oldman_2018-8.hdf5")

Environment_App_Bow_Oldman_2018.column_names

# Print the unique waterbody ids
Environment_App_Bow_Oldman_2018["waterbody_id"].unique()

# Fill null values with zero
Environment_App_Bow_Oldman_2018["hours_out"] = Environment_App_Bow_Oldman_2018["hours_out"].fillna(0)

# Rename "Date_FJ" column to "date"
Creel_data_daily_agg.rename(columns={"Date_FJ": "date"}, inplace=True)
len(Creel_data_daily_agg["date"].unique())

# Change date format
# Convert Vaex DataFrame to Pandas DataFrame
Environment_App_Bow_Oldman_2018 = Environment_App_Bow_Oldman_2018.to_pandas_df()
# Convert the date column to datetime format using Pandas
Environment_App_Bow_Oldman_2018['date'] = Environment_App_Bow_Oldman_2018['date'].dt.strftime('%Y-%m-%d')

# Check duplicated data
duplicates_AA = Environment_App_Bow_Oldman_2018.duplicated(subset=['waterbody_id', 'date'], keep=False)
duplicates_AA

# Compute the number of duplicated data
sum(duplicates_AA)

# Drop the duplicated data
AA_data = Environment_App_Bow_Oldman_2018.drop_duplicates(subset=['waterbody_id', 'date'], keep='last')

# Check again
duplicates_AA_check_again = AA_data.duplicated(subset=['waterbody_id', 'date'], keep=False)
duplicates_AA_check_again

sum(duplicates_AA_check_again)

# Initialize the column "app fishing duration" with zero
AA_data["app_fishing_duration"] = 0

# Define the column by dividing the time spent on fishing by the total trips
AA_data["app_fishing_duration"] = AA_data["hours_out"] / AA_data["total_trips"]

# Merge creel data and app data based on waterbody id and date
Oldman_Creel_app_data_daily_agg_merged = pd.merge(
    Creel_data_daily_agg, AA_data, on=["waterbody_id", "date"], how='inner')

# Fill the num values with zero
Oldman_Creel_app_data_daily_agg_merged.fillna(0)

# Rename variables
Oldman_Creel_app_data_daily_agg_merged = Oldman_Creel_app_data_daily_agg_merged.rename(
    columns={'hours_out': 'app_hours_out'})
Oldman_Creel_app_data_daily_agg_merged = Oldman_Creel_app_data_daily_agg_merged.rename(
    columns={'total_trips': 'app_number_of_trips'})
Oldman_Creel_app_data_daily_agg_merged = Oldman_Creel_app_data_daily_agg_merged.rename(
    columns={'catch_rate_div_avg': 'app_catch_rate'})
Oldman_Creel_app_data_daily_agg_merged = Oldman_Creel_app_data_daily_agg_merged.rename(
    columns={'unique_page_views': 'webpage_views'})
Oldman_Creel_app_data_daily_agg_merged = Oldman_Creel_app_data_daily_agg_merged.rename(
    columns={'wind_speed_at_2_meters': 'wind_speed'})

Oldman_Creel_app_data_daily_agg_merged["creel_fishing_duration"].describe()

Oldman_Creel_app_data_daily_agg_merged["app_number_of_trips"] = Oldman_Creel_app_data_daily_agg_merged[
    "app_number_of_trips"].fillna(0)
Oldman_Creel_app_data_daily_agg_merged["webpage_views"] = Oldman_Creel_app_data_daily_agg_merged[
    "webpage_views"].fillna(0)
Oldman_Creel_app_data_daily_agg_merged["app_catch_rate"] = Oldman_Creel_app_data_daily_agg_merged[
    "app_catch_rate"].fillna(0)
Oldman_Creel_app_data_daily_agg_merged["creel_catch_rate"] = Oldman_Creel_app_data_daily_agg_merged[
    "creel_catch_rate"].fillna(0)
Oldman_Creel_app_data_daily_agg_merged["creel_fishing_duration"] = Oldman_Creel_app_data_daily_agg_merged[
    "creel_fishing_duration"].fillna(0)
Oldman_Creel_app_data_daily_agg_merged["app_fishing_duration"] = Oldman_Creel_app_data_daily_agg_merged[
    "app_fishing_duration"].fillna(0)

# Fill the null values with zero
Oldman_Creel_app_data_daily_agg_merged.fillna(0)

Oldman_Creel_app_data_daily_agg_merged.info()

# Check the number of unique dates
len(Oldman_Creel_app_data_daily_agg_merged["date"].unique())

# Change the value of boolean variables to numeric
Oldman_Creel_app_data_daily_agg_merged['is_weekend'] = Oldman_Creel_app_data_daily_agg_merged['is_weekend'].map(
    {True: 1, False: 0})

Oldman_Creel_app_data_daily_agg_merged.columns

# Aggregate creel data based on date and waterbody id
Oldman_Creel_app_data_daily_agg_merged = Oldman_Creel_app_data_daily_agg_merged.groupby(
    ['date'], as_index=False).agg({"date": "first",
                                   "creel_catch_rate": "mean",
                                   "app_catch_rate": "mean",
                                   "creel_fishing_duration": "mean",
                                   "app_fishing_duration": "mean",
                                   "air_temperature": "mean",
                                   "total_precipitation": "mean",
                                   "relative_humidity": "mean", "solar_radiation": "mean",
                                   "wind_speed": "mean", "degree_days": "mean",
                                   "webpage_views": "mean", "is_weekend": "first",
                                   })

Oldman_Creel_app_data_daily_agg_merged.info()

# Save daily aggregated merged creel data and app data
file_name = "Oldman_Creel_app_data_daily_agg_merged.csv"
Oldman_Creel_app_data_daily_agg_merged.to_csv(outpath / file_name)

Oldman_Creel_app_data_daily_agg_merged.describe()

# Find Pearson correlation matrix before discretization
corr_matrix = Oldman_Creel_app_data_daily_agg_merged.corr()
corr_matrix.to_clipboard()

plt.figure(figsize=(8, 6))  # Adjust the figure size as needed
sns.heatmap(corr_matrix, cmap='PuBu', annot=True, annot_kws={'size': 8})  # Adjust the 'size' value to change font size
plt.savefig('Oldman_corr_matrix_before_discetization.pdf', format='pdf')
plt.show()

# Read data after discretization
Oldman_Discretized_data = pd.read_csv(outpath / "Discretized_Oldman_Creel_app_data_daily_agg.csv")

Oldman_Discretized_data

# Read the data after discretization to find the Pearson correlation after discretization
Oldman_Discretized_data = Oldman_Discretized_data[["creel_catch_rate", "app_catch_rate",
                                                   "creel_fishing_duration", "app_fishing_duration",
                                                   "air_temperature", "total_precipitation",
                                                   "relative_humidity", "solar_radiation",
                                                   "wind_speed", "degree_days",
                                                   "webpage_views", "is_weekend"
                                                   ]]

# Find the Pearson correlation matrix after discretization
Oldman_corr_matrix_discret = Oldman_Discretized_data.corr()

sns.heatmap(Oldman_corr_matrix_discret, cmap='PuBu', annot=True, annot_kws={'size': 8})
plt.savefig('corr_matrix_oldman_discrete.pdf', format='pdf')
# Show the heatmap
plt.show()
