#!/usr/bin/env python
# coding: utf-8

import pandas as pd
from datetime import datetime
import re
import pathlib
import os
import psutil  # to get to know required memory
import pyarrow as pa
import h5py

# Define onpath and outpath
inpath = pathlib.Path(r"C:\Azar_Drive\relationships-between-variables1\01_preprocessing\data")
outpath = pathlib.Path(r"C:\Azar_Drive\relationships-between-variables1\01_preprocessing\results")

# Read data
Oldman_creel = pd.read_csv(inpath/"Oldman_creel.csv")

# Display dataset statistics
Oldman_creel.describe()

# Find the number of unique days that we have data for
len(Oldman_creel["Date_FJ"].unique())

# Change the format of date
date_column = 'Date_FJ'
original_format = "%d/%m/%y"
new_format = "%Y-%m-%d"

Oldman_creel[date_column] = pd.to_datetime(Oldman_creel[date_column], format=original_format)
Oldman_creel[date_column] = Oldman_creel[date_column].dt.strftime(new_format)

# Print the name of the waterbodies
Oldman_creel["Waterbody"].unique()

# Assign an ID to the waterbodies using WaterbodiesInAA.txt file
Oldman_creel.loc[(Oldman_creel['WB_Region'] == "PP1") & (Oldman_creel['Waterbody'] == "Oldman River"), 'waterbody_id'] = 91720
Oldman_creel.loc[(Oldman_creel['WB_Region'] == "ES1") & (Oldman_creel['Waterbody'] == "Oldman River"), 'waterbody_id'] = 91721
Oldman_creel.loc[(Oldman_creel['WB_Region'] == "ES1") & (Oldman_creel['Waterbody'] == "Livingstone River"), 'waterbody_id'] = 107446
Oldman_creel.loc[(Oldman_creel['WB_Region'] == "ES1") & (Oldman_creel['Waterbody'] == "Dutch Creek"), 'waterbody_id'] = 108992
Oldman_creel.loc[(Oldman_creel['WB_Region'] == "ES1") & (Oldman_creel['Waterbody'] == "Racehorse Creek"), 'waterbody_id'] = 110177

# Print MyCatch rows in Oldman creel dataset
# Select only the rows from MyCatch
Oldman_creel_MyCatch = Oldman_creel[Oldman_creel["Source"] == "MyCatch"]
Oldman_creel_MyCatch

# Read Angler's Atlas (AA) citizen-reported dataset to find the actual date
AA_trips = pd.read_csv(inpath/"creel_surveys.csv")

len(Oldman_creel["Trip_ID_FJ"].unique())

# Merge AA_trips and creel data (Oldman) based on trip id and the name of the waterbody
merged_df_Oldman = pd.merge(AA_trips, Oldman_creel_MyCatch, left_on=["waterbody", "id"],
                            right_on=["Waterbody", "Trip_ID_FJ"], how='inner')

merged_df_Oldman

# Replace reported date (Date_FJ) with the actual trip date (for_date) based on trip id (Oldman)
for i in range(merged_df_Oldman.shape[0]):
    for j in range(Oldman_creel.shape[0]):
        if Oldman_creel.loc[j]["Trip_ID_FJ"] == merged_df_Oldman.loc[i]["id"]:
            Oldman_creel["Date_FJ"][j] = merged_df_Oldman["for_date"][i]

Oldman_creel.columns

# Rename "creel_hours_out" to "creel_hours_out"
Oldman_creel = Oldman_creel.rename(columns={'Time_Hr': 'creel_hours_out'})

# Exclude MyCatch rows in the creel data
Oldman_creel = Oldman_creel[Oldman_creel['Source'] != 'MyCatch']

Oldman_creel

# Count the number of trips in a specific date in a specific waterbody
Oldman_creel['creel_number_of_trips'] = Oldman_creel.groupby(['Date_FJ', 'waterbody_id'])['Trip_ID_FJ'].transform('count')

# Aggregate creel data based on date and waterbody id
Oldman_creel_agg = Oldman_creel.groupby(['Date_FJ'],
                                        as_index=False).agg({"creel_hours_out": "sum", "Date_FJ": "first",
                                                           "waterbody_id": "first", "Waterbody": "first",
                                                           "creel_number_of_trips": "first", "Source": "first",
                                                           "All_Fish_FJ": "sum"})

# Save daily aggregated Oldman_creel before finding mean catch rate and mean fishing time
file_name = "Oldman_creel_agg_before_mean.csv"
Oldman_creel_agg.to_csv(outpath / file_name)

Oldman_creel_agg

# Initialize the column with a default value
Oldman_creel_agg["creel_catch_rate"] = 0

# Calculate creel mean catch rate by dividing all fish caught by creel hours out
Oldman_creel_agg["creel_catch_rate"] = Oldman_creel_agg['All_Fish_FJ'] / Oldman_creel_agg['creel_hours_out']

# Fill the null values with zero
Oldman_creel_agg["creel_catch_rate"] = Oldman_creel_agg["creel_catch_rate"].fillna(0)

# Initialize the column with a default value
Oldman_creel_agg["creel_fishing_duration"] = 0

# Calculate average time spent on fishing by dividing all creel hours out by creel number of trips
Oldman_creel_agg["creel_fishing_duration"] = Oldman_creel_agg['creel_hours_out'] / Oldman_creel_agg['creel_number_of_trips']

# Display information about creel dataset
Oldman_creel_agg.info()

# Save daily aggregated creel data with corrected dates for MyCatch rows
file_name = "01-Oldman_creel_agg.csv"
Oldman_creel_agg.to_csv(outpath / file_name)


