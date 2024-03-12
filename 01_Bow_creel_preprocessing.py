#!/usr/bin/env python
# coding: utf-8

import pandas as pd
from datetime import datetime
import re
import pathlib
import os, psutil
import pyarrow as pa
import h5py

# Define onpath and outpath
inpath = pathlib.Path(r"C:\Azar_Drive\relationships-between-variables1\01_preprocessing\data")
outpath = pathlib.Path(r"C:\Azar_Drive\relationships-between-variables1\01_preprocessing\results")

# Read dataset
Bow_creel = pd.read_csv(inpath/"Bow_creel.csv")

# Find the number of unique days that we have data for
len(Bow_creel["Date_FJ"].unique())

# Display dataset statistics
Bow_creel.describe()

# Find the samples with daily time spent on fishing greater than 24
Bow_creel[Bow_creel["Time_Hr"] > 24]

# Change the format of date
date_column = 'Date_FJ'
original_format = "%d/%m/%y"
new_format = "%Y-%m-%d"
Bow_creel[date_column] = pd.to_datetime(Bow_creel[date_column], format=original_format)
Bow_creel[date_column] = Bow_creel[date_column].dt.strftime(new_format)

# Assign waterbody_id based on conditions
# Bow_creel.loc[(Bow_creel['WB_Region'] == "PP1") & (Bow_creel['Waterbody'] == "Oldman River"), 'waterbody_id'] = 91720
# ...

# Print MyCatch rows in Oldman creel dataset
Bow_creel_MyCatch = Bow_creel[Bow_creel["Source"]=="MyCatch"]
Bow_creel_MyCatch

# Read Angler's Atlas (AA) citizen-reported dataset to find the actual date
AA_trips = pd.read_csv(inpath/"creel_surveys.csv")

# Find unique values of region
AA_trips["region"].unique()

# Merge AAA_trips and creel data (Oldman) based on trip id and the name of the waterbody
merged_df_Bow = pd.merge(AA_trips, Bow_creel_MyCatch, left_on=['id'], right_on=['Trip_ID_FJ'], how='inner')

merged_df_Bow

# Replace reported date (Date_FJ) with the actual trip date (for_date) based on trip id (Oldman)
for i in range(merged_df_Bow.shape[0]):
    for j in range(Bow_creel.shape[0]):
        if Bow_creel.loc[j]["Trip_ID_FJ"] == merged_df_Bow.loc[i]["id"]:
            Bow_creel["Date_FJ"][j] = merged_df_Bow["for_date"][i]

# Exclude the rows with time spent on fishing greater than or equal to 24 hours
Bow_creel = Bow_creel[Bow_creel["Time_Hr"] < 24]

# Display updated dataset statistics
Bow_creel.describe()

# Rename "creel_hours_out" to "creel_hours_out"
Bow_creel = Bow_creel.rename(columns={'Time_Hr': 'creel_hours_out'})

# Exclude MyCatch rows in the creel data
Bow_creel = Bow_creel[Bow_creel['Source'] != 'MyCatch']

# Display columns
Bow_creel.columns

# Count the number of trips on a specific date in a specific waterbody
Bow_creel['creel_number_of_trips'] = Bow_creel.groupby(['Date_FJ'])['Trip_ID_FJ'].transform('count')

# Aggregate creel data based on date and waterbody id
Bow_creel_agg = Bow_creel.groupby(['Date_FJ'], as_index=False).agg({"creel_hours_out": "sum",
                                                                    "Date_FJ": "first",
                                                                    "creel_number_of_trips": "first",
                                                                    "Source": "first",
                                                                    "All_Fish_FJ": "sum"})

# Save daily aggregated Bow_creel before finding mean catch rate and mean fishing time
file_name = "Bow_creel_agg_before_mean.csv"
Bow_creel_agg.to_csv(outpath / file_name)

# Display aggregated dataset statistics
Bow_creel_agg.describe()

# Initialize the column with a default value
Bow_creel_agg["creel_catch_rate"] = 0

# Calculate creel mean catch rate by dividing all fish caught by creel hours out
Bow_creel_agg["creel_catch_rate"] = Bow_creel_agg['All_Fish_FJ'] / Bow_creel_agg['creel_hours_out']

# Initialize the column with a default value
Bow_creel_agg["creel_fishing_duration"] = 0

# Calculate average time spent on fishing by dividing all creel hours out by creel number of trips
Bow_creel_agg["creel_fishing_duration"] = Bow_creel_agg['creel_hours_out'] / Bow_creel_agg['creel_number_of_trips']

# Display information about creel dataset
Bow_creel_agg.info()

# Save daily aggregated creel data with corrected dates for MyCatch rows
file_name = "01-Bow_creel_agg.csv"
Bow_creel_agg.to_csv(outpath / file_name)




