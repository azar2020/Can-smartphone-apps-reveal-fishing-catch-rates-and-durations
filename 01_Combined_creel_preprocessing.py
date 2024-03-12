#!/usr/bin/env python
# coding: utf-8

import os
import vaex
import pyarrow.parquet as pq
import datetime
import pandas as pd
from datetime import datetime
import pathlib
import psutil  # to get to know required memory
import pyarrow as pa
import h5py
import seaborn as sns
import matplotlib.pyplot as plt
import calendar

# Define onpath and outpath
inpath = pathlib.Path(r"C:\Azar_Drive\relationships-between-variables1\01_preprocessing\data")
outpath = pathlib.Path(r"C:\Azar_Drive\relationships-between-variables1\01_preprocessing\results")

# Read the Bow River dataset
Bow_Creel_data_daily_agg = pd.read_csv(outpath/"Bow_creel_agg_before_mean.csv")

# Display Bow River dataset
Bow_Creel_data_daily_agg

# Read the Oldman River dataset
Oldman_Creel_data_daily_agg = pd.read_csv(outpath/"Oldman_creel_agg_before_mean.csv")

# Display Oldman River dataset
Oldman_Creel_data_daily_agg

# Concatenate the Bow River and Oldman River datasets
Creel_app_data_daily_aggregated = pd.concat([Bow_Creel_data_daily_agg, Oldman_Creel_data_daily_agg])

# Display the concatenated dataset
Creel_app_data_daily_aggregated

# Aggregate the dataset based on the date
Creel_app_data_daily_aggregated = Creel_app_data_daily_aggregated.groupby(['Date_FJ'], as_index=False).agg({
    "creel_hours_out": "sum",
    "Date_FJ": "first",
    "creel_number_of_trips": "first",
    "Source": "first",
    "All_Fish_FJ": "sum"
})

# Find the number of unique dates
len(Creel_app_data_daily_aggregated["Date_FJ"].unique())

# Calculate creel mean catch rate by dividing all fish caught on creel hours out
Creel_app_data_daily_aggregated["creel_catch_rate"] = Creel_app_data_daily_aggregated['All_Fish_FJ'] / Creel_app_data_daily_aggregated['creel_hours_out']

# Fill the null values with zero
Creel_app_data_daily_aggregated["creel_catch_rate"] = Creel_app_data_daily_aggregated["creel_catch_rate"].fillna(0)

# Initialize the column with a default value
Creel_app_data_daily_aggregated["creel_fishing_duration"] = 0

# Calculate average time spent on fishing by dividing all creel hours out by creel number of trips
Creel_app_data_daily_aggregated["creel_fishing_duration"] = Creel_app_data_daily_aggregated['creel_hours_out'] / Creel_app_data_daily_aggregated['creel_number_of_trips']

# Display information about creel dataset
Creel_app_data_daily_aggregated.info()

# Save daily aggregated creel data with corrected dates for MyCatch rows
file_name = "01-Creel_app_data_daily_aggregated.csv"
Creel_app_data_daily_aggregated.to_csv(outpath / file_name)


# In[ ]:





# In[ ]:





# In[ ]:




