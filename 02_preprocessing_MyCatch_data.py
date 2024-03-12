
import vaex
import pandas as pd
import numpy as np
import sqlalchemy
import pathlib
import os
import psutil  # to get to know required memory
import pyarrow as pa

# Define onpath and outpath
inpath = pathlib.Path(r"C:\Azar_Drive\relationships-between-variables1\01_preprocessing\data")
outpath = pathlib.Path(r"C:\Azar_Drive\relationships-between-variables1\01_preprocessing")


# Read static Angler's Atlas data
df_pandas = pd.read_excel(str(inpath / "static_data.xlsx"))
df_static = vaex.from_pandas(df_pandas)

# Read temporal Angler's Atlas data
df_temporal = vaex.open(inpath / 'Temporal_data_item1' / '*.parquet')
# Change datatype of variable 'date'
df_temporal['date'] = df_temporal['date'].astype('datetime64')


df_temporal.head(2)


# Create a subset of datasets for two waterbodies in Alberta (Bow Creek, Oldman Creek) and merge them

# Waterbody ids that we have data for
Bow_Oldman_ids = [87178, 87177, 91720, 91721, 107446, 108992, 110177]

# Get spatial subset of static data for the selected waterbodies
Bow_Oldman_static_data = df_static[df_static.id.isin(Bow_Oldman_ids)]

# Get spatial subset of temporal data for the selected waterbodies
Bow_Oldman_temporal_data = df_temporal[df_temporal.waterbody_id.isin(Bow_Oldman_ids)]


# Get temporal subset of temporal data (only year 2018)
Bow_Oldman_temporal_data_2018 = Bow_Oldman_temporal_data[Bow_Oldman_temporal_data['date'].dt.year == 2018]


# Merge temporal dataset and static dataset for the selected waterbodies
Bow_Oldman_app_dataset = Bow_Oldman_temporal_data_2018.join(Bow_Oldman_static_data, left_on='waterbody_id', right_on='id')


# Rename "hours_out" to "app_hours_out"
# Bow_Oldman_app_dataset = Bow_Oldman_app_dataset.rename({'hours_out': 'app_hours_out'})


# Save dataset
file_name = "Environment_App_Bow_Oldman_2018.hdf5"
Bow_Oldman_app_dataset.export_hdf5(outpath / file_name)

