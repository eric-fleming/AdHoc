# Dependencies
import numpy as np
import pandas as pd
import os as os
import time
from multiprocessing import Pool, Process
from collections import namedtuple
from functools import partial
# function defined in defs.py
# work around
import defs


# Import the data
plans_df = pd.read_csv('plans.csv')
zips_df = pd.read_csv('zips.csv')
slcsp_df = pd.read_csv('slcsp.csv')
print(slcsp_df.dtypes)
# Begin
start_time = time.time()

# Select the subset of Silver plans from the plans dataframe
silverplans_df = plans_df[plans_df['metal_level'] == 'Silver']

# merging zipcodes with plans
silver_by_zip_df = zips_df.merge(silverplans_df, how="left", on=['state','rate_area'])

# Compile a list for zipcodes
zips = zips_df.zipcode.unique()



# Create Thread pool
thread_pool = Pool()


# Keep only the unique list of zips with only one rate_area
zips = thread_pool.map(defs.zipAreaCountMap,zips)  
thread_pool.close()
thread_pool.join()

# Keep rows of silver plans with only one rate_area
# Drop the rows with zips with multiple rate_areas
silver_by_zip_df = silver_by_zip_df[silver_by_zip_df.zipcode.isin(zips)]



# Keep only the unique zips that have more than 1 rate price
thread_pool = Pool()
zips = thread_pool.map(defs.zipAreaRateMap,zips)  
thread_pool.close()
thread_pool.join()

# Drop the rows with zips with only 1 rate
# Keep the rows with multiple rates to compare against
silver_by_zip_df = silver_by_zip_df[silver_by_zip_df.zipcode.isin(zips)]


# Now the data is cleaned and ready for analysis to find the second lowest rate
# Only Silver plans
# Zipcodes with only 1 rate area
# Zipcodes with more than one rate price

# Compile a list of prices by zip code
# Sort from least to greatest, stop after 2nd run through
# Update the other csv


zips = silver_by_zip_df.zipcode.unique()



# TO FIX THIS I NEED TO CREATE A CLASS WHERE WE CAN ACCESS THE UNDERLYING DATAFRAME
# THE METHODS EXECUTE OUT OF SCOPE OTHERWISE

# Generare a unique list of tuples with zip and list of rates
SecondLowestRateByZip = namedtuple("SecondLowestRateByZip",["zip","rate"])
partialSLRBZ = partial(defs.secondLowestRateByZipTuple,silver_by_zip_df)

# List of tuples
thread_pool = Pool()
SecondLowestRateByZipList = thread_pool.map(partialSLRBZ,zips)  
thread_pool.close()
thread_pool.join()

print('first record',SecondLowestRateByZipList[0])
print('type of tuple.zip', type(SecondLowestRateByZipList[0].zip))
print('type of tuple.rate', type(SecondLowestRateByZipList[0].rate))
# Append to file
for t in SecondLowestRateByZipList:
    slcsp_df.loc[slcsp_df.zipcode == t.zip, 'rate'] = t.rate

# The csv has been updated.
# Export to file, show the results
slcsp_df.to_csv('out/slcsp_multi.csv', index=False)
#slcsp_df.to_csv('out/fast_slcsp_multi.csv', index=False)
# end
end_time = time.time()
elapsed = end_time - start_time
print(elapsed)