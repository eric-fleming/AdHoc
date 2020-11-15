# Dependencies
import numpy as np
import pandas as pd
import os as os
import time
from multiprocessing import Pool

# Import the data
plans_df = pd.read_csv('plans.csv')
zips_df = pd.read_csv('zips.csv')
slcsp_df = pd.read_csv('slcsp.csv')

# Begin
start_time = time.time()

# Select the subset of Silver plans from the plans dataframe
silverplans_df = plans_df[plans_df['metal_level'] == 'Silver']

# merging zipcodes with plans
silver_by_zip_df = zips_df.merge(silverplans_df, how="left", on=['state','rate_area'])

# Compile a drop list for zipcodes with multiple rate_areas 
zipcode_areas_df = pd.DataFrame(columns=['zipcode','area_count'])
zips = zips_df.zipcode.unique()

for z in zips:
    temp = silver_by_zip_df[silver_by_zip_df['zipcode'] == z]
    count = temp['rate_area'].unique().size
    zipcode_areas_df = zipcode_areas_df.append({
        'zipcode':z,
        'area_count':count
    }, ignore_index=True)

# Select records with only one rate_area
zipcode_areas_df = zipcode_areas_df[zipcode_areas_df['area_count'] == 1]


# Redefine the unique list of zips with only one rate_area
zips = zipcode_areas_df.zipcode.unique()

# Keep rows of silver plans with only one rate_area
# Drop the rows with zips with multiple rate_areas
silver_by_zip_df = silver_by_zip_df[silver_by_zip_df.zipcode.isin(zips)]



# Compile a drop list of with zips with only 1 rate
zips = silver_by_zip_df.zipcode.unique()
zipcode_rates_df = pd.DataFrame(columns=['zipcode','rates_count'])
for z in zips:
    temp = silver_by_zip_df[silver_by_zip_df['zipcode'] == z]
    count = temp['rate'].unique().size
    zipcode_rates_df = zipcode_rates_df.append({
        'zipcode':z,
        'rates_count':count
    }, ignore_index=True)


zipcode_rates_df = zipcode_rates_df[zipcode_rates_df['rates_count'] > 1]
zips = zipcode_rates_df.zipcode.unique()

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
for z in zips:
    temp = silver_by_zip_df[silver_by_zip_df['zipcode'] == z]
    vals = temp['rate'].unique()
    minimum = min(vals)
    vals = np.delete(vals, np.where(vals == minimum))
    secondMin = min(vals)
    slcsp_df.loc[slcsp_df.zipcode == z, 'rate'] = secondMin

# The csv has been updated.
# Export to file, show the results
slcsp_df.to_csv('out/slcsp.csv', index=False)

# end
end_time = time.time()
elapsed = end_time - start_time
print(elapsed)