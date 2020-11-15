# Dependencies
import numpy as np
import pandas as pd
import os as os
from collections import namedtuple
from functools import partial
# Import the data
plans_df = pd.read_csv('plans.csv')
zips_df = pd.read_csv('zips.csv')
slcsp_df = pd.read_csv('slcsp.csv')


# Creating a named tuple
# Generare a unique list of tuples with zip and list of rates
SecondLowestRateByZip = namedtuple("SecondLowestRateByZip",["zip","rate"])

# Select the subset of Silver plans from the plans dataframe
silverplans_df = plans_df[plans_df['metal_level'] == 'Silver']

# merging zipcodes with plans
silver_by_zip_df = zips_df.merge(silverplans_df, how="left", on=['state','rate_area'])

# return zips that only fall within a single rate-area
def zipAreaCountMap(zip):
    temp = silver_by_zip_df[silver_by_zip_df['zipcode'] == zip]
    count = temp['rate_area'].unique().size
    if count == 1:
        return zip
    return



# return zips that have more than one rate-price so we have something to compare
def zipAreaRateMap(zip):
    temp = silver_by_zip_df[silver_by_zip_df['zipcode'] == zip]
    count = temp['rate'].unique().size
    if count > 1:
        return zip
    return


def zipFindSecondLowestRate(zip):
    temp = silver_by_zip_df[silver_by_zip_df['zipcode'] == zip]
    vals = temp['rate'].unique()
    minimum = min(vals)
    vals = np.delete(vals, np.where(vals == minimum))
    secondMin = min(vals)
    # slcsp_df.loc[slcsp_df.zipcode == zip, 'rate'] = secondMin
    return secondMin


# for the fastest implementation
def secondLowestRateByZipTuple(df,zip):
    temp = df[df['zipcode'] == zip]
    vals = temp['rate'].unique()
    minimum = min(vals)
    vals = np.delete(vals, np.where(vals == minimum))
    secondMin = min(vals)
    return SecondLowestRateByZip(zip, secondMin)



# print to the output file
def printSecondLosestRateToFile(tuple):
    slcsp_df.loc[slcsp_df.zipcode == tuple.zip, 'rate'] = tuple.rate