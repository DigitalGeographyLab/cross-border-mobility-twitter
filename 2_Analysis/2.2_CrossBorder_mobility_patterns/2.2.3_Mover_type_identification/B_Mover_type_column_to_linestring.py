"""
This script adds identified mover type labels to Twitter LineString dataset.

@author: smassine
"""

import pandas as pd
import geopandas as gpd
import pickle

# Open LineString dataset
with open(r'C:\LocalData\smassine\Gradu\Data\Twitter\Luxemburg_border_region\API\Geotagged\LineString\Greater_Region_crs_update_missing_countries.pkl', 'rb') as f:
    data = pickle.load(f)
    
text_file = open(r"C:\LocalData\smassine\Gradu\Data\Twitter\Luxemburg_border_region\API\Geotagged\Daily\NullTripsExclude\daily_userids_20_threshold.txt", "r")
id_list = text_file.read().split('\n')

daily_movers = data.loc[data['userid'].isin(id_list)]
infrequent = data.loc[~data['userid'].isin(id_list)]

daily_movers['moverType'] = 'CB-commuter'
infrequent['moverType'] = 'Infrequent border crosser'

merged_df = pd.concat([daily_movers, infrequent], ignore_index = True)

merged_df.to_pickle(r"C:\LocalData\smassine\Gradu\Data\Twitter\Luxemburg_border_region\API\Geotagged\LineString\MoverColumn\Greater_Region_mover_type.pkl")

merged_df = merged_df.astype({"origTime": str, "destTime": str, 'avgTime': str, 'duration': str})
merged_df = gpd.GeoDataFrame(merged_df, geometry='geometry')
merged_df.to_file(r"C:\LocalData\smassine\Gradu\Data\Twitter\Luxemburg_border_region\API\Geotagged\LineString\MoverColumn\SHP\Greater_Region_LineString_mover_type.shp")
