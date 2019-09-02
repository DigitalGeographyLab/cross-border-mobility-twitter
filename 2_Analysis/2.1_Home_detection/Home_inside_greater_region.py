# -*- coding: utf-8 -*-
"""
Created on Fri Jan 25 11:59:02 2019

@author: Localadmin_smassine
"""

# Import necessary libraries
import pickle
import pandas as pd
import geopandas as gpd

# Open dataset
with open(r'C:\LocalData\smassine\Gradu\Data\Twitter\Luxemburg_border_region\API\Geotagged\TwitterAPI_lux_home_error_handling.pkl', 'rb') as f:
    data = pickle.load(f)

# Read country polygons. Create a bbox for Greater Region of Luxembourg
country_polygons = gpd.read_file(r"C:\LocalData\smassine\Gradu\Data\GreaterLux\SHP\Global_regions_GreatLux.shp")
greater_lux_region = country_polygons.loc[country_polygons['GreaterLux'] == 1]

for index, row in greater_lux_region.iterrows():

    if index == 41:
        mergedpoly = row['geometry']
    else:
        mergedpoly = mergedpoly.union(row['geometry'])

# Create column for detecting home region inside Greater Region of Luxembourg
data['lux_region_home'] = None

grouped = data.groupby('userid')

country_list = []
y = 1
df_list = []

# Iterate over Twitter users
for key, values in grouped:
    
    individual = values
    y = y + 1
    
    # We are only interested in users that have home located inside the Greater Region on Luxembourg
    if individual['home_unique_weeks'].unique()[0] == 'Greater Region of Luxembourg':
        
        print("Processing...", y, "/", len(grouped))
        
        # Use unique weeks algorithm to identify home country inside the Greater region
        unique_weeks = individual.drop_duplicates(subset='unique_weeks')
        
        # Iterate over user's unique weeks and calculate post counts per country inside the Greater Region
        for index, row in unique_weeks.iterrows():
        
            if row['coordinates'].within(mergedpoly) == True:
                country_list.append(row['post_country'])
        
        value_elements = {'Luxembourg': country_list.count('Luxembourg'), 'Germany': country_list.count('Germany'),
                          'Belgium': country_list.count('Belgium'), 'France': country_list.count('France')}
        
        home_country_list = [k for k,v in value_elements.items() if v == max(value_elements.values())]
        
        # Reset country_list for next Twitter user
        country_list = []
        
        # If the length of home country list is 1, the algorithm has detected unique home country
        if len(home_country_list) == 1:
            
            for index, row in individual.iterrows():
                individual.loc[index, 'lux_region_home'] = home_country_list[0]
            
            df_list.append(individual)
                
            print("Only one found! Added:", home_country_list[0])
        
        # If the length of home country list is more than 1, two or more countries inside the Greater Region have the same highest value
        # The Twitter user is a potential cross-border mover
        else:
            
            for index, row in individual.iterrows():
                individual.loc[index, 'lux_region_home'] = 'Potential cross-border mover'
            
            df_list.append(individual)
            
            print("Potential cross-border mover found! Added:", home_country_list)
    
    else:
        df_list.append(individual)
    
# Convert processed data back to GeoDataFrame
processed_data = gpd.GeoDataFrame(pd.concat(df_list, ignore_index=True))
# Save
processed_data.to_pickle(r"C:\LocalData\smassine\Gradu\Data\Twitter\Luxemburg_border_region\API\Geotagged\TwitterAPI_lux_home_inside_greater_region.pkl") 
    
    
    