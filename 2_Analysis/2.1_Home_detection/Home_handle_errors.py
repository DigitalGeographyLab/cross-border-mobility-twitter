"""
A script for processing errors do due home country detection algorithm's accuracy.
Some rows have been assigned with a wrong country. This rows will be updated with
ground truth value (user given home location).

Also, there are some problematic home locations for some Twitter users. These
individuals will be gathered into a list for further analysis.

@author: smassine
"""

# Import necessary libraries
import pickle
import pandas as pd
import geopandas as gpd
import json

# Open dataset
with open(r'C:\LocalData\smassine\Gradu\Data\Twitter\Luxemburg_border_region\API\Geotagged\TwitterAPI_lux_ready_for_analysis_home_day_week.pkl', 'rb') as f:
    data = pickle.load(f)

"""
# Data columns:
        ['coordinates', 'created_at', 'id', 'lang', 'source', 'text',
       'post_loc_name', 'post_loc_country', 'userid', 'user_name',
       'screen_name', 'user_flag_home', 'utc_offset', 'time_zone',
       'index_right', 'post_country', 'home_loc', 'datetime',
       'datetime_stripped', 'home_unique_days', 'home_unique_weeks',
       'unique_weeks']
"""
# Group the data by userid
grouped = data.groupby('userid')

# Some user given home locations were problematic, let's store these rows first to a list to get a better understanding
problematics_list = []

# Create list for user storage, and a accessory function for tracking the for loop
user_list = []
y = 1

# Iterate over each Twitter user
# Assign user given home location to home_unique_weeks column if the unique weeks home detection method has been off
for key, values in grouped:
    
    individual = values
    y = y + 1
    
    # We are only interested in individuals that have ground truth home information associated in home_loc column
    if (pd.isnull(individual['home_loc'].unique()[0])):
        
        user_list.append(individual)
        
    else:
        
        print("Processing 'home_loc' column... ", y, "/", len(grouped))
    
        if individual['home_unique_weeks'].unique()[0] != individual['home_loc'].unique()[0] and individual['home_unique_weeks'].unique()[0] != 'Greater Region of Luxembourg':
            
            if individual['home_loc'].unique()[0] == 'Problematic':
                
                list_element = {'userid': individual['userid'].unique()[0], 'user_flag_home': individual['user_flag_home'].unique()[0],
                                'home_loc': individual['home_loc'].unique()[0], 'home_unique_weeks': individual['home_unique_weeks'].unique()[0]}
                problematics_list.append(list_element)
                
                user_list.append(individual)
                print("Problematic found!")
            
            elif individual['home_loc'].unique()[0] == 'Luxembourg':
                
                for index, row in individual.iterrows():
                    
                    individual.loc[index, 'home_unique_weeks'] = 'Greater Region of Luxembourg'
                
                user_list.append(individual)   
                print('Greater Region of Luxembourg added!')
                    
            else:
                
                for index, row in individual.iterrows():
                    
                    individual.loc[index, 'home_unique_weeks'] = row['home_loc']
                
                user_list.append(individual)   
                print(row['home_loc'], 'added!')
            
        else:
            
            user_list.append(individual)
            print("No changes!")

# Convert individuals list to GeoDataFrame
processed_data = gpd.GeoDataFrame(pd.concat(user_list, ignore_index=True))
print("Lenght of problematics list: ", len(problematics_list))
print("Lenght of processed data: ", len(processed_data))

# Save data
processed_data.to_pickle(r"C:\LocalData\smassine\Gradu\Data\Twitter\Luxemburg_border_region\API\Geotagged\TwitterAPI_lux_home_error_handling.pkl") 

# Save also problematics list
output_file = open(r"C:\LocalData\smassine\Gradu\Data\Twitter\Luxemburg_border_region\API\Geotagged\problematics_list.txt", 'w', encoding='utf-8')

for dic in problematics_list:
   json.dump(dic, output_file) 
   output_file.write("\n")

"""
read_in_list = []
for line in open(r"C:\LocalData\smassine\Gradu\Data\Twitter\Luxemburg_border_region\API\Geotagged\problematics_list.txt", 'r'):
    read_in_list.append(json.loads(line))
"""
