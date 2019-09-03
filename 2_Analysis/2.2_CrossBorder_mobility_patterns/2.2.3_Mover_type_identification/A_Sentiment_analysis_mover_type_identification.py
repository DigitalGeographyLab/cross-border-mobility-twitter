"""
This script identifies and classifies Twitter users to different cross-border mover classes.

Two criteria:

    1)	“Inside GRL” trips’ share of all trips inside the Greater Region, and
    2)	Country sections’ share of geo-tagged posts.

The first criterion is passed if “Inside GRL” trips’ share of all trips is >= 20 %.
The second criterion, again, is passed if a country section’s share of geo-tagged posts
iss >= 20 % and the 20 % threshold iss exceeded in at least two countries. Also, one of
these countries has to be a user’s home country.

If both criteria are satisfied, a user is being labeled as a CROSS-BORDER COMMUTER.
Else, a user is being given an INFREQUENT BORDER CROSSER label.

@author: smassine
"""
# Import necessary libraries
import pickle
import pandas as pd
import geopandas as gpd
import sys
import fiona

## 1) Inside GRL portion of all trips in GRL, threshold >= 20 %

# Open LineString dataset
with open(r'C:\LocalData\smassine\Gradu\Data\Twitter\Luxemburg_border_region\API\Geotagged\LineString\Greater_Region_crs_update_missing_countries.pkl', 'rb') as f:
    line_data = pickle.load(f)

# Let's select only those rows that we need under scrutiny
wanted_rows = ['Inside GRL, no CB', 'Inside GRL', 'Partly inside GRL, no CB']
line_data_grl = line_data.loc[line_data['CB_move'].isin(wanted_rows)]

# Exclude 0 km trips
line_data_grl = line_data_grl.loc[line_data_grl['distanceKm'] > 0.000000]

# Group by userid
grouped = line_data_grl.groupby('userid')
user_list = []
y = 1

# Iterate over the grouped dataset and calculate 'Inside GRL's portion of movements inside GRL
for key, values in grouped:
    
    print("Processing:", y, "/", len(grouped))
    y = y + 1
    
    individual = values
    
    # Let's extract individual's CB values inside GRL
    CB_values = individual.CB_move.value_counts().to_frame()
    
    # Calculate sum of all movements inside the GRL
    CB_sum = CB_values.CB_move.sum()
    
    if 'Inside GRL' in CB_values.index.values:
        
        # Get values for 'Inside GRL'
        inside_grl = CB_values.loc['Inside GRL'][0]
        
        # Calculate 'Inside GRL' portion
        portion = inside_grl/CB_sum * 100
        
        if portion >= 20:
            
            user_element = {'userid': individual.userid.unique()[0], 'inside_GRL_%': portion, 'factor_line': 1}
            user_list.append(user_element)
        
        else:
            
            user_element = {'userid': individual.userid.unique()[0], 'inside_GRL_%': portion, 'factor_line': 0}
            user_list.append(user_element)
    
    else:
        
        user_element = {'userid': individual.userid.unique()[0], 'inside_GRL_%': 0, 'factor_line': 0}
        user_list.append(user_element)
        
portion_df = pd.DataFrame(user_list)

## 2) Country sections’ share of geo-tagged posts.
    
# Open Point dataset
with open(r'C:\LocalData\smassine\Gradu\Data\Twitter\Luxemburg_border_region\API\Geotagged\Twitter_data_update_missing_countries.pkl', 'rb') as f:
    point_data = pickle.load(f)

point_data.crs = fiona.crs.from_epsg(4326)

# Let's select all Tweets from users living inside the Greater Region of Luxembourg
wanted_tweets = ['Luxembourg', 'Germany', 'Belgium', 'France']
point_data_grl = point_data.loc[point_data['lux_region_home'].isin(wanted_tweets)]

# Read country polygons. Create a bbox for Greater Region of Luxembourg
greater_lux_region = gpd.read_file(r"C:\LocalData\smassine\Gradu\Data\GreaterLux\SHP\Global_regions_GreatLux.shp")
greater_lux_region = greater_lux_region.to_crs({'init': 'epsg:4326', 'no_defs': True})
greater_lux_region = greater_lux_region.loc[greater_lux_region['GreaterLux'] == 1]

for index, row in greater_lux_region.iterrows():

    if index == 41:
        mergedpoly = row['geometry']
    else:
        mergedpoly = mergedpoly.union(row['geometry'])

# Group by userid
grouped_points = point_data_grl.groupby('userid')
point_list = []
discarded_users = []
i = 1

# Iterate
for key, values in grouped_points:
    
    print("Processing:", i, "/", len(grouped_points))
    i = i + 1
    
    individual = values
    
    # Store individual home into a variable
    user_home = individual.lux_region_home.unique()[0]
    
    # Drop Tweets that are outside of the Greater Region
    for index, row in individual.iterrows():
        
        if row['coordinates'].within(mergedpoly) == False:
            individual = individual.drop([index])
    
    individual = individual.reset_index()    
    
    # This shouldn't happen but check nevertheless
    if len(individual) > 0:
        
        # Exclude 0 km trips
        for index, row in individual.iterrows():
            
            if index == 0:
                starting_tweet = row['coordinates']
                continue
            
            if row['coordinates'] == starting_tweet:
                individual = individual.drop([index])
            else:
                starting_tweet = row['coordinates']
    
    else:
        print("Individual doesn't have posts inside GRL!")
        sys.exit()
    
    # Store post counts in different countries, calculate sum and portion of Tweets for each country
    post_country_values = individual.country.value_counts().to_frame()
    post_country_sum = post_country_values.country.sum()
    post_country_values['portion'] = post_country_values.apply(lambda row: (row.country / post_country_sum) * 100, axis=1)
        
    # Filter values based on condition
    condition_values = post_country_values.loc[(post_country_values['portion'] >= 20)]
    
    if len(condition_values) == 0:
        
        element = {'userid': individual.userid.unique()[0], 'factor_point': 0, 'C1': 0, 'C1_%': 0, 'C2': 0, 'C2_%': 0, 'C3': 0, 'C3_%': 0, 'home_country': user_home}
        point_list.append(element)
        print("Zero")
    
    elif len(condition_values) == 1:
        
        element = {'userid': individual.userid.unique()[0], 'factor_point': 1, 'C1': condition_values.index[0],
                   'C1_%': condition_values['portion'].values[0], 'C2': 0, 'C2_%': 0, 'C3': 0, 'C3_%': 0, 'home_country': user_home}
        
        if element['C1'] == user_home:
            point_list.append(element)
        else:
            print("User discarded!")
            discarded_users.append(element)
        
    elif len(condition_values) == 2:
        
        element = {'userid': individual.userid.unique()[0], 'factor_point': 2, 'C1': condition_values.index[0],
                   'C1_%': condition_values['portion'].values[0], 'C2': condition_values.index[1],
                   'C2_%': condition_values['portion'].values[1], 'C3': 0, 'C3_%': 0, 'home_country': user_home}

        if (user_home in element['C1'] or user_home in element['C2']):
            point_list.append(element)
        else:
            print("User discarded!")
            discarded_users.append(element)
    
    elif len(condition_values) == 3:
        
        element = {'userid': individual.userid.unique()[0], 'factor_point': 3, 'C1': condition_values.index[0],
                   'C1_%': condition_values['portion'].values[0], 'C2': condition_values.index[1],
                   'C2_%': condition_values['portion'].values[1], 'C3': condition_values.index[2],
                   'C3_%': condition_values['portion'].values[2], 'home_country': user_home}

        if (user_home in element['C1'] or user_home in element['C2'] or user_home in element['C3']):
            point_list.append(element)
        else:
            print("User discarded!")
            discarded_users.append(element)
    
    elif len(condition_values) == 4:
        
        element = {'userid': individual.userid.unique()[0], 'factor_point': 3, 'C1': condition_values.index[0],
                   'C1_%': condition_values['portion'].values[0], 'C2': condition_values.index[1],
                   'C2_%': condition_values['portion'].values[1], 'C3': condition_values.index[2],
                   'C3_%': condition_values['portion'].values[2], 'C4': condition_values.index[3],
                   'C4_%': condition_values['portion'].values[3], 'home_country': user_home}
        
        print("Four")
        
        if (user_home in element['C1'] or user_home in element['C2'] or user_home in element['C3'] or user_home in element['C4']):
            point_list.append(element)
        else:
            print("User discarded!")
            discarded_users.append(element)
        
    else:
    
        print("More than four, impossible!")
        sys.exit()

point_df = pd.DataFrame(point_list)

# Merge dfs
merged_df = pd.merge(portion_df, point_df, on=['userid','userid'])
merged_df.to_excel(r"C:\LocalData\smassine\Gradu\Data\Twitter\Luxemburg_border_region\API\Geotagged\Commuters\NullTripsExclude\commuter_identification_20_threshold.xlsx")

# Save userid lists
wanted_users = merged_df.loc[merged_df['factor_line'] == 1]
wanted_users = wanted_users.loc[wanted_users['factor_point'] > 1]

userid_list = wanted_users['userid']
userid_list.to_csv(r'C:\LocalData\smassine\Gradu\Data\Twitter\Luxemburg_border_region\API\Geotagged\Commuters\NullTripsExclude\commuter_userids_20_threshold.txt', sep=' ', index=False)

# Save discarded users
with open(r'C:\LocalData\smassine\Gradu\Data\Twitter\Luxemburg_border_region\API\Geotagged\Commuters\NullTripsExclude\discarded_userids_20_threshold.txt', 'w') as f:
    for item in discarded_users:
        f.write("%s\n" % item)
        