"""
A script for home country detection for each individual Twitter user.

Representing two methods:
    
    1) Home is a country where user's geotagged tweet count is the greatest based on unique days classification
    2) Home is a country where user's geotagged tweet count is the greatest based on unique weeks classification
    
The Greater Region of Luxembourg is defined as an individual country.

@author: smassine
"""

# Import necessary libraries
import pickle
import pandas as pd
import geopandas as gpd
import sys
import csv

# Read Twitter data
# In order to validate home detection results, there should be a 'home_loc' column with some ground truth home values (taken from user given home location for those users
# whose home country could be detected unequivocally)
with open(r'C:\LocalData\smassine\Gradu\Data\Twitter\Luxemburg_border_region\API\Geotagged\TwitterAPI_lux_ready_for_analysis_homeattr.pkl', 'rb') as f:
    data = pickle.load(f)

# Read country polygons. Create a bbox for Greater Region of Luxembourg
greater_lux_region = gpd.read_file(r"C:\LocalData\smassine\Gradu\Data\GreaterLux\SHP\Global_regions_GreatLux.shp")
greater_lux_region = greater_lux_region.loc[greater_lux_region['GreaterLux'] == 1]

for index, row in greater_lux_region.iterrows():

    if index == 41:
        mergedpoly = row['geometry']
    else:
        mergedpoly = mergedpoly.union(row['geometry'])

# Add new columns for datetime: one with everything, other with only years, months and days
# Add columns for different home detection methods: unique days and unique weeks. Also, create a column for unique week calculation
data['datetime'] = None
data['datetime'] = pd.to_datetime(data['created_at'])
data['datetime_stripped'] = None
data['datetime_stripped'] = data['datetime'].dt.strftime("%Y-%m-%d")
data['home_unique_days'] = None
data['home_unique_weeks'] = None
data['unique_weeks'] = None

# Group the data by userid in order to define home country easily for each individual
grouped = data.groupby('userid')

def calculateUniqueWeeks(grouped_gdf):
    
    """
    A Function for calculating a unique week for every individual Twitter post in GeoDataFrame (week number  + year).
    
    Parameter
    ----------
    grouped_gdf: <gpd.GeoDataFrame>
    
        A GeoDataFrame grouped by some unique user ID (e.g. 'userid'). Hence, key refers to users Twitter ID and values
        to every Twitter post an individual has made.
        
    Output
    ------
    <gpd.GeoDataFrame>
        A GeoDataFrame including unique week information for each individual post.
     
    """
    
    y = 1
    # Create an accessory list for contemporary storing of values
    user_list = []
    
    for key, values in grouped_gdf:
        
        print("Processing: ", y, "/" ,len(grouped_gdf))
        y = y + 1
        
        individual = values
        individual = individual.sort_values(by='datetime')
        
        for index, row in individual.iterrows():
            
            date = row['datetime']
            year = date.year
            week_number = date.isocalendar()[1]
            unique_week = str(week_number) + "_" + str(year)
            
            individual.loc[index, 'unique_weeks'] = unique_week
            
        user_list.append(individual)
        print("Unique week added!")
    
    # Convert individuals list to GeoDataFrame
    twitter_home_week = gpd.GeoDataFrame(pd.concat(user_list, ignore_index=True))
    
    return(twitter_home_week)

def detectHomeCountry(grouped_gdf, home_detection_method):
    
    """
    A Function for detecting user's home country.
    
    Parameters
    ----------
    grouped_gdf: <gpd.GeoDataFrame>
    
        A GeoDataFrame grouped by some unique user ID (e.g. 'userid'). Hence, key refers to users Twitter ID and values
        to every Twitter post an individual has made.
        
    home_detection_method: <str>
    
        Input either 'home_unique_days' or 'home_unique_weeks'
        
    Output
    ------
    <gpd.GeoDataFrame>
        A GeoDataFrame including user's home country.
     
    """
    
    y = 1
    # Create accessory lists for contemporary storing of values
    user_list = []
    apu_lista = []
    
    for key, values in grouped_gdf:
        
        print("Processing: ", y, "/" ,len(grouped_gdf))
        y = y + 1
        
        individual = values
        individual = individual.sort_values(by='datetime')
        
        if home_detection_method == 'home_unique_days':
        
            method = individual.drop_duplicates(subset='datetime_stripped')
            value_list = method.post_country.value_counts()
            value_dict = dict(method.post_country.value_counts())
            
        elif home_detection_method == 'home_unique_weeks':
            
            method = individual.drop_duplicates(subset='unique_weeks')
            value_list = method.post_country.value_counts()
            value_dict = dict(method.post_country.value_counts())
        
        else:
            
            print("Specify either 'home_unique_days' or 'home_unique_weeks' as home_detection method!")
            sys.exit()
                
        if (len(value_list) == 1 or value_list[0] != value_list[1]):
                
            home = next(iter(value_dict))
                
            if home == 'Luxembourg':
                    
                home = 'Greater Region of Luxembourg'
                
                for index, row in individual.iterrows():
                    individual.loc[index, home_detection_method] = home
                    
                user_list.append(individual)   
                print(home, 'added!')
                    
            elif (home == 'Germany' or home == 'France' or home == 'Belgium'):
                    
                for index, row in method.iterrows():
                    
                    if row['coordinates'].within(mergedpoly) == True:
                        apu_lista.append('True')
                    
                if len(apu_lista) >= value_list[0]:
                    
                    home = 'Greater Region of Luxembourg'
                    apu_lista = []
                    
                    for index, row in individual.iterrows():
                        individual.loc[index, home_detection_method] = home
                        
                    user_list.append(individual)   
                    print(home, 'added!')
                
                else:
                    apu_lista = []
                    
                    for index, row in individual.iterrows():
                        individual.loc[index, home_detection_method] = home
                        
                    user_list.append(individual)   
                    print(home, 'added!')
                
            else:
                for index, row in individual.iterrows():
                    individual.loc[index, home_detection_method] = home
                    
                user_list.append(individual)   
                print(home, 'added!')
            
        elif value_list[0] == value_list[1]:
    
            home = next(iter(value_dict)) + ' or ' +  list(value_dict.keys())[1]
                
            if (next(iter(value_dict)) == 'Luxembourg' or next(iter(value_dict)) == 'Germany' or next(iter(value_dict)) == 'France' or next(iter(value_dict)) == 'Belgium' or 
                list(value_dict.keys())[1] == 'Luxembourg' or list(value_dict.keys())[1] == 'Germany' or list(value_dict.keys())[1] == 'France' or list(value_dict.keys())[1] == 'Belgium'):
                    
                for index, row in method.iterrows():
                        
                    if row['coordinates'].within(mergedpoly) == True:
                        apu_lista.append('True')
                
                if len(apu_lista) > value_list[0]:
                        
                    home = 'Greater Region of Luxembourg'
                    apu_lista = []
                    
                    for index, row in individual.iterrows():
                        individual.loc[index, home_detection_method] = home
                        
                    user_list.append(individual)   
                    print(home, 'added!')
                    
                else:
                    apu_lista = []
                    
                    for index, row in individual.iterrows():
                        individual.loc[index, home_detection_method] = home
                        
                    user_list.append(individual)   
                    print(home, 'added!')
                    print("Problematic! Two same")
    
            else:
                for index, row in individual.iterrows():
                    individual.loc[index, home_detection_method] = home
                    
                user_list.append(individual)   
                print(home, 'added!')
                print("Problematic! Two same")
                
        elif value_list[0] == value_list[1] == value_list[3]:
                
            home = next(iter(value_dict)) + ' or ' +  list(value_dict.keys())[1] + ' or ' +  list(value_dict.keys())[2]
            
            for index, row in individual.iterrows():
                individual.loc[index, home_detection_method] = home
                
            user_list.append(individual)
            print(home, 'added!')
            print("Problematic! Three same")
                
        else:
            home = 'Too many options'
            
            for index, row in individual.iterrows():
                individual.loc[index, home_detection_method] = home
                
            user_list.append(individual)
            print(home, 'added!')
            print("Problematic! Too many options")
            
    # Convert individuals list to GeoDataFrame
    twitter_home = gpd.GeoDataFrame(pd.concat(user_list, ignore_index=True))
    
    return(twitter_home)
    
# USE THE FUNCTIONS
    ## If unique days:
twitter_home_unique_days = detectHomeCountry(grouped, 'home_unique_days')

    ## If unique weeks (let's calculate it accumalately, with home_unique_days already there):
grouped_week = twitter_home_unique_days.groupby('userid')
unique_weeks = calculateUniqueWeeks(grouped_week)

grouped_week = unique_weeks.groupby('userid')
twitter_home_unique_weeks = detectHomeCountry(grouped_week, 'home_unique_weeks')

# Save
twitter_home_unique_weeks.to_pickle(r"C:\LocalData\smassine\Gradu\Data\Twitter\Luxemburg_border_region\API\Geotagged\TwitterAPI_lux_ready_for_analysis_home_day_week.pkl") 
# And open
with open(r'C:\LocalData\smassine\Gradu\Data\Twitter\Luxemburg_border_region\API\Geotagged\TwitterAPI_lux_ready_for_analysis_home_day_week.pkl', 'rb') as f:
    data = pickle.load(f)

def compareHomeDetectionMethods(df):
    
    """
    A Function for comparing the results (both unique days and unique weeks) to ground truth data.
    
    Parameter
    ----------
    df: <pd.DataFrame or gpd.GeoDataFrame>
    
        A DataFrame where home country location has been calculated useing both methods.
        
    Output
    ------
    <csv>
        A csv file containing four columns:
            
            userid: The user ID of a Twitter user account
            ground_truth_home: The ground truth home country
            home_unique_days: Home country calculated using the unique days classification
            home_unique_weeks: Home country calculated using the unique weeks classification
     
    """
    
    df = df.reset_index()
    
    # Select only wanted data for comparison
    
    # Ground truth home rows
    wanted_values = ['Belgium', 'France', 'Germany', 'Luxembourg']
    filter_data = df.loc[df['home_loc'].isin(wanted_values)]
    # Columns for comparison: ground truth and different methods. Userid also just in case.
    wanted_columns = ['userid', 'home_loc', 'home_unique_days', 'home_unique_weeks']
    filter_data = filter_data[wanted_columns]
    filter_data = filter_data.drop_duplicates(subset='userid')

    return(filter_data)

comparison_data = compareHomeDetectionMethods(data)
#comparison_data.to_csv(r"C:\LocalData\smassine\Gradu\Tilastot\home_country_detection_comparison.csv", sep = ",")

# Look at result accuracy
home_days = []
home_weeks = []
error_days = []
error_weeks = []

for index, row in comparison_data.iterrows():
    
    if row.home_loc == row.home_unique_days:
        home_days.append(row)
    
    elif row.home_loc == 'Luxembourg' and row.home_unique_days == 'Greater Region of Luxembourg':
        home_days.append(row)
    
    elif (row.home_loc == 'Belgium' or row.home_loc == 'France' or row.home_loc == 'Germany' or row.home_loc == 'Luxembourg') and row.home_unique_days == 'Greater Region of Luxembourg':
        home_days.append(row)
        
    else:
        error_days.append(dict(row))
    
    if row.home_loc == row.home_unique_weeks:
        home_weeks.append(row)
        
    elif row.home_loc == 'Luxembourg' and row.home_unique_weeks == 'Greater Region of Luxembourg':
        home_weeks.append(row)
    
    elif (row.home_loc == 'Belgium' or row.home_loc == 'France' or row.home_loc == 'Germany' or row.home_loc == 'Luxembourg') and row.home_unique_weeks == 'Greater Region of Luxembourg':
        home_weeks.append(row)
    
    else:
        error_weeks.append(dict(row))

print('Successful home unique days: ', len(home_days), '/', len(comparison_data))
print('Successful home unique weeks: ', len(home_weeks), '/', len(comparison_data))

print('Overall accuracy days: ', len(home_days)/len(comparison_data))
print('Overall accuracy weeks: ', len(home_weeks)/len(comparison_data))

keys = error_weeks[0].keys()
with open(r'C:\LocalData\smassine\Gradu\Tilastot\error_days.csv', 'w') as output_file:
    dict_writer = csv.DictWriter(output_file, keys)
    dict_writer.writeheader()
    dict_writer.writerows(error_days)

with open(r'C:\LocalData\smassine\Gradu\Tilastot\error_weeks.csv', 'w') as output_file:
    dict_writer = csv.DictWriter(output_file, keys)
    dict_writer.writeheader()
    dict_writer.writerows(error_weeks)