"""
This script preprocesses data collected from Twitter API for further use:
    1. Extraction of geotagged tweets
    2. Removal of likely bots
    3. Creation of data schema, extraction of wanted user profile information
    4. Producing hashed pseudo-ids to support GDPR
    (5. Accessory functions to support above phases)

@author: smassine
"""
# Import necessary libraries
import pickle
import pandas as pd
import json
import geopandas as gpd
import random

## 1. EXTRACTION OF GEO-TAGGED ROWS
def getGeotaggedRows(fp):
    
    # Read in Twitter data
    with open(fp, "rb") as f:
        data = pickle.load(f)
    
    print("The lenght of unfiltered data: ", len(data))
    
    # Drop rows that do not have spatial information(coordinates)
    twitter_data = data.dropna(axis=0, subset=['coordinates'])
    twitter_data = twitter_data.reset_index(drop=True)

    return(twitter_data)

data1 = getGeotaggedRows(r"C:\LocalData\smassine\Gradu\Data\Twitter\Luxemburg_border_region\API\Twitter_userid_0_600.pkl")
data2 = getGeotaggedRows(r"C:\LocalData\smassine\Gradu\Data\Twitter\Luxemburg_border_region\API\Twitter_userid_601_1700.pkl")
data3 = getGeotaggedRows(r"C:\LocalData\smassine\Gradu\Data\Twitter\Luxemburg_border_region\API\Twitter_userid_1701_2283.pkl")
data4 = getGeotaggedRows(r"C:\LocalData\smassine\Gradu\Data\Twitter\Luxemburg_border_region\API\Twitter_userid_2284_3349.pkl")
data5 = getGeotaggedRows(r"C:\LocalData\smassine\Gradu\Data\Twitter\Luxemburg_border_region\API\Twitter_userid_3350_4020.pkl")

print("Total length of geo-tagged data: ", len(data1) + len(data2) + len(data3) + len(data4) + len(data5))

## ACCESSORY FUNCTION FOR getGeotaggedRows
def combineDataframes(data1, data2):
    
    print("The lenght of data1: ", len(data1))
    print("The lenght of data2: ", len(data2))
    print("Combining datasets...")

    combined_gdf = pd.concat([data1, data2])
    combined_gdf = combined_gdf.reset_index(drop=True)
    
    print("Combining successful!")
    print("The lenght of data now: ", len(combined_gdf))
    print("Saving combined data...")
    
    combined_gdf.to_pickle(r'C:\LocalData\smassine\Gradu\Data\Twitter\FIN_EST\API\TwitterAPI_lux_geotagged.pkl')
    
    return(combined_gdf)

"""
geotagged_data = combineDataframes(data1, data2)
geotagged_data_2 = combineDataframes(geotagged_data, data3)
geotagged_data_3 = combineDataframes(geotagged_data_2, data4)
geotagged_data_4 = combineDataframes(geotagged_data_3, data5)
"""

# Read in Twitter data
with open(r"C:\LocalData\smassine\Gradu\Data\Twitter\Luxemburg_border_region\API\Geotagged\TwitterAPI_lux_geotagged.pkl", "rb") as f:
    geotagged_data = pickle.load(f)

## ACCESSORY FUNCTION FOR removeBots
def getCombinedListAsDict(fp):
    
    lista = open(r"C:\LocalData\smassine\Gradu\Data\Twitter\Luxemburg_border_region\bot_list_combined.txt" ,"r")
    lines = lista.read().split(',\n')
    
    dictionary = []
    
    for item in lines:
        
        item = item.replace("\'", "\"")
        d = json.loads(item)
        dictionary.append(d)
    
    # df = pd.DataFrame(dictionary)
    
    return(dictionary)

bot_list = getCombinedListAsDict(r"C:\LocalData\smassine\Gradu\Data\Twitter\Luxemburg_border_region\bot_list_combined.txt")

## 2. REMOVAL OF LIKELY BOTS
def removeBots(gdf, bot_list):
    
    """
    A Function for removing Twitter bots.
    
    Parameters
    ----------
    gdf: <gpd.GeoDataFrame>
    
        A GeoDataFrame from which Twitter bots should be removed.
        
    bot_list: <list>
    
        Input either 'home_unique_days' or 'home_unique_weeks'
        
    Output
    ------
    <gpd.GeoDataFrame>
        A processed GeoDataFrame. Likely bots removed.
     
    """
    
    copy = gdf
    
    for index, row in gdf.iterrows():
        
        userid = str(row['user']['id'])
    
        for item in bot_list:
            
            bot_id = item['userid']
        
            if bot_id == userid:
                
                gdf = gdf.drop(index)
                print("A bot dropped: ID", userid, ". Length of GDF now: ", len(gdf))
                print("Processing: ", index, "/", len(copy))
    
    return(gdf)

bots_filtered_gdf = removeBots(geotagged_data, bot_list)
bots_filtered_gdf = bots_filtered_gdf.reset_index(drop=True)
        
bots_filtered_gdf.to_pickle(r'C:\LocalData\smassine\Gradu\Data\Twitter\Luxemburg_border_region\API\Geotagged\TwitterAPI_lux_geotagged_no_bots.pkl')

## 3. CREATION OF DATA SCHEMA, EXTRACTION OF WANTED USER PROFILE INFORMATION
def getUserSchema(columns_list):
    
    """
    A Function for creating wanted data schema.
    
    Parameter
    ----------
    columns_list: <list>
    
        A list of wanted columns to be selected from Twitter data.
        
    Output
    ------
    <gpd.GeoDataFrame>
        A processed GeoDataFrame.
     
    """
    
    # Read in geotagged Twitter data
    with open(r"C:\LocalData\smassine\Gradu\Data\Twitter\Luxemburg_border_region\API\Geotagged\TwitterAPI_lux_geotagged_no_bots.pkl", "rb") as f:
        twitter_data = pickle.load(f)
    
    # Select columns that we are interested
    twitter_data = twitter_data[columns_list]
    
    # Some columns in our GeoDataFrame are dictionaries. Hence, there are information in those columns that need to be inserted into own columns
    # Let's process them
    twitter_data['post_loc_name'] = None
    twitter_data['post_loc_country'] = None
    twitter_data['userid'] = None
    twitter_data['user_name'] = None
    twitter_data['screen_name'] = None
    twitter_data['user_flag_home'] = None
    twitter_data['utc_offset'] = None
    twitter_data['time_zone'] = None
    
    for index, row in twitter_data.iterrows():
        
        print("Processing... ", index, "/", len(twitter_data))
        
        if row['place'] == None:
            continue
        else:
            twitter_data.loc[index, 'post_loc_name'] = row['place']['name']
            twitter_data.loc[index, 'post_loc_country'] = row['place']['country']
        if row['user'] == None:
            continue
        else:
            twitter_data.loc[index, 'userid'] = row['user']['id']
            twitter_data.loc[index, 'user_name'] = row['user']['name']
            twitter_data.loc[index, 'screen_name'] = row['user']['screen_name']
            twitter_data.loc[index, 'user_flag_home'] = row['user']['location']
            twitter_data.loc[index, 'utc_offset'] = row['user']['utc_offset']
            twitter_data.loc[index, 'time_zone'] = row['user']['time_zone']
    
    # Delete unnecessary columns
    del twitter_data['place']
    del twitter_data['user']
    
    # Save with Pickle
    twitter_data.to_pickle(r'C:\LocalData\smassine\Gradu\Data\Twitter\Luxemburg_border_region\API\Geotagged\TwitterAPI_lux_geotagged_no_bots_schema.pkl')
    
    return(twitter_data)

# Get wanted user schema out of geotagged data
selected_columns = ['coordinates', 'created_at', 'id', 'lang', 'place', 'source', 'text', 'user']
data = getUserSchema(selected_columns)

## 4. PRODUCE HASHED PSEUDO-IDS
def createPseudoIDs(gdf, userid_col):
    
    """
    A Function for creating hashed pseudo-ids.
    
    Parameters
    ----------
    gdf: <gpd.GeoDataFrame>
    
        A GeoDataFrame in which pseudo-ids will be created.
        
    userid_col: <str>
    
        Userid column name.
        
    Output
    ------
    <gpd.GeoDataFrame>
        A GeoDataFrame with hashed pseudo-ids.
        Original userid's removed.
     
    """
    
    # Drop null 'userid' values
    data_filtered = gdf.dropna(axis=0, subset=[userid_col])
    
    # Create a column for hashed pseudo-ids
    data_filtered['pseudoid'] = None
    
    # Group the dataframe by 'userid'
    grouped = data_filtered.groupby(userid_col)
    
    # Store user count to a variable
    user_count = len(grouped)
    
    # Create a list of hashed pseudo-ids
    # This will return a list of len(user_count) numbers selected from the range 0 to 999999, without duplicates
    pseudo_id_list = random.sample(range(1000000), user_count)
    
    # Accessory variables
    x = 0
    y = 1
    gdf = gpd.GeoDataFrame()
    
    # Iterate over users and create hashed pseudoids
    for key, values in grouped:
            
        print("Processing:", y, "/", len(grouped))
        y = y + 1
            
        individual = values
        individual['pseudoid'] = pseudo_id_list[x]
        x = x + 1
        
        gdf = gdf.append(individual)
    
    gdf = gdf.reset_index()
    gdf = gdf.drop(columns=['index', userid_col])
    gdf = gdf.rename(columns = {"pseudoid": "userid"})
    
    # Save with Pickle
    twitter_data.to_pickle(r'C:\LocalData\smassine\Gradu\Data\Twitter\Luxemburg_border_region\API\Geotagged\TwitterAPI_lux_geotagged_no_bots_schema_pseudo.pkl')
    
    return(gdf)

# Read in geotagged Twitter data
with open(r"C:\LocalData\smassine\Gradu\Data\Twitter\Luxemburg_border_region\API\Geotagged\TwitterAPI_lux_geotagged_no_bots_schema.pkl", "rb") as f:
    twitter_data = pickle.load(f)

# Create pseudo-ids
pseudo_df = createPseudoIDs(twitter_data, 'userid')
