"""
A script for collecting Twitter user's tweet history based on user ID.

Using Tweepy module and user_timeline function for gathering information.

@author: smassine
"""

# Import necessary libraries
import tweepy
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
import os
import pickle

CONSUMER_KEY = "XXXXXXXXXXXXXXXXXXXXXXXXX"
CONSUMER_SECRET = "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"
ACCESS_TOKEN = "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"
ACCESS_TOKEN_SECRET = "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"

auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)
api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)

#data = api.rate_limit_status()
#print(data['resources']['statuses']['/statuses/user_timeline'])

# Read in the list of userids that have posted in Luxembourg
user_ids = pd.read_csv(r"C:\LocalData\smassine\Gradu\Luxembourg_twitter_userid_unique_ei_rajan_ylitysta.txt")

def getUserInfo(user_id_list):
    
    """
    A Function for collecting Twitter user's user timeline.
    
    Parameter
    ----------
    user_id_list: <list>
    
        A list of Twitter user IDs.
        
    Output
    ------
    <gpd.GeoDataFrame>
        A GeoDataFrame containing user timelines of the Twitter users specified in the parameter list.
     
    """
    
    # Define root directory for saving data
    root_dir = r"C:\LocalData\smassine\Gradu\Data\Twitter\Luxemburg_border_region\API"
    
    # Store userids that raise errors into lists
    known_errors = []
    unknown_errors =[]
    
    # Define a list for storing user information
    user_info = []
    # Define upper limit for user's tweet count (Twitter has defined it as 3200)
    user_tweet_limit = 3200
    
    # Iterate over the list of userids that have posted in Luxembourg
    for index, row in user_id_list.iterrows():
                
        print("Processing...", index, "/" ,len(user_id_list))
        userid = row['userid']
        
        # Save the progress once in every 100th index
        if index % 100 == 0 and index != 0:
            # Save user information GeoDataFrame as .pkl file
            gdf = gpd.GeoDataFrame(user_info)
            gdf = gdf.set_geometry('coordinates')
            fp = os.path.join(root_dir, "uudet_%s.pkl" % index)
            gdf.to_pickle(fp)
            print("Successfully saved the current acquisition process!")
            # Also save errors lists if necessary
            if len(known_errors) > 0:
                with open(r"C:\LocalData\smassine\Gradu\Data\Twitter\Luxemburg_border_region\API\known_errors.txt", "w") as output:
                    output.write(str(known_errors))
                    print("Successfully saved the known_errors list!")
            if len(unknown_errors) > 0:
                with open(r"C:\LocalData\smassine\Gradu\Data\Twitter\Luxemburg_border_region\API\unknown_errors.txt", "w") as output:
                    output.write(str(unknown_errors))
                    print("Successfully saved the unknown_errors list!")
            
        try:
        
            # Get a collection of the most recent Tweets posted by the user. The method can return max 3200 of a user’s most recent Tweets.
            for status in tweepy.Cursor(api.user_timeline, id=userid).items(user_tweet_limit):
                                    
                status = status._json
                
                if status['coordinates'] != None:
                    status['coordinates'] = Point(status['coordinates']['coordinates'])
                    user_info.append(status)
                else:
                    user_info.append(status)
                        
        except tweepy.TweepError as e:
            print(e.reason)
            if e.response.status_code == 401 or e.response.status_code == 404:
                error_element = {"error": e.response.status_code, "userid": row['userid']}
                known_errors.append(error_element)
            else:
                error_element = {"error": e.response.status_code, "userid": row['userid']}
                unknown_errors.append(error_element)
            
    # Convert JSON list into GeoDataFrame and pickle it for file storage    
    print("Converting to GeoDataFrame...")
    gdf = gpd.GeoDataFrame(user_info)
    gdf = gdf.set_geometry('coordinates')
    
    # Append newly gathered data to data storing list
    #twitter_api_list.append(gdf)
    #data_combined = gpd.GeoDataFrame(pd.concat(twitter_api_list, ignore_index=True))
    
    gdf.to_pickle(r"C:\LocalData\smassine\Gradu\Data\Twitter\Luxemburg_border_region\API\Twitter_total_set.pkl")
    
    if len(known_errors) > 0:
        with open(r"C:\LocalData\smassine\Gradu\Data\Twitter\Luxemburg_border_region\API\known_errors.txt", "w") as output:
            output.write(str(known_errors))
            print("Successfully saved the known_errors list!")
    if len(unknown_errors) > 0:
        with open(r"C:\LocalData\smassine\Gradu\Data\Twitter\Luxemburg_border_region\API\unknown_errors.txt", "w") as output:
            output.write(str(unknown_errors))
            print("Successfully saved the unknown_errors list!")
    
    return(gdf)

# Use the function
getUserInfo(user_ids)