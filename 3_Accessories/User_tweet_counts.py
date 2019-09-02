"""
A script for counting Twitter user's tweets.

@author: smassine
"""

def getUserTweetCounts(fp, ind):
    
    """
    A Function counting Twitter user's tweets.
    
    Parameters
    ----------
    fp: <str>
    
        Filepath to data collected from Twitter API using Tweepy (extension .pkl).
        Raw (r) strings recommended.
        
    ind: <int>
    
        An index for naming output list.
        
    Output
    ------
    <list>
        A txt-file containing tweet counts.
     
    """
    
    # Import necessary libraries
    import pickle
    import os
    
    # Read social media dataset in as GeoDataFrame
    with open(fp, 'rb') as f:
        data = pickle.load(f)
    
    user_tweet_counts = []
    count = 0
    
    for index, row in data.iterrows():
        
        print("Processing: ", index, "/", len(data))
        
        if index == 0:
            last_user = row['user']['id']
            count = count + 1
        
        if str(row['user']['id']) == str(last_user):
            count = count + 1
            
        else:    
            user_tweet_counts.append(count)
            last_user = row['user']['id']
            count = 1
    
    root_dir = r"C:\LocalData\smassine\Gradu\Tilastot"
    fp = os.path.join(root_dir, "user_tweet_counts_%s.txt" % str(ind))
    
    with open(fp, 'w') as f:
        for item in user_tweet_counts:
            f.write("%s\n" % item)
    
    return(user_tweet_counts)

lista_1 = getUserTweetCounts(r"C:\LocalData\smassine\Gradu\Data\Twitter\Luxemburg_border_region\API\Twitter_userid_0_600.pkl", 1)
lista_2 = getUserTweetCounts(r"C:\LocalData\smassine\Gradu\Data\Twitter\Luxemburg_border_region\API\Twitter_userid_601_1700.pkl", 2)
lista_3 = getUserTweetCounts(r"C:\LocalData\smassine\Gradu\Data\Twitter\Luxemburg_border_region\API\Twitter_userid_1701_2283.pkl", 3)
lista_4 = getUserTweetCounts(r"C:\LocalData\smassine\Gradu\Data\Twitter\Luxemburg_border_region\API\Twitter_userid_2284_3349.pkl", 4)
lista_5 = getUserTweetCounts(r"C:\LocalData\smassine\Gradu\Data\Twitter\Luxemburg_border_region\API\Twitter_userid_3350_4020.pkl", 5)

merged_list = lista_1 + lista_2 + lista_3 + lista_4 + lista_5

with open(r'C:\LocalData\smassine\Gradu\Tilastot\user_tweet_counts.txt', 'w') as f:
    for item in merged_list:
        f.write("%s\n" % item)