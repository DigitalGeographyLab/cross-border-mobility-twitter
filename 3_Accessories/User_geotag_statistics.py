"""
A script informing how many geotagged posts and ungeotagged post each individual has in a Twitter dataset.
Also, telling the geotagged rows ratio in relation to all posts by user.

@author: smassine
"""
import pickle
import pandas as pd
import json

def getUserGeotagStatistics(fp):
    
    with open(fp, "rb") as f:
        data = pickle.load(f)
    
    selected_columns = ['coordinates', 'created_at', 'user']
    copy_data = data[selected_columns]
    
    geotag_count = 0
    no_geotag_count = 0
    statistics_list = []
    
    for index, row in copy_data.iterrows():
        
        print("Processing: ", index, "/", len(copy_data))
        
        if index == 0:
            last_user = row['user']['id']
            print("Assigned first user: ", last_user)
        
        if str(row['user']['id']) == str(last_user):
            
            if (pd.isnull(row['coordinates'])) == True:
                no_geotag_count = no_geotag_count + 1
                
                if index == len(copy_data) - 1:
                    average = (geotag_count / (geotag_count + no_geotag_count)) * 100
                    list_element = {"userid": last_user, "geotagged_rows": geotag_count, "other_rows": no_geotag_count, "geotags_percent": average}
                    statistics_list.append(list_element)
                    print("The end!")
                    break
                
            else:
                geotag_count = geotag_count + 1
                
                if index == len(copy_data) - 1:
                    average = (geotag_count / (geotag_count + no_geotag_count)) * 100
                    list_element = {"userid": last_user, "geotagged_rows": geotag_count, "other_rows": no_geotag_count, "geotags_percent": average}
                    statistics_list.append(list_element)
                    print("The end!")
                    break
            
        else:
            
            try:
                average = (geotag_count / (geotag_count + no_geotag_count)) * 100
            except:
                ZeroDivisionError
                average = "Couldn't calculate (zero division)"
                
            list_element = {"userid": last_user, "geotagged_rows": geotag_count, "other_rows": no_geotag_count, "geotags_percent": average}
            statistics_list.append(list_element)
            print("User", last_user," appended to list! Moving on to next user...")
                
            last_user = row['user']['id']
            geotag_count = 0
            no_geotag_count = 0
    
    return(statistics_list)
        
lista_2 = getUserGeotagStatistics(r"C:\LocalData\smassine\Gradu\Data\Twitter\Luxemburg_border_region\API\Twitter_userid_601_1700.pkl")
lista_3 = getUserGeotagStatistics(r"C:\LocalData\smassine\Gradu\Data\Twitter\Luxemburg_border_region\API\Twitter_userid_1701_2283.pkl")
lista_4 = getUserGeotagStatistics(r"C:\LocalData\smassine\Gradu\Data\Twitter\Luxemburg_border_region\API\Twitter_userid_2284_3349.pkl")
lista_5 = getUserGeotagStatistics(r"C:\LocalData\smassine\Gradu\Data\Twitter\Luxemburg_border_region\API\Twitter_userid_3350_4020.pkl")

output_file_2 = open(r"C:\LocalData\smassine\Gradu\Tilastot\user_geotag_statistics_2.txt", 'w', encoding='utf-8')
output_file_3 = open(r"C:\LocalData\smassine\Gradu\Tilastot\user_geotag_statistics_3.txt", 'w', encoding='utf-8')
output_file_4 = open(r"C:\LocalData\smassine\Gradu\Tilastot\user_geotag_statistics_4.txt", 'w', encoding='utf-8')
output_file_5 = open(r"C:\LocalData\smassine\Gradu\Tilastot\user_geotag_statistics_5.txt", 'w', encoding='utf-8')

for dic in lista_2:
   json.dump(dic, output_file_2) 
   output_file_2.write("\n")

for dic in lista_3:
   json.dump(dic, output_file_3) 
   output_file_3.write("\n")

for dic in lista_4:
   json.dump(dic, output_file_4) 
   output_file_4.write("\n")

for dic in lista_5:
   json.dump(dic, output_file_5) 
   output_file_5.write("\n")
    
#lista = open(r"C:\LocalData\smassine\Gradu\Tilastot\user_geotag_statistics.txt" ,"r")
#lines = lista.read().split("\n")
