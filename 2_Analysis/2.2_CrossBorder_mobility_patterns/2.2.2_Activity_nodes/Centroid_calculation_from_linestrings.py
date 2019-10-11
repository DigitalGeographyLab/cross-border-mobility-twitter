"""
This script calculates median and mean centroids from point datasets.
Used for identifying Twitter user's activity nodes (i.e. daily life
spaces of people).

@author: smassine
"""

import pickle
import pandas as pd
import geopandas as gpd
from shapely.geometry import MultiPoint, Point
import sys
import numpy as np

# Open dataset
with open(r'C:\LocalData\smassine\Gradu\Data\Twitter\Luxemburg_border_region\API\Geotagged\LineString\MoverColumn\Greater_Region_daily_crs_update.pkl', 'rb') as f:
    data = pickle.load(f)

# Read country polygons and create bboxes for different country areas inside the Greater Region of Luxembourg
country_polygons = gpd.read_file(r"C:\LocalData\smassine\Gradu\Data\GreaterLux\SHP\Global_regions_GreatLux.shp")
# Luxembourg
lux = country_polygons.loc[country_polygons['Country'] == 'Luxembourg']
lux = lux.to_crs({'init': 'epsg:4326', 'no_defs': True})
# The Greater Region of Luxembourg
bbox = country_polygons.loc[country_polygons['GreaterLux'] == 1]
bbox = bbox.to_crs({'init': 'epsg:4326', 'no_defs': True})

for index, row in lux.iterrows():
    
    if index == 344:
        lux = row['geometry']
    else:
        lux = lux.union(row['geometry'])

for index, row in bbox.iterrows():
    
    if index == 41:
        greater_region = row['geometry']
    else:
        greater_region = greater_region.union(row['geometry'])

# The Greater Region without Luxembourg
greater_region = greater_region - lux
        
# Filter data based on dominance area inside the Greater Region
belgium_gr = data.loc[data['domArea'] == 'Belgium']
france_gr = data.loc[data['domArea'] == 'France']
germany_gr = data.loc[data['domArea'] == 'Germany']
luxembourg_gr = data.loc[data['domArea'] == 'Luxembourg']

# Create MultiPoint sets for centroid calculation
def getPointSetCentorid(gdf, group_users, calc_method):
    
    """
    A Function for calculating centroids from point datasets (Twitter activity nodes)
    
    Parameters
    ----------
    gdf: <gpd.GeoDataFrame>
    
        A GeoDataFrame to base the calculations on.
    
    group_users: <boolean>
    
        Either True or False.
        If True, centroids will be calculated for each TWITTER USER.
        If False, centroids will be calculated for each COUNTRY.
    
    calc_method: <str>
        
        Calculation method for point set centroids.
        Must be either 'median' or 'mean'.
        
    Output
    ------
    <pd.DataFrame>
        A DataFrame containing centroids.
     
    """
    
    gdf = gdf.reset_index()
    
    point_list = []
    user_centroids_list = []
    listarray_x = []
    listarray_y = []
    
    if group_users == False:
        
        for index, row in gdf.iterrows():
            
            print("Processing:", index, "/", len(gdf))
                
            start_point = Point(row['geometry'].coords.xy[0][0], row['geometry'].coords.xy[1][0])
            end_point = Point(row['geometry'].coords.xy[0][1], row['geometry'].coords.xy[1][1])
                
            if start_point.within(greater_region) == True:
                
                point_list.append(start_point)
                listarray_x.append([start_point.coords.xy[0]])
                listarray_y.append([start_point.coords.xy[1]])
                
            if end_point.within(greater_region) == True:
                
                point_list.append(end_point)
                listarray_x.append([end_point.coords.xy[0]])
                listarray_y.append([end_point.coords.xy[1]])

        if len(point_list) > 0:
            point_set = MultiPoint(point_list)
        
            if calc_method == 'mean':
            
                point_set_centroid = point_set.centroid
            
            elif calc_method == 'median':
                
                array_x = np.array(listarray_x)
                array_y = np.array(listarray_y)
                point_set_centroid = [np.median(array_x), np.median(array_y)]
    
            else:
        
                print("calc_method parameter must be either 'mean' or 'median'!")
                sys.exit()
        
        else:
            
            print("No points in array!")
            sys.exit()
            
        return(point_set_centroid)
    
    elif group_users == True:
        
        grouped = gdf.groupby('userid')
        y = 0
        
        for key, values in grouped:
        
            print("Processing:", y, "/", len(grouped))
            y = y + 1
            individual = values
            
            for index, row in individual.iterrows():
                
                start_point = Point(row['geometry'].coords.xy[0][0], row['geometry'].coords.xy[1][0])
                end_point = Point(row['geometry'].coords.xy[0][1], row['geometry'].coords.xy[1][1])
                    
                if start_point.within(greater_region) == True:
                    
                    point_list.append(start_point)
                    listarray_x.append([start_point.coords.xy[0]])
                    listarray_y.append([start_point.coords.xy[1]])
            
                if end_point.within(greater_region) == True:
                    
                    point_list.append(end_point)
                    listarray_x.append([end_point.coords.xy[0]])
                    listarray_y.append([end_point.coords.xy[1]])
            
            if len(point_list) > 0:
                user_point_set = MultiPoint(point_list)
                point_list = []
            
                if calc_method == 'mean':
                
                    user_centroid = user_point_set.centroid
                
                elif calc_method == 'median':
                
                    array_x = np.array(listarray_x)
                    array_y = np.array(listarray_y)
                    user_centroid = [np.median(array_x), np.median(array_y)]
                    listarray_x = []
                    listarray_y = []
                    
                else:
                
                    print("calc_method parameter must be either 'mean' or 'median'!")
                    sys.exit()
                
                list_element = {'geometry': user_centroid, 'userid': key, 'mover': individual.moverType.unique()[0]}
                user_centroids_list.append(list_element)
                
        df = pd.DataFrame(user_centroids_list)
        df = df.rename(columns={0: 'geometry'})
        
        return(df)
        
    else:
        
        print("group_users parameter must be boolean!")
        sys.exit()

# Calculate centorids for all points
belgium_centroid_all = getPointSetCentorid(belgium_gr, False, 'median')
france_centroid_all = getPointSetCentorid(france_gr, False, 'median')
germany_centroid_all = getPointSetCentorid(germany_gr, False, 'median')
luxembourg_centroid_all = getPointSetCentorid(luxembourg_gr, False, 'median')

all_centroid = [{'geometry': belgium_centroid_all, 'country': 'Belgium', 'all_code': 1, 'userid': None, 'mover': None},
                {'geometry': france_centroid_all, 'country': 'France', 'all_code': 1, 'userid': None, 'mover': None},
                {'geometry': luxembourg_centroid_all, 'country': 'Luxembourg', 'all_code': 1, 'userid': None, 'mover': None},
                {'geometry': germany_centroid_all, 'country': 'Germany', 'all_code': 1, 'userid': None, 'mover': None}]

all_centroids_list = pd.DataFrame(all_centroid)
all_centroids_list['geometry'] = all_centroids_list['geometry'].apply(Point)

# Calculate centroids on user level
belgium_centroids_list = getPointSetCentorid(belgium_gr, True, 'median')
france_centroids_list = getPointSetCentorid(france_gr, True, 'median')
germany_centroids_list = getPointSetCentorid(germany_gr, True, 'median')
luxembourg_centroids_list = getPointSetCentorid(luxembourg_gr, True, 'median')

belgium_centroids_list['country'] = 'Belgium'
france_centroids_list['country'] = 'France'
germany_centroids_list['country'] = 'Germany'
luxembourg_centroids_list['country'] = 'Luxembourg'

belgium_centroids_list['all_code'] = 0
france_centroids_list['all_code'] = 0
germany_centroids_list['all_code'] = 0
luxembourg_centroids_list['all_code'] = 0

all_data = gpd.GeoDataFrame(pd.concat([belgium_centroids_list, france_centroids_list, germany_centroids_list, luxembourg_centroids_list], ignore_index=True))
all_data['geometry'] = all_data['geometry'].apply(Point)

full = gpd.GeoDataFrame(pd.concat([all_data, all_centroids_list], ignore_index=True))

full.to_file(r"C:\LocalData\smassine\Gradu\Data\Twitter\Luxemburg_border_region\API\Geotagged\HomeAggregation\luxRegion\medians_lux_ext.shp")
