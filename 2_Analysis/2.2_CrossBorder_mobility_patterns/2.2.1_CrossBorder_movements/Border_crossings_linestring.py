"""
This script creates movement datasets (LineString) from geo-tagged Twitter datasets (Point).
Trips distance calculations are based on Haversine formula.

@author: smassine
"""

# Import necessary libraries
import pickle
from shapely.geometry import LineString, MultiPolygon, Polygon
from math import cos, sin, asin, sqrt, radians
import geopandas as gpd
import fiona
import sys

def calc_distance(lat1, lon1, lat2, lon2):
    
    """
    Calculate the great circle distance between two points
    on Earth (specified in decimal degrees)
    """
    
    # Convert decimal degrees to radians
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
    # Haversine formula
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
    c = 2 * asin(sqrt(a))
    km = 6371 * c
    return km

# Read country polygons. Create a bbox for Greater Region of Luxembourg
greater_region = gpd.read_file(r"C:\LocalData\smassine\Gradu\Data\GreaterLux\SHP\GreatLux.shp")
greater_region = greater_region.to_crs({'init': 'epsg:4326', 'no_defs': True})
Polygons_1 = list(greater_region['geometry'][1:7])
Polygons_2 = list(greater_region['geometry'][0])
lista = Polygons_1 + Polygons_2
greater_region = MultiPolygon(lista)
greater_region = greater_region.buffer(0)

# Open datasets
with open(r'C:\LocalData\smassine\Gradu\Data\Twitter\Luxemburg_border_region\API\Geotagged\TwitterAPI_lux_home_inside_greater_region.pkl', 'rb') as f:
    data = pickle.load(f)

data.crs = fiona.crs.from_epsg(4326)

data['origCountry'] = None
data['origTime'] = None
data['avgTime'] = None
data['duration'] = None
data['CB_move'] = None
data['distanceKm'] = None

data = data.rename(columns={'coordinates': 'geometry', 'home_unique_weeks': 'homeLoc', 'lux_region_home': 'luxRegion', 'post_country': 'destCountry', 'datetime': 'destTime'})
selected_columns = ['geometry', 'userid', 'homeLoc', 'luxRegion', 'origCountry', 'destCountry', 'origTime' , 'destTime', 'avgTime', 'duration', 'CB_move', 'distanceKm']
selected_data = data[selected_columns]

# Select data subsamples
## Greater region of Luxembourg
greater_lux_region = selected_data.loc[selected_data['homeLoc'] == 'Greater Region of Luxembourg']
## Potential users linked to Greater Region (= Belgium, France or Germany)
wanted_values = ['Belgium', 'France', 'Germany']
potential_users = selected_data.loc[selected_data['homeLoc'].isin(wanted_values)]
## Others (=every other country)
other_values = ['Belgium', 'France', 'Germany', 'Greater Region of Luxembourg']
other_users = selected_data.loc[~selected_data['homeLoc'].isin(other_values)]

def createLineDF(gdf, output_fp_name):
    
    """
    A Function for creating LineString movements from Twitter point data.
    
    Parameters
    ----------
    gdf: <gpd.GeoDataFrame>
    
        Twitter point dataset to be processed.
        
    output_fp_name: <str>
    
        Output filepath for saving, including filename and extension (.pkl).
        Raw (r) strings recommended.
        
    Output
    ------
    <gpd.GeoDataFrame>
        LineString movements GeoDataFrame packed with Pickle
     
    """
    
    gdf.set_geometry(col='geometry')
    
    line_data = gpd.GeoDataFrame(columns=['geometry', 'userid', 'homeLoc', 'luxRegion', 'origCountry', 'destCountry', 'origTime' , 'destTime', 'avgTime', 'duration', 'CB_move', 'distanceKm'], geometry='geometry')
    line_data.crs = fiona.crs.from_epsg(4326)
    
    grouped = gdf.groupby('userid')
    y = 1
    
    for key, values in grouped:
        
        print("Processing:", y, "/", len(grouped))
        y = y + 1
        
        individual = values
        individual = individual.sort_values(by='destTime')
            
        point = 'Empty'
        date_start = 'Empty'
    
        for index, row in individual.iterrows():
            
            if type(point) == str:
                    
                point = row['geometry']
                date_start = row['destTime']
                origCountry = row['destCountry']
                
            elif type(point) != str:
    
                line = LineString([point, row['geometry']])
                length_km = calc_distance(line.xy[1][0], line.xy[0][0], line.xy[1][1], line.xy[0][1])
                
                date_end = row['destTime']
                average_time_delta = (date_end - date_start) / 2
                avgTime = date_start + average_time_delta
                avgTime = avgTime.strftime("%Y-%m-%d-%H")
                duration = date_end - date_start
    
                line_data = line_data.append(row)
                
                line_data.loc[index, 'geometry'] = line
                line_data.loc[index, 'origCountry'] = origCountry
                line_data.loc[index, 'origTime'] = date_start
                line_data.loc[index, 'avgTime'] = avgTime
                line_data.loc[index, 'duration'] = duration
                line_data.loc[index, 'distanceKm'] = length_km
                
                if row['destCountry'] != origCountry:
                    
                    if row['geometry'].within(greater_region) == True and point.within(greater_region) == True:
                        
                        line_data.loc[index, 'CB_move'] = "Inside GRL"
                        
                    elif row['geometry'].within(greater_region) == False and point.within(greater_region) == True:
                        
                        line_data.loc[index, 'CB_move'] = "Outbound from GRL"
                        
                    elif row['geometry'].within(greater_region) == True and point.within(greater_region) == False:
                        
                        line_data.loc[index, 'CB_move'] = "Inbound to GRL"
                    
                    elif row['geometry'].within(greater_region) == False and point.within(greater_region) == False:
                        
                        line_data.loc[index, 'CB_move'] = "Outside GRL"
                    
                    else:
                        
                        print("Something went wrong!")
                        sys.exit()
                        
                elif row['destCountry'] == origCountry:
                    
                    if row['geometry'].within(greater_region) == True and point.within(greater_region) == True:
                        
                        line_data.loc[index, 'CB_move'] = "Inside GRL, no CB"
                    
                    elif row['geometry'].within(greater_region) == False and point.within(greater_region) == True:
                        
                        line_data.loc[index, 'CB_move'] = "Partly inside GRL, no CB"
                        
                    elif row['geometry'].within(greater_region) == True and point.within(greater_region) == False:
                        
                        line_data.loc[index, 'CB_move'] = "Partly inside GRL, no CB"
                    
                    elif row['geometry'].within(greater_region) == False and point.within(greater_region) == False:
                        
                        line_data.loc[index, 'CB_move'] = "Outbound from GRL, no CB"
                        
                    else:
                        
                        print("Something went wrong!")
                        sys.exit()
                
                else:
                    
                    print("Something went wrong!")
                    sys.exit()
                        
                point = row['geometry']
                date_start = row['destTime']
                origCountry = row['destCountry']
    
    line_data.to_pickle(output_fp_name)
    
    return(line_data)

greater_region = createLineDF(greater_lux_region, r"C:\LocalData\smassine\Gradu\Data\Twitter\Luxemburg_border_region\API\Geotagged\LineString\Greater_Region.pkl")
potentials = createLineDF(potential_users, r"C:\LocalData\smassine\Gradu\Data\Twitter\Luxemburg_border_region\API\Geotagged\LineString\Potentials.pkl")
others = createLineDF(other_users, r"C:\LocalData\smassine\Gradu\Data\Twitter\Luxemburg_border_region\API\Geotagged\LineString\Others.pkl")
