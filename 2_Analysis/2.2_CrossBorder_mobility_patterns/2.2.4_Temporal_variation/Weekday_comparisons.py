"""
A script for identifying Twitter user's cross-border movement activities on weekday level.
Calculating average weekday activity and variance.

@author: smassine
"""

import pandas as pd
import geopandas as gpd
import os

# Open LineString datasets
bel = gpd.read_file(r"C:\LocalData\smassine\Gradu\Data\Twitter\Luxemburg_border_region\API\Geotagged\LineString\MoverColumn\SHP\InUse\bel-lux.shp")
fra = gpd.read_file(r"C:\LocalData\smassine\Gradu\Data\Twitter\Luxemburg_border_region\API\Geotagged\LineString\MoverColumn\SHP\InUse\fra-lux.shp")
ger = gpd.read_file(r"C:\LocalData\smassine\Gradu\Data\Twitter\Luxemburg_border_region\API\Geotagged\LineString\MoverColumn\SHP\InUse\ger-lux.shp")

def calculateWeekdays(gdf, move_type, file_name):
    
    """
    A Function calculating weekday average activity and variance.
    
    Parameters
    ----------
    gdf: <gpd.GeoDataFrame>
    
        A GeoDataFrame containing Twitter user's cross-border movements as LineString geometries.
        
    move_type: <str>
    
        Cross-border mover type based on binary classification.
        Required: either "Daily cross-border mover" or "Infrequent border crosser".
        
    file_name: <str>
    
        Output filename with extension (.csv).
        
    Output
    ------
    <Python dict, csv-file>
        A csv-file containing weekday temporal variation information.
     
    """
    
    # Select by mover type
    gdf_type = gdf.loc[gdf['moverType'] == move_type]
    gdf_type = gdf_type.reset_index()
    
    # Create weekday variables
    mondays = 0
    tuesdays = 0
    wednesdays = 0
    thursdays = 0
    fridays = 0
    saturdays = 0
    sundays = 0

    # Iterate over a LineString dataset, calculate average weekdays and variance
    for index, row in gdf_type.iterrows():
        
        print("Processing:", index, "/", len(gdf_type) - 1)
        
        # Store weekdays from trip endpoints, convert to timestamps
        day_name_orig = pd.to_datetime(row['origTime']).strftime("%A")
        day_name_dest = pd.to_datetime(row['destTime']).strftime("%A")
        
        # Count weekdays
        if any( [day_name_orig == 'Monday', day_name_dest == 'Monday'] ):
            mondays = mondays + 1
    
        if any( [day_name_orig == 'Tuesday', day_name_dest == 'Tuesday'] ):
            tuesdays = tuesdays + 1
    
        if any( [day_name_orig == 'Wednesday', day_name_dest == 'Wednesday'] ):
            wednesdays = wednesdays + 1
    
        if any( [day_name_orig == 'Thursday', day_name_dest == 'Thursday'] ):
            thursdays = thursdays + 1
    
        if any( [day_name_orig == 'Friday', day_name_dest == 'Friday'] ):
            fridays = fridays + 1
        
        if any( [day_name_orig == 'Saturday', day_name_dest == 'Saturday'] ):
            saturdays = saturdays + 1
            
        if any( [day_name_orig == 'Sunday', day_name_dest == 'Sunday'] ):
            sundays = sundays + 1
    
    avg = (mondays + tuesdays + wednesdays + thursdays + fridays + saturdays + sundays) / 7
    dictionary = {'average': avg, 'mondays': mondays, 'tuesdays': tuesdays, 'wednesdays': wednesdays, 'thursdays': thursdays, 'fridays': fridays, 'saturdays': saturdays, 'sundays': sundays}
    
    print("")
    print("Result:", dictionary)
    print("")
    
    rootdir = r"C:\LocalData\smassine\Gradu\Tilastot\Temporal"
    filepath = os.path.join(rootdir, file_name)
    
    with open(filepath, 'w') as f:
        for key in dictionary.keys():
            f.write("%s,%s\n"%(key, dictionary[key]))
    
    return dictionary

bel_cbc_result = calculateWeekdays(bel, 'Daily cross-border mover', 'bel_cbc_temporal.csv')
bel_ibc_result = calculateWeekdays(bel, 'Infrequent border crosser', 'bel_ibc_temporal.csv')
fra_cbc_result = calculateWeekdays(fra, 'Daily cross-border mover', 'fra_cbc_temporal.csv')
fra_ibc_result = calculateWeekdays(fra, 'Infrequent border crosser', 'fra_ibc_temporal.csv')
ger_cbc_result = calculateWeekdays(ger, 'Daily cross-border mover', 'ger_cbc_temporal.csv')
ger_ibc_result = calculateWeekdays(ger, 'Infrequent border crosser', 'ger_ibc_temporal.csv')
