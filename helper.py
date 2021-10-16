#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Oct 15 13:33:49 2021

@author: haydenlw4
"""

import pandas as pd
import geopy

def get_year_month_list(start_month, start_year, end_month, end_year):
    '''
    Gets list of years and month, as a string e.g. '1403' for March of 2014, 
    between the given start month/year and end month/year

    Parameters
    ----------
    start_month: int 1-12
    start_year: int 2000+
    end_month: int 1-12
    end_year: int 2000+
    
    Returns
    -------
    year_month_list : list of strings
    '''
    year_month_list = []
    ym_start= 12*start_year + start_month - 1
    ym_end= 12*end_year + end_month 
    for ym in range(ym_start, ym_end):
        y, m = divmod(ym, 12)
        y = str(y)[2:]
        m = '{0:02d}'.format(m+1)
        year_month_list += [y+m]
    return year_month_list

def get_all_citi_df(start_month=4, start_year=2016, 
                          end_month=4, end_year=2021):
    '''
    Gets all Citi bike trip DataFrames within a given date range and 
    concats them together

    Parameters
    ----------
    start_month: int 1-12
    start_year: int 2016-
    end_month: int 1-12
    end_year: int -2021
        
    Returns
    -------
    citi_all : pandas DataFrame
    '''
    year_month_list = get_year_month_list(start_month, start_year, 
                                          end_month, end_year)
    citis = list()
    for year_month in year_month_list:
        citi = pd.read_parquet(f'https://s3.amazonaws.com/ctbk/20{year_month}-citibike-tripdata.parquet')
        citis.append(citi)
    citi_all = pd.concat(citis, axis=0, ignore_index=True)
    return citi_all

def get_zipcode(df, geolocator, lat_field, lon_field):
    '''
    For eacGets zipcode from given latitude and longitude.

    Parameters
    ----------
    df: pandas DataFrame
    geolocator: geopy.Nominatim object
    lat_field: string
        column name where latitude value is held
    lon_field: string
        column name where longitude value is held
    Returns
    -------
    location.raw['address']['postcode'] : string
        zipcode of location.
    '''
    location = geolocator.reverse((df[lat_field], df[lon_field]))
    try:
        return location.raw['address']['postcode']
    except KeyError:
        return 'XXXXXXXXX'

def update_zip_code_dict(citi_, id_to_zipcode_={}):
    '''
    Checks if any station id's don't have a pre existing zipcode match. 
    updates the zipcode dictionary if there are new matches. 

    Parameters
    ----------
    citi_: pandas DataFrame
    id_to_zipcode_: dictionary
        
    Returns
    -------
    id_to_zipcode_ : dictionary
    '''
    current_id_to_zipcode = list(id_to_zipcode_.keys())
    citi_ = citi_[~citi_['End Station ID'].isin(current_id_to_zipcode)]
    citi_zip = citi_.drop_duplicates(subset=['End Station ID'])[['End Station ID',
                                                                    'End Station Latitude',
                                                                    'End Station Longitude']]
    try:
        geolocator = geopy.Nominatim(user_agent='testing_for_citi_bike')
        citi_zip['zipcodes'] = citi_zip.apply(get_zipcode, axis=1, geolocator=geolocator, 
                                            lat_field='End Station Latitude', 
                                            lon_field='End Station Longitude')
        
        id_to_zipcode_.update(dict(zip(citi_zip['End Station ID'], 
                                      citi_zip['zipcodes'])))
    except ValueError:
        print('No new ZIP codes.')
    # Updating station id's with correct zipcodes that were manually checked. 
    id_to_zipcode_.update({150:'10009',151:'10012',216:'11201',
                          217:'11201',250:'10012',252:'10011',
                          265:'10002',298:'11217',367:'10022',
                          386:'10013',387:'10007',391:'11201',
                          392:'11201',400:'10002',422:'10023',
                          434:'10011',455:'10017',473:'10002',
                          479:'10036',501:'10016',509:'10011',
                          514:'10018',516:'10017',530:'10069',
                          536:'10016',3137:'10021',3141:'10065',
                          3158:'10023',3160:'10024',3163:'10023',
                          3292:'10029',3299:'10029',3309:'10029',
                          3336:'10029',3341:'10025',3342:'11231',
                          3343:'10025',3366:'10025',3383:'10025',
                          3394:'11231',3443:'10104',3472:'10011',
                          3541:'10027',3676:'11231',3686:'10014',
                          3744:'10003',3746:'10013',3809:'10019',
                          3921:'10454',3924:'10451',3942:'10027',
                          3983:'10455',4045:'10069',4073:'10019',    
                          4090:'10451',4113:'10035',4115:'10035',
                          4121:'10016',4136:'10451',3263:'10003',
                          4478:'10003'  
                          })
    return id_to_zipcode_

