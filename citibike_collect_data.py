#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jul 21 11:38:48 2021

@author: haydenlw4
"""

import pandas as pd

def get_monthy_local_citi_df(lat_,lng_,citi_):
    '''
    Filters Citi bike trip DataFrame to have a Start or End Station
    within +/- 0.005 latitude and longitude

    Parameters
    ----------
    lat_ : float
        Latitude.
    lng_ : float
        Longitude.
    citi_ : pandas DataFrame
        Citi bike data with Start Station Latitude, Start Station Longitude,
        End Station Latitude, and End Station Longitude columns.
        
    Returns
    -------
    citi_local : pandas DataFrame
        Citi bike data with Start and End Stations within a square box of
        given latitude and longitude.
    '''
    citi_local = citi_[(((citi_.loc[:,'Start Station Latitude']-lat_)**2 + 
                         (citi_.loc[:,'Start Station Longitude']-lng_)**2)**.5<=.005)
                       |
                       (((citi_.loc[:,'End Station Latitude']-lat_)**2 + 
                         (citi_.loc[:,'End Station Longitude']-lng_)**2)**.5<=.005)]
    return citi_local

def get_year_month_list(start_month, start_year, end_month, end_year):
    year_month_list = []
    ym_start= 12*start_year + start_month - 1
    ym_end= 12*end_year + end_month 
    for ym in range( ym_start, ym_end ):
        y, m = divmod( ym, 12 )
        y = str(y)[2:]
        m = '{0:02d}'.format(m+1)
        year_month_list += [y+m]
    return year_month_list

def get_all_local_citi_df(lat, lng, file_name, 
                          start_month=4, start_year=2016, 
                          end_month=4, end_year=2021):
    
    year_month_list = get_year_month_list(start_month, start_year, 
                                          end_month, end_year)
    citi_locals = list()
    for year_month in year_month_list:
        citi = pd.read_parquet(f'https://s3.amazonaws.com/ctbk/20{year_month}-citibike-tripdata.parquet')
        citi_local = get_monthy_local_citi_df(lat,lng,citi)
        citi_locals.append(citi_local)
    citi_local_all = pd.concat(citi_locals, axis=0, ignore_index=True)
    citi_local_all.to_csv('{file_name}.csv')

    return citi_local_all
# 04-19 is start of 2019 having reports here.
#2020-2021 is all good
lat = 40.7560568
lng = -73.9834879
citi_local_sara = get_all_local_citi_df(lat, lng, 'citi_bike_local_sara')




