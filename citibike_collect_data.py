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
    # citi_locals = list()
    citis = list()

    for year_month in year_month_list:
        citi = pd.read_parquet(f'https://s3.amazonaws.com/ctbk/20{year_month}-citibike-tripdata.parquet')
        # citi_local = get_monthy_local_citi_df(lat,lng,citi)
        # citi_locals.append(citi_local)
        citis.append(citi)
    citi_all = pd.concat(citis, axis=0, ignore_index=True)
    citi_all.to_csv('{file_name}.csv')
    return citi_all

    # citi_local_all = pd.concat(citi_locals, axis=0, ignore_index=True)
    # citi_local_all.to_csv('{file_name}.csv')
    # return citi_local_all
# 04-19 is start of 2019 having reports here.
#2020-2021 is all good
lat = 40.7560568
lng = -73.9834879
citi_last_year = get_all_local_citi_df(lat, lng, 'citi_last_year',
                                        start_month=4, start_year=2020)


station_zipcode1 = pd.read_csv(r'station_zipcode.csv',
                   index_col=0,
                              )
zipcode_dict = dict(zip(station_zipcode1.station_id, station_zipcode1.zipcode))

import geopy

def get_zipcode(df, geolocator, lat_field, lon_field):
    location = geolocator.reverse((df[lat_field], df[lon_field]))
    try:
        return location.raw['address']['postcode']
    except KeyError:
        return 'XXXXXXXXX'

def update_zip_code_dict(citi_, id_to_zipcode_={}):
    print('updatezipcode dict')
    current_id_to_zipcode = list(id_to_zipcode_.keys())
    citi_ = citi_[~citi_['End Station ID'].isin(current_id_to_zipcode)]
    citi_zip = citi_.drop_duplicates(subset=['End Station ID'])[['End Station ID',
                                                                    'End Station Latitude',
                                                                    'End Station Longitude']]
    try:
        print('get zipcode')
        geolocator = geopy.Nominatim(user_agent='testing_for_citi_bike')
        citi_zip['zipcodes'] = citi_zip.apply(get_zipcode, axis=1, geolocator=geolocator, 
                                            lat_field='End Station Latitude', 
                                            lon_field='End Station Longitude')
        
        id_to_zipcode_.update(dict(zip(citi_zip['End Station ID'], 
                                      citi_zip['zipcodes'])))
    except ValueError:
        print('No new ZIP codes.')
    return id_to_zipcode_


id_to_zipcode = update_zip_code_dict(citi_last_year,zipcode_dict)

citi_last_year["start_zipcode"] = citi_last_year["Start Station ID"].map(id_to_zipcode)
citi_last_year["end_zipcode"] = citi_last_year["End Station ID"].map(id_to_zipcode)

id_to_zipcode.update({150:'10009',
                      151:'10012',
                      216:'11201',
                      217:'11201',
                      250:'10012',
                      252:'10011',
                      265:'10002',
                      298:'11217',
                      367:'10022',
                      386:'10013',
                      387:'10007',
                      391:'11201',
                      392:'11201',
                      400:'10002',
                      422:'10023',
                      434:'10011',
                      455:'10017',
                      473:'10002',
                      479:'10036',
                      501:'10016',
                      509:'10011',
                      514:'10018',
                      516:'10017',
                      530:'10069',
                      536:'10016',
                      3137:'10021',
                      3141:'10065',
                      3158:'10023',
                      3160:'10024',
                      3163:'10023',
                      3292:'10029',
                      3299:'10029',
                      3309:'10029',
                      3336:'10029',
                      3341:'10025',
                      3342:'11231',
                      3343:'10025',
                      3366:'10025',    
                      3383:'10025',
                      3394:'11231',
                      3443:'10104',
                      3472:'10011',
                      3541:'10027',
                      3676:'11231',
                      3686:'10014',
                      3744:'10003',
                      3746:'10013',
                      3809:'10019',
                      3921:'10454',
                      3924:'10451',
                      3942:'10027',
                      3983:'10455',
                      4045:'10069',
                      4073:'10019',    
                      4090:'10451',
                      4113:'10035',
                      4115:'10035',
                      4121:'10016',
                      4136:'10451',
                      4478:'10003'  
                      })

id_to_zipcode_df = pd.DataFrame(id_to_zipcode.items(), columns=['station_id', 'zipcode'])
id_to_zipcode_df.to_csv('station_zipcode.csv')


import geopandas
nyc_zip_df = geopandas.read_file("ZIP_CODE_040114.shp")  
nyc_zip_list = list(nyc_zip_df.ZIPCODE)
dicts_to_fix = id_to_zipcode_df[~id_to_zipcode_df['zipcode'].isin(nyc_zip_list)]

dicts_to_fix=dicts_to_fix.merge(citi_last_year, how='inner',
                   left_on='station_id',right_on='End Station ID')
dicts_to_fix = dicts_to_fix.drop_duplicates('station_id')








