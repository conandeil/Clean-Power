import logging
import os
import posixpath
import urllib.parse
import urllib.request
import re
import zipfile
import pickle

import numpy as np
import pandas as pd
import utm  # for transforming geoinformation in the utm format
import requests
from string import Template
from IPython.display import display
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%d %b %Y %H:%M:%S'
)

logger = logging.getLogger()

fof = os.path.realpath(__file__)
path = os.path.split(fof)[0][:-2]

# Get column translation list
columnnames = pd.read_csv(os.path.join('input', path + 'service\\' + 'column_translation_list.csv'))
columnnames.head(2)

# Get value translation list
valuenames = pd.read_csv(os.path.join('input', path + 'service\\' + 'value_translation_list.csv'))
valuenames.head(2)


def download_and_cache(url, session=None):
    """This function downloads a file into a folder called 
    original_data and returns the local filepath."""
    path = urllib.parse.urlsplit(url).path
    filename = posixpath.basename(path)
    filepath = os.path.join('input', 'original_data', filename)

    # check if file exists, if not download it
    if not os.path.exists(filepath):
        if not session:
            print('No session')
            session = requests.session()

        r = session.get(url, stream=True)

        chuncksize = 1024
        with open(filepath, 'wb') as file:
            for chunck in r.iter_content(chuncksize):
                file.write(chunck)
    filepath = '' + filepath
    return filepath

def check_file_existence(url):
    hh = url.split('/')[-1]
    ff = './input/' + hh
    try:
        file = open(ff)
        file.close()
        link = url.split('/')[-1]
    except:
        r = requests.get(url).status_code
        if r != 200: print('Invalid file path: ' + '"' + url + '"')
        return
        link = url
    return link

def main():
    # Here you need to specify the path with the location of the files on the Internet

    DK_ens = 'https://ens.dk/sites/ens.dk/files/Vindenergi/anlaegprodtilnettet.xls'
    DK_energinet = 'https://www.energinet.dk/-/media/Energinet/El-CSI/Dokumenter/Data/SolcellerGraf-2016-11.xlsx'
    DK_geo = 'http://download.geonames.org/export/zip/DK.zip'

    url_DK_ens =  check_file_existence(DK_ens)
    url_DK_energinet = check_file_existence(DK_energinet)
    url_DK_geo = check_file_existence(DK_geo)
    
    # Get wind turbines data
    DK_wind_df = pd.read_excel('./input/' + url_DK_ens,
                               sheet_name='IkkeAfmeldte-Existing turbines',
                               thousands='.',
                               header=17,
                               skipfooter=3,
                               usecols=16,
                               converters={'Møllenummer (GSRN)': str,
                                           'Kommune-nr': str,
                                           'Postnr': str}
                               )

    # Get photovoltaic data
    DK_solar_df = pd.read_excel('./input/' + url_DK_energinet,                            
                                sheet_name='Data',
                                converters={'Postnr': str}
                                )

    # Choose the translation terms for Denmark, create dictionary and show dictionary
    idx_DK = columnnames[columnnames['country'] == 'DK'].index
    column_dict_DK = columnnames.loc[idx_DK].set_index('original_name')['opsd_name'].to_dict()

    # Windows has problems reading the csv entry for east and north (DK).
    # The reason might be the difference when opening the csv between linux and
    # windows.
    column_dict_DK_temp = {}
    for k, v in column_dict_DK.items():
        column_dict_DK_temp[k] = v
        if v == 'utm_east' or v == 'utm_north':
            # merge 2 lines to 1
            new_key = ''.join(k.splitlines())
            column_dict_DK_temp[new_key] = v

    column_dict_DK = column_dict_DK_temp

    column_dict_DK

    # Translate columns by list
    DK_wind_df['X (øst) koordinat UTM 32 Euref89'] = DK_wind_df['X (øst) koordinat \nUTM 32 Euref89']
    DK_wind_df['Y (nord) koordinat UTM 32 Euref89'] = DK_wind_df['Y (nord) koordinat \nUTM 32 Euref89']

    #and 13 are the keys that make problems
    DK_wind_df.drop(DK_wind_df.columns[[12, 13]], axis=1, inplace=True)

    # Replace column names based on column_dict_DK
    DK_wind_df.rename(columns=column_dict_DK, inplace=True)
    DK_solar_df.rename(columns=column_dict_DK, inplace=True)

    # Add names of the data sources to the DataFrames
    DK_wind_df['data_source'] = 'Energistyrelsen'
    DK_solar_df['data_source'] = 'Energinet.dk'

    # Add energy source level 2 and technology for each of the two DataFrames
    DK_wind_df['energy_source_level_2'] = 'Wind'
    DK_solar_df['energy_source_level_2'] = 'Solar'
    DK_solar_df['technology'] = 'Photovoltaics'

    # Choose the translation terms for Denmark, create dictionary and show dictionary
    idx_DK = valuenames[valuenames['country'] == 'DK'].index
    value_dict_DK = valuenames.loc[idx_DK].set_index('original_name')['opsd_name'].to_dict()
    value_dict_DK

    # Replace all original value names by the OPSD value names
    DK_wind_df.replace(value_dict_DK, inplace=True)

    # Index for all values with utm information
    idx_notnull = DK_wind_df['utm_east'].notnull()

    # Convert from UTM values to latitude and longitude coordinates
    DK_wind_df['lonlat'] = DK_wind_df.loc[idx_notnull, ['utm_east', 'utm_north']
                                          ].apply(lambda x: utm.to_latlon(x[0],
                                                                          x[1],
                                                                          32,
                                                                          'U'), axis=1).astype(str)

    # Split latitude and longitude in two columns
    lat = []
    lon = []

    for row in DK_wind_df['lonlat']:
        try:
            # Split tuple format
            # into the column lat and lon
            row = row.lstrip('(').rstrip(')')
            lat.append(row.split(',')[0])
            lon.append(row.split(',')[1])
        except:
            # set NAN
            lat.append(np.NaN)
            lon.append(np.NaN)

    DK_wind_df['lat'] = pd.to_numeric(lat)
    DK_wind_df['lon'] = pd.to_numeric(lon)

    # drop lonlat column that contains both, latitute and longitude
    DK_wind_df.drop('lonlat', axis=1, inplace=True)


    # Get geo-information
    zip_DK_geo = zipfile.ZipFile('./input/' + url_DK_geo)

    # Read generated postcode/location file
    DK_geo = pd.read_csv(zip_DK_geo.open('DK.txt'), sep='\t', header=-1)

    # add column names as defined in associated readme file
    DK_geo.columns = ['country_code', 'postcode', 'place_name', 'admin_name1',
                      'admin_code1', 'admin_name2', 'admin_code2', 'admin_name3',
                      'admin_code3', 'lat', 'lon', 'accuracy']

    # Drop rows of possible duplicate postal_code
    DK_geo.drop_duplicates('postcode', keep='last', inplace=True)
    DK_geo['postcode'] = DK_geo['postcode'].astype(str)

    # Add longitude/latitude infomation assigned by postcode (for Energinet.dk data)
    DK_solar_df = DK_solar_df.merge(DK_geo[['postcode', 'lon', 'lat']],
                                    on=['postcode'],
                                    how='left')

    # Show number of units with missing coordinates seperated by wind and solar
    print('Missing Coordinates DK_wind ', DK_wind_df.lat.isnull().sum())
    print('Missing Coordinates DK_solar ', DK_solar_df.lat.isnull().sum())

    # Merge DataFrames for wind and solar into DK_renewables
    dataframes = [DK_wind_df, DK_solar_df]
    DK_renewables = pd.concat(dataframes)
    DK_renewables = DK_renewables.reset_index()

    # Assign energy source level 1 to the dataframe
    DK_renewables['energy_source_level_1'] = 'Renewable energy'

    # Select those columns of the orignal data which are utilised further
    column_interest = ['commissioning_date', 'energy_source_level_1', 'energy_source_level_2',
                       'technology', 'electrical_capacity_kW', 'dso', 'gsrn_id', 'postcode',
                       'municipality_code', 'municipality', 'address', 'address_number',
                       'utm_east', 'utm_north', 'lon', 'lat', 'hub_height',
                       'rotor_diameter', 'manufacturer', 'model', 'data_source']

    # Clean DataFrame from columns other than specified above
    DK_renewables = DK_renewables.loc[:, column_interest]
    DK_renewables.reset_index(drop=True, inplace=True)

    # kW to MW
    DK_renewables['electrical_capacity_kW'] /= 1000

    # adapt column name
    DK_renewables.rename(columns={'electrical_capacity_kW': 'electrical_capacity'},
                         inplace=True)

    DK_renewables.to_pickle('output/DK_renewables.pickle')
    DK_renewables.to_csv('output/DK_renewables.csv',sep=';', index=False, encoding='utf-8-sig', mode='w', header=True)


if __name__ == '__main__':
    main()
