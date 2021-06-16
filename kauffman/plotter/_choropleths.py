import io
import sys
import requests
import numpy as np
import pandas as pd
import geopandas as gpd
from zipfile import ZipFile
import kauffman.constants as c
import matplotlib.pyplot as plt
from shapely.geometry import Polygon, MultiPolygon


def _geo_format(x):
    if len(x) == 1:
        return Polygon(x[0])
    else:
        return MultiPolygon([Polygon(x[i]) for i in range(len(x))])


def _us_map_filter(df, fips_lst):
    states_lst = list(set(['0' + str(fips)[0] if len(str(fips)) == 4 else str(fips)[:2] for fips in fips_lst]))
    if '02' not in states_lst:
        df.query('fips != 2', inplace=True)
    if '15' not in states_lst:
        df.query('fips != 15', inplace=True)
    return df


def _plotter(gdf, outcome, outcome_interval, title, annotation, us_map):
    fig, ax = plt.subplots(1, figsize=(30, 10))
    ax.axis('off')
    ax.set_title(title, fontdict={'fontsize': '25', 'fontweight': '3'})
    ax.annotate(annotation, xy=(0.6, .05), xycoords='figure fraction', fontsize=12, color='#555555')

    sm = plt.cm.ScalarMappable(cmap='Blues', norm=plt.Normalize(vmin=outcome_interval[0], vmax=outcome_interval[1]))
    sm.set_array([])
    fig.colorbar(sm, orientation="horizontal", fraction=0.036, pad=0.1, aspect=30)

    if us_map:
        _state_geo().\
            loc[lambda x: ~x['fips'].isin([15, 2]) if us_map == 48 else x['fips'] == x['fips'] if us_map == 50 else x['fips'] != x['fips']].\
            plot(ax=ax, alpha=0.1, color='gray')

    gdf.plot(column=outcome, cmap='Blues', linewidth=0.8, ax=ax, edgecolor='0.8')
    plt.show()


def _state_geo():
    # is this one or the census file better?
    shapefile_url = 'https://services2.arcgis.com/DEoxb4q3EJppiDKC/arcgis/rest/services/States_shapefile/FeatureServer/0/query?where=1%3D1&outFields=*&outSR=4326&f=json'
    geo_json = requests.get(shapefile_url).json()['features']

    return pd.json_normalize(geo_json, meta=['attributes', ['geometry', 'rings']]).\
        assign(
            geometry=lambda x: x['geometry.rings'].apply(_geo_format),
            fips=lambda x: x['attributes.State_Code'].map(c.state_abb_to_fips).astype(int)
        ) \
        [['fips', 'geometry']].\
        pipe(gpd.GeoDataFrame, crs='EPSG:4326').\
        set_geometry('geometry')


def choropleth(df, outcome, obs_level, outcome_interval, title, annotation, us_map=48):
    """
    us_map: int,
        50: all states
        48: lower 48 states
        0: no states
    """

    if obs_level == 'county':
        gdf = gpd.read_file('https://www2.census.gov/geo/tiger/TIGER2020/COUNTY/tl_2020_us_county.zip'). \
            assign(fips=lambda x: (x['STATEFP'] + x['COUNTYFP'])) \
            [['fips', 'geometry']]
    elif obs_level == 'msa':
        gdf = gpd.read_file('https://www2.census.gov/geo/tiger/TIGER2020/CBSA/tl_2020_us_cbsa.zip'). \
            rename(columns={'CBSAFP': 'fips'}) \
            [['fips', 'geometry']]
    elif obs_level == 'state':
        gdf = _state_geo()

    gdf.\
        merge(df, on='fips', how='right'). \
        pipe(_plotter, outcome, outcome_interval, title, annotation, us_map)
