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


def _state_lst(state_lst, include_ak, include_hi):
    if state_lst:
        return [int(c.state_abb_to_fips[s]) for s in state_lst]
    else:
        state_lst_fips = [int(c.state_abb_to_fips[s]) for s in c.states]
        if not include_ak:
            state_lst_fips.remove(2)
        if not include_hi:
            state_lst_fips.remove(15)
        return state_lst_fips


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
            pipe(_us_map_filter, gdf['fips'].unique()).\
            plot(ax=ax, alpha=0.1, color='gray')

    gdf.plot(column=outcome, cmap='Blues', linewidth=0.8, ax=ax, edgecolor='0.8')
    plt.show()


def _state_geo():
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


def _state_choropleth(df, outcome, outcome_interval, title, annotation, us_map):
    _state_geo().\
        merge(df, on='fips', how='right').\
        pipe(_plotter, outcome, outcome_interval, title, annotation, us_map)


def _county_choropleth(df, outcome, outcome_interval, title, annotation, us_map):
    # zip_url = 'https://www2.census.gov/geo/tiger/TIGER2020/COUNTY/tl_2020_us_county.zip'
    # filename = 'tl_2020_us_county.shp'
    # 'https://www.sciencebase.gov/catalog/file/get/4f4e4a2ee4b07f02db615738?f=18%2Fae%2Fb6%2F18aeb60870a3d10d164715ba1da6a5b34497d527'

    # z = ZipFile(io.BytesIO(requests.get(zip_url).content))
    # z = ZipFile(io.StringIO(requests.get(zip_url).text))
    # gdf = gpd.read_file(z.open(filename))

    # todo what do we do with this file? It is big and takes a long time to IO
    # maybe download, extract, read in, and cleanup
    gpd.read_file('/Users/thowe/Downloads/tl_2020_us_county/tl_2020_us_county.shp').\
        assign(
            fips=lambda x: (x['STATEFP'] + x['COUNTYFP']).astype(int)
        ) \
        [['fips', 'geometry']].\
        merge(df, on='fips', how='right'). \
        pipe(_plotter, outcome, outcome_interval, title, annotation, us_map)


def choropleth(df, outcome, obs_level, outcome_interval, title, annotation, state_lst=[], us_map=True, include_ak=False, include_hi=False):
    # todo: the data should already be subsetted.
    # todo: can I infer obs_level?
    if obs_level == 'county':
        _county_choropleth(df, outcome, outcome_interval, title, annotation, us_map)
    elif obs_level == 'state':
        df.query(f'fips in {_state_lst(state_lst, include_ak, include_hi)}', inplace=True)
        _state_choropleth(df, outcome, outcome_interval, title, annotation, us_map)
