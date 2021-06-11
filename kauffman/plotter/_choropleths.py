import sys
import requests
import numpy as np
import pandas as pd
import geopandas as gpd
import kauffman.constants as c
import matplotlib.pyplot as plt
from shapely.geometry import Polygon, MultiPolygon


def _state_lst(state_lst, include_ak, include_hi):
    if state_lst:
        return [int(c.state_abb_fips_dic[s]) for s in state_lst]
    else:
        state_lst_fips = [int(c.state_abb_fips_dic[s]) for s in c.states]
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


def _state_plotter(gdf, outcome, outcome_interval, title, annotation, us_map):
    fig, ax = plt.subplots(1, figsize=(30, 10))
    ax.axis('off')
    ax.set_title(title, fontdict={'fontsize': '25', 'fontweight': '3'})
    ax.annotate(annotation, xy=(0.6, .05), xycoords='figure fraction', fontsize=12, color='#555555')

    if us_map:
        gdf.plot(ax=ax, alpha=0.3)

    sm = plt.cm.ScalarMappable(cmap='Blues', norm=plt.Normalize(vmin=outcome_interval[0], vmax=outcome_interval[1]))
    sm.set_array([])

    fig.colorbar(sm, orientation="horizontal", fraction=0.036, pad=0.1, aspect=30)
    gdf.plot(column=outcome, cmap='Blues', linewidth=0.8, ax=ax, edgecolor='0.8')
    plt.show()


def _state_choropleth(df, outcome, outcome_interval, title, annotation, us_map):
    shapefile_url = 'https://services2.arcgis.com/DEoxb4q3EJppiDKC/arcgis/rest/services/States_shapefile/FeatureServer/0/query?where=1%3D1&outFields=*&outSR=4326&f=json'
    geo_json = requests.get(shapefile_url).json()['features']

    pd.json_normalize(geo_json, meta=['attributes', ['geometry', 'rings']]).\
        assign(
            geometry=lambda x: x['geometry.rings'].apply(_geo_format),
            fips=lambda x: x['attributes.State_Code'].map(c.state_abb_fips_dic).astype(int)
        ) \
        [['fips', 'geometry']].\
        pipe(gpd.GeoDataFrame, crs='EPSG:4326').\
        set_geometry('geometry').\
        merge(df, on='fips', how='right').\
        pipe(_state_plotter, outcome, outcome_interval, title, annotation, us_map)


def choropleth(df, outcome, obs_level, outcome_interval, title, annotation, state_lst=[], us_map=True, include_ak=False, include_hi=False):

    if obs_level == 'state':
        df.query(f'fips in {_state_lst(state_lst, include_ak, include_hi)}', inplace=True)
        _state_choropleth(df, outcome, outcome_interval, title, annotation, us_map)
