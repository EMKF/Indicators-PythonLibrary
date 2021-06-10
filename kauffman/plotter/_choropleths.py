import sys
import requests
import numpy as np
import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt
from shapely.geometry import Polygon, MultiPolygon


def _geo_format(x):
    if len(x) == 1:
        return Polygon(x[0])
    else:
        return MultiPolygon([Polygon(x[i]) for i in range(len(x))])


def _choropleth(gdf, outcome, outcome_min, outcome_max, title, annotation, state_lst=[], us_map=True, include_ak=False, include_hi=False):
    fig, ax = plt.subplots(1, figsize=(30, 10))
    ax.axis('off')
    ax.set_title(title, fontdict={'fontsize': '25', 'fontweight': '3'})
    ax.annotate(annotation, xy=(0.6, .05), xycoords='figure fraction', fontsize=12, color='#555555')

    if us_map:
        gdf.plot(ax=ax, alpha=0.3)
    if state_lst:
        gdf.query(f'region in {state_lst}', inplace=True)

    sm = plt.cm.ScalarMappable(cmap='Blues', norm=plt.Normalize(vmin=outcome_min, vmax=outcome_max))
    sm.set_array([])

    fig.colorbar(sm, orientation="horizontal", fraction=0.036, pad=0.1, aspect=30)
    gdf.plot(column=outcome, cmap='Blues', linewidth=0.8, ax=ax, edgecolor='0.8')
    plt.show()


def _state_choropleth(df, outcome, state_lst):
    url = 'https://services2.arcgis.com/DEoxb4q3EJppiDKC/arcgis/rest/services/States_shapefile/FeatureServer/0/query?where=1%3D1&outFields=*&outSR=4326&f=json'
    geo_json = requests.get(url).json()['features']

    gdf = pd.json_normalize(geo_json, meta=['attributes', ['geometry', 'rings']]).\
        assign(geometry=lambda x: x['geometry.rings'].apply(_geo_format)) \
        [['attributes.State_Name', 'geometry']].\
        pipe(gpd.GeoDataFrame, crs='EPSG:4326').\
        set_geometry('geometry').\
        merge(df, )  #todo: here
        # query('region not in ["HI", "AK"]')

    np.random.seed(42)
    gdf['outcome'] = np.random.uniform(0, 100, size=(gdf.shape[0], 1))


def choropleth(df, outcome, obs_level, state_lst):
    if obs_level == 'state':
        _state_choropleth(df, outcome, state_lst)
