"""Utilities for plotting, mostly Colab and visualization stuff."""
import enum
import pathlib

import branca
import folium
import geopandas as gpd
import pandas as pd
import pyproj
import shapely

# Required in order to allow geopandas to load KML files (no need to install a new driver)
gpd.io.file.fiona.drvsupport.supported_drivers['KML'] = 'rw'


PROJ_UTM = 'EPSG:32636'  # UTM zone 36 (matches Israel)
PROJ_LNGLAT = 'EPSG:4326'

utm_to_lnglat = pyproj.Transformer.from_proj(
    pyproj.Proj(PROJ_UTM), pyproj.Proj(PROJ_LNGLAT), always_xy=True)
lnglat_to_utm = pyproj.Transformer.from_proj(
    pyproj.Proj(PROJ_LNGLAT), pyproj.Proj(PROJ_UTM), always_xy=True)


_ISR_ADM0_GEOJSON_PATH = pathlib.Path('data/gis/boundaries/geoBoundaries-ISR-ADM0-all.zip')
_ISR_ADM1_GEOJSON_PATH = pathlib.Path('data/gis/boundaries/geoBoundaries-ISR-ADM1-all.zip')
_PSE_ADM0_GEOJSON_PATH = pathlib.Path('data/gis/boundaries/geoBoundaries-PSE-ADM0-all.zip')
_PSE_ADM1_GEOJSON_PATH = pathlib.Path('data/gis/boundaries/geoBoundaries-PSE-ADM1-all.zip')
_ISR_WATER_BODIES_PATH = pathlib.Path('data/gis/boundaries/Israel_water_bodies.kml')
_ISR_ADM1 = (
    gpd.read_file(_ISR_ADM1_GEOJSON_PATH)
    .set_index('shapeName')
    .to_crs(PROJ_UTM))

def generate_circle_utm(center, radius_meters):
    return shapely.geometry.Point(center).buffer(radius_meters)


def generate_rectangle_utm(min_x, min_y, max_x, max_y):
    return shapely.geometry.Polygon(
        [(min_x, min_y), (min_x, max_y), (max_x, max_y), (max_x, min_y)])


def load_israel_polygon():
    """Returns a Shapely polygon (UTM) for the state of Israel (including the west bank)."""
    # Take all Israel and the West Bank only from PSE.
    israel = pd.concat([
        gpd.read_file(_ISR_ADM1_GEOJSON_PATH),
        gpd.read_file(_PSE_ADM1_GEOJSON_PATH),
    ]).query('shapeGroup=="ISR" or shapeISO=="PS-WBK"').unary_union

    all_water_bodies_polygon = gpd.read_file(_ISR_WATER_BODIES_PATH).unary_union

    # Eliminate all tiny holes due to imperfect alignment between ISR and PSE files.
    israel = shapely.geometry.Polygon(israel.exterior)
    israel -= all_water_bodies_polygon
    israel_utm = shapely.ops.transform(lnglat_to_utm.transform, israel)
    return israel_utm


class Maps(enum.Enum):
    """Holds known maps for easier reference."""
    # Areas
    ISRAEL = load_israel_polygon()
    NORTH = _ISR_ADM1.loc['North District'].geometry
    CENTER = _ISR_ADM1.loc[['Tel Aviv District', 'Center District']].dissolve().iloc[0].geometry
    SOUTH = _ISR_ADM1.loc['South District'].geometry

    # Cities
    BEERSHEVA = generate_circle_utm(center=(671041.49, 3458353.12), radius_meters=5000)
    HAIFA = generate_circle_utm(center=(687075.41, 3631881.30), radius_meters=5000)
    JERUSALEM = generate_circle_utm(center=(709616.69, 3517636.21), radius_meters=5000)
    TLV = generate_circle_utm(center=(668017.45, 3550814.14), radius_meters=2500)

    @property
    def center(self):
        return self.value.centroid

    @property
    def bounds(self):
        return self.value.bounds


def map(map_def, width='100%', height=400):  # pylint: disable=redefined-builtin
    """Generates a map object that plays nicely with Colab."""
    fig = branca.element.Figure(width=width, height=height)
    m = folium.Map(tiles='OpenStreetMap')

    lnglat_polygon = shapely.ops.transform(utm_to_lnglat.transform, map_def.value)
    minx, miny, maxx, maxy = lnglat_polygon.bounds
    # fit_bounds() requires (lat, lng) of southwest, northeast.
    m.fit_bounds(((miny, minx), (maxy, maxx)))

    fig.add_child(m)
    return m
