"""Utils for loading data files."""
import pathlib

import geopandas as gpd
import pandas as pd
import shapely

_ISRAEL_POLYGON_PATH = pathlib.Path('data/gis/boundaries/geoBoundaries-ISR-ADM0-all.zip')
_PALESTINE_POLYGON_PATH = pathlib.Path('data/gis/boundaries/geoBoundaries-PSE-ADM0-all.zip')
_EQUAL_AREA_METRIC_PROJECTION = 'EPSG:3857'


def load_israel_polygon():
    """Returns a Shapely polygon for the state of Israel (including the west bank)."""
    boundaries = pd.concat([
        gpd.read_file(_ISRAEL_POLYGON_PATH),
        gpd.read_file(_PALESTINE_POLYGON_PATH).explode(index_parts=True),
    ])

    israel = (
        boundaries
        .groupby('shapeName')
        # Take only the largest polygon (if a MultiPolygon object).
        .apply(lambda df: (df.assign(area=df.to_crs(_EQUAL_AREA_METRIC_PROJECTION).area)
                           .sort_values('area').iloc[-1]))
        .unary_union)
    # Eliminate all tiny holes due to imperfect alignment between ISR and PSE files.
    israel = shapely.geometry.Polygon(israel.exterior)
    return israel
