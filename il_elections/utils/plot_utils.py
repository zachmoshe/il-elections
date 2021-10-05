"""Utilities for plotting, mostly Colab and visualization stuff."""
import dataclasses
import enum
from typing import Sequence

import branca
import folium
import geopandas as gpd
import shapely

from il_elections.utils import data_utils


@dataclasses.dataclass
class PointWithRadiusMapBounds:
    """Represents map bounds from a center point and a radius."""
    center: Sequence[float]  # (lng, lat)
    radius_meters: float

    @property
    def bounds(self):
        """Returns the bounds (min_lng, min_lat, max_lng, max_lat) of the map."""
        bounds = (gpd.GeoSeries(shapely.geometry.Point(*self.center),
                                crs=data_utils.DEGREE_PROJECTION)
                  .to_crs(data_utils.EQUAL_AREA_METRIC_PROJECTION)
                  .buffer(self.radius_meters)
                  .to_crs(data_utils.DEGREE_PROJECTION)
                  ).total_bounds
        return tuple(bounds)


@dataclasses.dataclass
class RectangleMapBounds:
    """Represents arbitrary map bounds."""
    bounds: Sequence[float]  # (min_lng, min_lat, max_lng, max_lat)

    @property
    def center(self):
        return tuple(self.bounds.reshape((2, 2)).mean(axis=0))


class Maps(enum.Enum):
    """Holds known maps for easier reference."""
    # Cities
    TLV = PointWithRadiusMapBounds(center=[32.07319, 34.79328], radius_meters=2500)
    HAIFA = PointWithRadiusMapBounds(center=[32.805037, 35.025720], radius_meters=5000)
    JERUSALEM = PointWithRadiusMapBounds(center=[31.770260, 35.208957], radius_meters=5000)
    BEERSHEVA = PointWithRadiusMapBounds(center=[31.252142, 34.792088], radius_meters=3500)

    # Areas
    NORTH = RectangleMapBounds(bounds=(34.8101, 32.4171, 35.9143, 33.303))
    CENTER = RectangleMapBounds(bounds=(34.552, 31.9102, 35.3045, 32.2685))
    SOUTH = RectangleMapBounds(bounds=(34.2, 29.5, 35.6, 31.35))
    ISRAEL = RectangleMapBounds(bounds=(34.2674, 29.4906, 35.8950, 33.3356))


def map(map_def, width='100%', height=400):  # pylint: disable=redefined-builtin
    """Generates a map object that plays nicely with Colab."""
    map_def = map_def.value
    fig = branca.element.Figure(width=width, height=height)
    m = folium.Map(tiles='OpenStreetMap')
    m.fit_bounds((map_def.bounds[:2][::-1], map_def.bounds[2:][::-1]))
    fig.add_child(m)
    return m
