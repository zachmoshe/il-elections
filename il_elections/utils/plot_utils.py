"""Utilities for plotting, mostly Colab and visualization stuff."""
import enum
import pathlib
from typing import Optional

import branca
import folium
import geopandas as gpd
import importlib_resources
import jinja2
import numpy as np
import pandas as pd
import pyproj
import shapely

import il_elections.utils

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


def ilmap(map_def, width='100%', height=400):  # pylint: disable=redefined-builtin
    """Generates a map object that plays nicely with Colab."""
    fig = branca.element.Figure(width=width, height=height)
    m = folium.Map(tiles='OpenStreetMap')

    lnglat_polygon = shapely.ops.transform(utm_to_lnglat.transform, map_def.value)
    minx, miny, maxx, maxy = lnglat_polygon.bounds
    # fit_bounds() requires (lat, lng) of southwest, northeast.
    m.fit_bounds(((miny, minx), (maxy, maxx)))

    fig.add_child(m)
    return m

JINJA_ENV = jinja2.Environment(
    loader=jinja2.FileSystemLoader(
        searchpath=importlib_resources.files(il_elections.utils) / 'html_templates'))

def generate_tooltip_html_for_per_location_row(
    per_location_row: pd.Series,
    limit_num_parties: Optional[int] = None,
    top_pct_coverage: Optional[float] = None,
    remove_zero_votes: bool = True):
    """Generates HTML popup for a per-location ballot's data."""
    total_votes = sum(per_location_row['parties_votes'].values())
    normed_votes = {k: v / total_votes for k, v in per_location_row['parties_votes'].items()}
    sorted_normed_votes = sorted(normed_votes.items(), key=lambda x: x[1], reverse=True)

    if remove_zero_votes:
        sorted_normed_votes = [x for x in sorted_normed_votes if x[1]]

    limit_idx = min(
        limit_num_parties or len(sorted_normed_votes),
        np.where(np.cumsum([x[1] for x in sorted_normed_votes]) > top_pct_coverage)[
            0][0] + 1 if top_pct_coverage else len(sorted_normed_votes),
    )
    sorted_normed_votes = sorted_normed_votes[:limit_idx]
    parties_data_to_display = [(k, per_location_row['parties_votes'][k], v)
                                for k, v in sorted_normed_votes]

    locality_str = '/'.join(per_location_row['locality_name'])
    location_str = '/'.join(per_location_row['location_name'])
    address_str = '/'.join(per_location_row['address'])
    location = ', '.join([x for x in [locality_str, location_str, address_str] if x])

    ballot_ids_str = ', '.join(per_location_row['ballot_id'])

    num_registered_voters = per_location_row['num_registered_voters']
    num_voted = per_location_row['num_voted']
    num_approved = per_location_row['num_approved']
    num_disqualified = per_location_row['num_disqualified']

    tmpl = JINJA_ENV.get_template('per_location_tooltip.html.jinja2')

    return tmpl.render(
        parties_data=parties_data_to_display,
        location=location,
        ballot_ids=ballot_ids_str,
        num_registered_voters=num_registered_voters,
        num_voted=num_voted,
        num_approved=num_approved,
        num_disqualified=num_disqualified)


def create_ballots_feature_group(
    ballots_per_location_data: gpd.GeoDataFrame,
    feature_group_name: str = 'Ballots Info',
    voting_top_pct_coverage: float = 0.9):
    """Creates a folium.FeatureGroup layer with all ballots info including voting tooltips."""

    grp = folium.FeatureGroup(feature_group_name)
    for _, row in ballots_per_location_data.to_crs(PROJ_LNGLAT).iterrows():
        html_str = generate_tooltip_html_for_per_location_row(
            row, top_pct_coverage=voting_top_pct_coverage)
        popup = folium.Popup(html_str)
        grp.add_child(
            folium.Circle(location=(row['geometry'].y, row['geometry'].x), radius=1, popup=popup))
    return grp