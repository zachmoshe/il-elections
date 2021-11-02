"""Utilities to ease working with the ballots geo data."""
import functools as ft
import itertools as it
import pathlib
import re
from typing import Iterator, Sequence, Mapping, Optional
import yaml

import geopandas as gpd
import numpy as np
import pandas as pd
import shapely.geometry

from il_elections.data import data


@ft.lru_cache(maxsize=None)
def _read_file(file_path: pathlib.Path):
    return gpd.read_file(file_path)

def read_gis_file(gis_file: data.GisFile, proj: str = data.PROJ_UTM) -> gpd.GeoDataFrame:
    """Reads a GIS file."""
    gdf = _read_file(gis_file.value)
    gdf = gdf.to_crs(proj)
    if 'shapeName' in gdf.columns:
        gdf = gdf.set_index('shapeName')
    return gdf


@ft.lru_cache()
def load_israel_polygon():
    """Returns a Shapely polygon (UTM) for the state of Israel (including the west bank)."""
    # Take all Israel and the West Bank only from PSE.
    israel = pd.concat([
        read_gis_file(data.GisFile.ISR_ADM1),
        read_gis_file(data.GisFile.PSE_ADM1),
    ]).query('shapeGroup=="ISR" or shapeISO=="PS-WBK"').unary_union

    all_water_bodies_polygon = (
        read_gis_file(data.GisFile.ISR_WATERBODIES).unary_union)

    # Eliminate all tiny holes due to imperfect alignment between ISR and PSE files.
    israel = shapely.geometry.Polygon(israel.exterior)
    israel -= all_water_bodies_polygon
    return israel


def clean_hebrew_address(address_string: Optional[str]):
    if pd.isna(address_string):
        return ''
    return re.sub(r'[^\w\d]+', ' ', address_string).strip()


def _generate_covering_polygons_grid_cells_by_grid_size(
    polygon: shapely.geometry.Polygon,
    grid_size: int) -> Iterator[shapely.geometry.Polygon]:
    """Generates (grid_size x grid_size) grid cells polygons that cover the given polygon.

    Generated polygons split the area binding the polygon evenly.
    """
    min_lng, min_lat, max_lng, max_lat = polygon.bounds
    lats = np.linspace(min_lat, max_lat, grid_size + 1)
    lngs = np.linspace(min_lng, max_lng, grid_size + 1)
    yield from (
        shapely.geometry.box(lng_start, lat_start, lng_end, lat_end)
        for lat_start, lat_end in zip(lats[:-1], lats[1:])
        for lng_start, lng_end in zip(lngs[:-1], lngs[1:])
    )

def _generate_covering_polygons_grid_cells_by_grid_length(
    polygon: shapely.geometry.Polygon,
    grid_length: float) -> Iterator[shapely.geometry.Polygon]:
    """Generates square grid cells polygons that cover the given polygon with a given grid length .

    Generated polygons cover the area binding the polygon and all have the same requested size.
    Notice that grid_length will have a meaning corresponding to the projection of the polygon, i.e.
    if polygon is UTM, then grid_length is meters, if lng-lat then degrees.
    """
    min_lng, min_lat, max_lng, max_lat = polygon.bounds
    lats = np.arange(min_lat, max_lat + grid_length, grid_length)
    lngs = np.arange(min_lng, max_lng + grid_length, grid_length)
    yield from (
        shapely.geometry.box(lng_start, lat_start, lng_end, lat_end)
        for lat_start, lat_end in zip(lats[:-1], lats[1:])
        for lng_start, lng_end in zip(lngs[:-1], lngs[1:])
    )


def _generate_grid(bounded_polygon: shapely.geometry.Polygon,
                   grid_polygons: Sequence[shapely.geometry.Polygon],
                   crs=data.PROJ_UTM) -> gpd.GeoSeries:
    grid = gpd.GeoSeries(grid_polygons, crs=crs)
    grid = grid[grid.intersects(bounded_polygon)]
    return grid


def generate_grid_by_size(
    bounded_polygon: shapely.geometry.Polygon,
    grid_size: int,
    crs: str = data.PROJ_UTM):
    """Generates a grid that covers the polygon with (size x size) cells."""
    grid_polygons = list(_generate_covering_polygons_grid_cells_by_grid_size(
        bounded_polygon, grid_size))
    return _generate_grid(bounded_polygon, grid_polygons, crs)


def generate_grid_by_length(
    bounded_polygon: shapely.geometry.Polygon,
    grid_length: float,
    crs: str = data.PROJ_UTM):
    """Generates a grid that covers the polygon where every cell is at size (length x length)."""
    grid_polygons = list(_generate_covering_polygons_grid_cells_by_grid_length(
        bounded_polygon, grid_length))
    return _generate_grid(bounded_polygon, grid_polygons, crs)


def group_points_by_polygons(points, polygons):
    """Groups together all points that fall inside the same polygon.

    Returns a DataFrameGroupBy object which the user can continue querying. For example:
    ```
    points = ...
    polygons = generate_grid_by_size(...)
    # Gives the average number of voters in all ballots inside each polygon on the grid.
    group_points_by_polygons(points, polygons)['num_voters'].mean()
    ```
    """
    polygons = (polygons
                .to_frame('geometry')
                .reset_index()
                .rename({'index': 'polygon_id'}, axis='columns'))
    grouped = polygons.sjoin(points, how='inner', predicate='contains').groupby('polygon_id')
    return grouped


VotingCounts = Mapping[str, int]
def aggregate_parties_votes(parties_votes: Sequence[VotingCounts]) -> VotingCounts:
    """Aggregates the counts of every parts from a sequence of counts."""
    sorted_votes_items = sorted(it.chain(i for d in parties_votes for i in d.items()),
                                key=lambda x: x[0])
    return dict((party, sum(x[1] for x in items))
                for party, items in it.groupby(sorted_votes_items, key=lambda x: x[0]))


def load_preprocessed_campaign_data(
    data_folder: pathlib.Path, campaign_name: str) -> data.PreprocessedCampaignData:
    """Loading preprocessed data. Converting and aggregating based on the geo-data."""
    data_path = data_folder / f'{campaign_name}.data'
    metadata_path = data_folder / f'{campaign_name}.metadata'

    with open(metadata_path, 'rt', encoding='utf8') as f:
        metadata = data.CampaignMetadata(**yaml.safe_load(f))
    df = pd.read_parquet(data_path)
    # Dropping ballots without geo (should be only "external votes").
    df = df.dropna(subset=['lat', 'lng'])

    raw_votes_gdf = gpd.GeoDataFrame(
        df,
        geometry=gpd.points_from_xy(df['lng'], df['lat']),
        crs=data.PROJ_LNGLAT).to_crs(data.PROJ_UTM)

    _nanunique = lambda x: list(np.unique(x.dropna()))
    per_location_df = df.assign(num_ballots=1).groupby(['lng', 'lat']).agg({
        'num_ballots': np.sum,
        'ballot_id': _nanunique,
        'locality_id': 'first',
        'locality_name': 'first',
        'location_name': _nanunique,
        'address': _nanunique,
        'num_registered_voters': np.sum,
        'num_voted': np.sum,
        'num_disqualified': np.sum,
        'num_approved': np.sum,
        'parties_votes': aggregate_parties_votes,
    }).reset_index()
    per_location_gdf = gpd.GeoDataFrame(
        per_location_df,
        geometry=gpd.points_from_xy(per_location_df['lng'], per_location_df['lat']),
        crs=data.PROJ_LNGLAT).to_crs(data.PROJ_UTM)

    return data.PreprocessedCampaignData(
        raw_votes=raw_votes_gdf, per_location=per_location_gdf, metadata=metadata)


def norm_parties_votes_to_pct(votes: VotingCounts) -> Mapping[str, float]:
    """Normalized each party votes to pct of total votes."""
    total_votes = sum(votes.values())
    normed_votes = {k: (v / total_votes if total_votes else 0.) for k, v in votes.items()}
    return normed_votes

def project(parties_data: Mapping[str, float], weights: Mapping[str, float]) -> float:
    """Projects votes counts by a linear set of weights."""
    projected = sum(parties_data[k] * weights.get(k, 0.) for k in parties_data.keys())
    return projected
