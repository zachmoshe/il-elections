"""Utilities to ease working with the ballots geo data."""
import dataclasses
import datetime
import itertools as it
import pathlib
import re
from typing import Iterator, Sequence, Mapping, Optional, Union
import yaml

import geopandas as gpd
import numpy as np
import pandas as pd
import shapely.geometry

from il_elections.utils import plot_utils


@dataclasses.dataclass(frozen=True)
class CampaignMetadata:
    name: str
    date: datetime.date


@dataclasses.dataclass
class PreprocessedCampaignData:
    raw_votes: gpd.GeoDataFrame
    per_location: gpd.GeoDataFrame
    metadata: CampaignMetadata


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
                   crs=plot_utils.PROJ_UTM) -> gpd.GeoSeries:
    grid = gpd.GeoSeries(grid_polygons, crs=crs)
    grid = grid[grid.intersects(bounded_polygon)]
    return grid


def generate_grid_by_size(
    bounded_polygon: Union[shapely.geometry.Polygon, plot_utils.Maps],
    grid_size: int,
    crs: str = plot_utils.PROJ_UTM):
    """Generates a grid that covers the polygon with (size x size) cells."""
    if isinstance(bounded_polygon, plot_utils.Maps):
        bounded_polygon = bounded_polygon.value
    grid_polygons = list(_generate_covering_polygons_grid_cells_by_grid_size(
        bounded_polygon, grid_size))
    return _generate_grid(bounded_polygon, grid_polygons, crs)


def generate_grid_by_length(
    bounded_polygon: Union[shapely.geometry.Polygon, plot_utils.Maps],
    grid_length: float,
    crs: str = plot_utils.PROJ_UTM):
    """Generates a grid that covers the polygon where every cell is at size (length x length)."""
    if isinstance(bounded_polygon, plot_utils.Maps):
        bounded_polygon = bounded_polygon.value
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
    data_folder: pathlib.Path, campaign_name: str) -> PreprocessedCampaignData:
    """Loading preprocessed data. Converting and aggregating based on the geo-data."""
    data_path = data_folder / f'{campaign_name}.data'
    metadata_path = data_folder / f'{campaign_name}.metadata'

    with open(metadata_path, 'rt', encoding='utf8') as f:
        metadata = CampaignMetadata(**yaml.safe_load(f))
    data = pd.read_parquet(data_path)
    # Dropping ballots without geo (should be only "external votes").
    data = data.dropna(subset=['lat', 'lng'])

    raw_votes_gdf = gpd.GeoDataFrame(
        data,
        geometry=gpd.points_from_xy(data['lng'], data['lat']),
        crs=plot_utils.PROJ_LNGLAT).to_crs(plot_utils.PROJ_UTM)

    _nanunique = lambda x: list(np.unique(x.dropna()))
    per_location_df = data.assign(num_ballots=1).groupby(['lng', 'lat']).agg({
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
        crs=plot_utils.PROJ_LNGLAT).to_crs(plot_utils.PROJ_UTM)

    return PreprocessedCampaignData(
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
