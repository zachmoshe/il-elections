"""Utilities for the preprocessing pipeline."""
from concurrent import futures
import dataclasses
import functools as ft
import itertools as it
import pathlib
from typing import Iterator, TypeVar, Sequence, Tuple, Mapping

from absl import logging
import numpy as np
import pandas as pd
import shapely
import yaml


from il_elections.data import data
from il_elections.data import geodata_fetcher
from il_elections.data import parsers
from il_elections.utils import data_utils


BallotsVotesParserType = TypeVar('BallotsVotesParserType', bound=parsers.BallotsVotesParser)
BallotsMetadataParserType = TypeVar('BallotsMetadataParserType',
                                    bound=parsers.BallotsMetadataParser)

@dataclasses.dataclass(frozen=True)
class CampaignDataLocation:
    ballots_votes_path: pathlib.Path
    ballots_votes_format: str
    ballots_metadata_path: pathlib.Path
    ballots_metadata_format: str


@dataclasses.dataclass(frozen=True)
class CampaignConfig:
    """Holds all the relevant config needed to process a single campaign."""
    metadata: data.CampaignMetadata
    data: CampaignDataLocation


@dataclasses.dataclass(frozen=True)
class PreprocessingConfig:
    """A run config for the preprocessing pipeline.

    Consists of a sequence of campaigns that need to be parsed.
    """
    campaigns: Sequence[CampaignConfig]

    @classmethod
    def from_yaml(cls, yaml_path: pathlib.Path):
        """Loads a PreprocessingConfig object from yaml file."""
        with open(yaml_path, encoding='utf8') as f:
            obj = yaml.safe_load(f)

        campaign_configs = []
        for obj in obj['preprocessing_config']['campaigns']:
            campaign_configs.append(
                CampaignConfig(
                    metadata=data.CampaignMetadata(
                        name=obj['campaign_name'],
                        date=obj['campaign_date'],
                    ),
                    data=CampaignDataLocation(
                        ballots_metadata_path=obj['data']['ballots_metadata']['filename'],
                        ballots_metadata_format=obj['data']['ballots_metadata']['format'],
                        ballots_votes_path=obj['data']['ballots_votes']['filename'],
                        ballots_votes_format=obj['data']['ballots_votes']['format'],
                    )))
        return PreprocessingConfig(campaigns=campaign_configs)


@dataclasses.dataclass(frozen=True)
class RawCampaignData:
    """Holds the processing results from a single campaign."""
    metadata: data.BallotsMetadata
    votes: data.BallotsVotes


def load_raw_campaign_data(config: CampaignConfig) -> RawCampaignData:
    """Loads and parses the metadata and votes files for a campaign."""
    metadata_parser = parsers.get_ballots_metadata_parser(config.data.ballots_metadata_format)
    metadata = metadata_parser.parse(pathlib.Path(config.data.ballots_metadata_path))

    votes_parser = parsers.get_ballots_votes_parser(config.data.ballots_votes_format)
    votes = votes_parser.parse(pathlib.Path(config.data.ballots_votes_path))

    return RawCampaignData(metadata=metadata, votes=votes)


_ISRAEL = 'ישראל'
_VILLAGE = 'יישוב'


def _normalize_optional_addresses(
    locality_name: str, location_name: str, address: str) -> Sequence[str]:
    """Returns ordered potential variation of the address to enrich."""
    # Notice that since we process Dataframes, it would have been more efficient to
    # work on the whole column at once. This method handles addresses separately
    # to allow for more flexibility in tweaking the strings.
    locality_name = data_utils.clean_hebrew_address(locality_name)
    location_name = data_utils.clean_hebrew_address(location_name)
    address = data_utils.clean_hebrew_address(address)

    if address == locality_name:
        # This happens in very small villages where the location is meaningless, like 'grocery
        # store'. In that case, since some localities names are generic (like רחוב) we're more
        # likely to get a false match if we try all combinations.
        return [
            _VILLAGE + ' ' + locality_name,
            locality_name,
        ]

    return [
        address + ', ' + locality_name + ', ' + _ISRAEL,
        location_name + ', ' + locality_name + ', ' + _ISRAEL,
        location_name + ' ' + address + ', ' + locality_name  + ', ' + _ISRAEL,
        locality_name,  # In case nothing else matched, we use the city center.
    ]


@ft.lru_cache(maxsize=1)
def _load_israel_polygon_lnglat():
    israel = data_utils.load_israel_polygon()
    israel = shapely.ops.transform(data.utm_to_lnglat.transform, israel)
    return israel


def _within_israel_bounds(geodata: geodata_fetcher.GeoDataResults):
    """Checks if lng/lat boundaries are within Israel (approx.)"""
    point = shapely.geometry.Point(geodata.longitude, geodata.latitude)
    return point.within(_load_israel_polygon_lnglat())


_NON_GEOGRAPHICAL_LOCALITY_IDS = set([
    '0',  # Duplicate votes (knesset-18 only).
    '875',  # External votes (knesset-19/20 only).
    '99999',  # External votes (knesset-21 only).
    '9999',  # External votes (knesset-22/23/24 only).
])

# Approximation of 1 KM in lat/lng degree units. Very rough. Only somewhat accurate around Israel.
_KM_IN_DEGREES = 0.01
MAX_ALLOWED_DISTANCE_FROM_LOCALITY_CENTER_KM = 10

def _enrich_addresses_strategy(addresses, locality_center, locality_polygon, fetcher):
    """Enriches a sequence of possible ordered sequences of addresses for a ballot.

    Tries one by one until a result comes back from the GeoCode fetcher which is valid and inside
    the locality polygon. If locality polygon is not given, looks for a close distance from the
    locality center.
    """
    for address in addresses:
        geodata = fetcher.fetch_geocode_data(address)
        if geodata is None:
            continue

        geodata_point = shapely.geometry.Point(geodata.longitude, geodata.latitude)
        if locality_polygon is not None:
            is_valid = locality_polygon.contains(geodata_point)
        else:
            # Calculate distance from locality_center
            geodata_distance_from_center = geodata_point.distance(locality_center)
            is_valid = (
                geodata_distance_from_center <
                MAX_ALLOWED_DISTANCE_FROM_LOCALITY_CENTER_KM * _KM_IN_DEGREES)

        if is_valid:
            return pd.Series({'lat': geodata.latitude, 'lng': geodata.longitude})

    return pd.Series({'lat': None, 'lng': None})


def _enrich_locality_strategy(locality_name, fetcher):
    addresses_to_search = [_VILLAGE + ' ' + locality_name, locality_name]
    for add in addresses_to_search:
        r = fetcher.fetch_geocode_data(add)
        if r is not None and _within_israel_bounds(r):
            return r
    raise ValueError(f'could\'t find locality geocoding for "{locality_name}"')


def _enrich_per_locality(locality_name, ldf, fetcher):
    """Enriches all ballots from the same locality together."""
    r = _enrich_locality_strategy(locality_name, fetcher)
    locality_center = shapely.geometry.Point(r.longitude, r.latitude)

    # See if we can find a ISR_ADM2 polygon for this locality.
    isr_adm2 = data_utils.read_gis_file(data.GisFile.ISR_ADM2, proj=data.PROJ_LNGLAT)  # memoized.
    matched_polygons = isr_adm2[isr_adm2.contains(locality_center)]
    if matched_polygons.empty:
        locality_polygon = None
    elif len(matched_polygons) > 1:
        logging.warning(
            f'Found {len(matched_polygons)} polygons in ISR_ADM2 that match locality '
            f'"{locality_name}" ({locality_center}). Not using locality polygon.')
        locality_polygon = None
    else:
        locality_polygon = matched_polygons.iloc[0].geometry  # Single row.

    res = ldf.apply(ft.partial(_enrich_addresses_strategy,
                               locality_center=locality_center,
                               locality_polygon=locality_polygon,
                               fetcher=fetcher))
    return res


def enrich_metadata_with_geolocation(metadata_df: pd.DataFrame) -> pd.DataFrame:
    """Enriches the metadata dataframe with the `lat` and `lng` columns of the ballot's address."""
    metadata_df = metadata_df.copy()
    # Add the potential normalized addresses
    normalized_addresses_options = (
        metadata_df[['locality_name', 'location_name', 'address']]
        .apply(
            lambda x: _normalize_optional_addresses(*x),
            axis='columns')
        )
    fetcher = geodata_fetcher.GeoDataFetcher(duplicate_known_addresses_with_prefixes=(_VILLAGE,))

    grouped = normalized_addresses_options.groupby(metadata_df['locality_name'])
    with futures.ThreadPoolExecutor() as exc:
        all_enriched = exc.map(
            lambda x: _enrich_per_locality(*x, fetcher=fetcher),
            list(grouped))

    enriched = pd.concat(list(all_enriched))
    return metadata_df.join(enriched)


def _get_ballot_index(ballots_dataframe, drop_subballot=False):
    ballot_id = ballots_dataframe['ballot_id']
    if drop_subballot:
        ballot_id = ballot_id.str.replace(r'\.[1-9]+', '.0', regex=True)
    return ballots_dataframe['locality_id'] + '-' + ballot_id


def preprocess(config: PreprocessingConfig
               ) -> Iterator[Tuple[data.CampaignMetadata, pd.DataFrame]]:
    """Runs a preprocessing pipeline for a given config (multiple campaigns).
    Yields tuples of (campaign_metadata, dataframe) for every campaign.
    """
    logging.info('Started preprocessing the following campaigns: '
                 f'{[c.metadata.name for c in config.campaigns]}')

    for campaign_config in config.campaigns:
        campaign_name = campaign_config.metadata.name

        logging.info(f'Loading data for campaign {campaign_name}')
        raw_campaign_data = load_raw_campaign_data(campaign_config)

        logging.info('Enriching with geolocation')
        votes_df = raw_campaign_data.votes.df
        metadata_df = raw_campaign_data.metadata.df
        # Make sure all localities from `votes_df` exist in `metadata_df`. For the missing ones, add
        # an empty row to metadata with only the locality_name and a fake ballot_id so we can at
        # least enrich with the locality_name alone.
        missing_locality_ids = (set(votes_df.locality_id.unique())
                                - set(metadata_df.locality_id.unique())
                                - _NON_GEOGRAPHICAL_LOCALITY_IDS)
        missing_localities = (
            votes_df[votes_df.locality_id.isin(missing_locality_ids)]
            .groupby('locality_id').agg(min)['locality_name'].to_frame()  # All are the same.
            .assign(ballot_id='0.0').reset_index())  # Adding a fake ballot_id.
        metadata_df = metadata_df.append(missing_localities)
        metadata_df = enrich_metadata_with_geolocation(metadata_df)

        votes_df.set_index(_get_ballot_index(votes_df, drop_subballot=False), inplace=True)
        metadata_df.set_index(_get_ballot_index(metadata_df, drop_subballot=False), inplace=True)

        # First, try matching exactly the same index (locality_id + ballot_id)
        df = (
            votes_df
            .merge(metadata_df.drop(votes_df.columns, errors='ignore', axis='columns'),
                   how='left',
                   left_index=True,
                   right_index=True))

        # Try to match unmatced rows to the same index but with ballot_id with a '.0' suffix.
        # This will help when `votes_df` contains ballots like '123.1' and '123.2' but `metadata_df`
        # only have data for `123.0` (that's normally the case when a ballot is splitted based on
        # family name first letter).
        missing_idxs = df[df['lat'].isnull() | df['lng'].isnull()].index
        df.loc[missing_idxs] = (
            votes_df.loc[missing_idxs]
            .merge(metadata_df.drop(votes_df.columns, errors='ignore', axis='columns'),
                   how='left',
                   left_on=_get_ballot_index(votes_df.loc[missing_idxs], drop_subballot=True),
                   right_index=True))

        # Records that are still not matched will be matched to the average lat/lng in their
        # locality (apprently it happens that some ballot_ids are just missing from the metadata
        # file), in that case we'll just assume it's somewhere in the locality.
        missing_idxs = df[df['lat'].isnull() | df['lng'].isnull()].index
        df.loc[missing_idxs] = (
            votes_df.loc[missing_idxs]
            .merge(metadata_df.groupby('locality_id').agg(np.mean)[['lat', 'lng']],
                   how='left',
                   left_on='locality_id',
                   right_index=True))

        yield campaign_config.metadata, df


@dataclasses.dataclass
class CampaignDataAnalysis:
    """Holds aggregate information on a single campaign data."""
    num_voters: int
    num_voted: int
    voting_ratio: float
    parties_votes: Mapping[str, int]
    missing_geo_location: pd.Series


def analyze_campaign_data(campaign_data: pd.DataFrame) -> CampaignDataAnalysis:
    """Generate aggregate stats on campaign data (in the final dataframe format)."""
    num_voters = campaign_data['num_registered_voters'].sum()
    num_voted = campaign_data['num_voted'].sum()
    parties_votes = dict(
        (key, sum(v[1] for v in values))
         for key, values in it.groupby(sorted(
             it.chain(*[d.items() for d in campaign_data['parties_votes']])), key=lambda x: x[0])
    )
    missing_geo = campaign_data[campaign_data['lat'].isnull() | campaign_data['lng'].isnull()]
    missing_geo_counts = missing_geo.fillna('NA').groupby(
        ['locality_name', 'location_name', 'address']).size().sort_values(ascending=False)

    return CampaignDataAnalysis(
        num_voters=num_voters,
        num_voted=num_voted,
        voting_ratio=num_voted / num_voters,
        parties_votes=parties_votes,
        missing_geo_location=missing_geo_counts)
