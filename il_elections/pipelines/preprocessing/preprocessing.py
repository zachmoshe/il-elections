"""Utilities for the preprocessing pipeline."""
from concurrent import futures
import dataclasses
import datetime
import itertools as it
import pathlib
import re
from typing import Iterator, TypeVar, Sequence, Tuple, Mapping

from absl import logging
import numpy as np
import pandas as pd
import yaml


from il_elections.data import data
from il_elections.data import geodata_fetcher
from il_elections.data import parsers


BallotsVotesParserType = TypeVar('BallotsVotesParserType', bound=parsers.BallotsVotesParser)
BallotsMetadataParserType = TypeVar('BallotsMetadataParserType',
                                    bound=parsers.BallotsMetadataParser)

@dataclasses.dataclass(frozen=True)
class CampignDataLocation:
    ballots_votes_path: pathlib.Path
    ballots_votes_format: str
    ballots_metadata_path: pathlib.Path
    ballots_metadata_format: str

@dataclasses.dataclass(frozen=True)
class CampignMetadata:
    name: str
    date: datetime.date

@dataclasses.dataclass(frozen=True)
class CampignConfig:
    """Holds all the relevant config needed to process a single campign."""
    metadata: CampignMetadata
    data: CampignDataLocation

@dataclasses.dataclass(frozen=True)
class PreprocessingConfig:
    """A run config for the preprocessing pipeline.

    Consists of a sequence of campigns that need to be parsed.
    """
    campigns: Sequence[CampignConfig]

    @classmethod
    def from_yaml(cls, yaml_path: pathlib.Path):
        """Loads a PreprocessingConfig object from yaml file."""
        with open(yaml_path, encoding='utf8') as f:
            obj = yaml.safe_load(f)

        campign_configs = []
        for obj in obj['preprocessing_config']['campigns']:
            campign_configs.append(
                CampignConfig(
                    metadata=CampignMetadata(
                        name=obj['campign_name'],
                        date=obj['campign_date'],
                    ),
                    data=CampignDataLocation(
                        ballots_metadata_path=obj['data']['ballots_metadata']['filename'],
                        ballots_metadata_format=obj['data']['ballots_metadata']['format'],
                        ballots_votes_path=obj['data']['ballots_votes']['filename'],
                        ballots_votes_format=obj['data']['ballots_votes']['format'],
                    )))
        return PreprocessingConfig(campigns=campign_configs)


@dataclasses.dataclass(frozen=True)
class CampignData:
    """Holds the processing results from a single campign."""
    metadata: data.BallotsMetadata
    votes: data.BallotsVotes


def load_campign_data(config: CampignConfig) -> CampignData:
    """Loads and parses the metadata and votes files for a campign."""
    metadata_parser = parsers.get_ballots_metadata_parser(config.data.ballots_metadata_format)
    metadata = metadata_parser.parse(pathlib.Path(config.data.ballots_metadata_path))

    votes_parser = parsers.get_ballots_votes_parser(config.data.ballots_votes_format)
    votes = votes_parser.parse(pathlib.Path(config.data.ballots_votes_path))

    return CampignData(metadata=metadata, votes=votes)


_NON_ALPHANUMERIC_SEQUENCE = re.compile('[^a-z0-9\u0590-\u05ff]+', flags=re.IGNORECASE)
_ISRAEL = 'ישראל'

def _strip_string(s):
    if pd.isna(s):
        return ''
    s = _NON_ALPHANUMERIC_SEQUENCE.sub(' ', s)
    return s.strip()

def _normalize_optional_addresses(
    locality_name: str, location_name: str, address: str) -> Sequence[str]:
    """Returns ordered potential variation of the address to enrich."""
    # Notice that since we process Dataframes, it would have been more efficient to
    # work on the whole column at once. This method handles addresses separately
    # to allow for more flexibility in tweaking the strings.
    locality_name = _strip_string(locality_name)
    location_name = _strip_string(location_name)
    address = _strip_string(address)

    return [
        address + ', ' + locality_name + ', ' + _ISRAEL,
        location_name + ', ' + locality_name + ', ' + _ISRAEL,
        location_name + ' ' + address + ', ' + locality_name  + ', ' + _ISRAEL,
        locality_name + ', ' + _ISRAEL,  # In case nothing else matched, we use the city center.
        _ISRAEL,  # As a really last-resort...
    ]


_NUM_PANDAS_APPLY_THREADS = 8
def _pandas_apply_multithreaded(df, func):
    """Applies the given `func` on `df` in parallel.
    (could be a DataFrame or a Series if `func` can handle it).
    """
    chunks = np.array_split(df, _NUM_PANDAS_APPLY_THREADS)
    with futures.ThreadPoolExecutor(_NUM_PANDAS_APPLY_THREADS) as exc:
        results = pd.concat(exc.map(
            lambda df: df.apply(func),
            chunks))
    return results

def _is_lng_lat_legal(geodata: geodata_fetcher.GeoDataResults):
    """Checks if lng/lat boundaries are within Israel (approx.)"""
    return (34. < geodata.longitude < 36.) and (29. < geodata.latitude < 34.)  # pylint: disable=chained-comparison


_NON_GEOGRAPHICAL_LOCALITY_IDS = set([
    '0',  # Duplicate votes (knesset-18 only).
    '875',  # External votes (knesset-19/20 only).
    '99999',  # External votes (knesset-21 only).
    '9999',  # External votes (knesset-22/23/24 only).
])


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
    fetcher = geodata_fetcher.GeoDataFetcher()
    def _enrich_strategy(addresses):
        for address in addresses:
            geodata = fetcher.fetch_geocode_data(address)
            if geodata is not None and _is_lng_lat_legal(geodata):
                return geodata
        return None
    geodata = _pandas_apply_multithreaded(
        normalized_addresses_options, _enrich_strategy)
    metadata_df['lat'] = geodata.apply(lambda g: g.latitude)
    metadata_df['lng'] = geodata.apply(lambda g: g.longitude)
    return metadata_df


def _get_ballot_index(ballots_dataframe, drop_subballot=False):
    ballot_id = ballots_dataframe['ballot_id']
    if drop_subballot:
        ballot_id = ballot_id.str.replace(r'\.[1-9]+', '.0', regex=True)
    return ballots_dataframe['locality_id'] + '-' + ballot_id


def preprocess(config: PreprocessingConfig
               ) -> Iterator[Tuple[CampignMetadata, pd.DataFrame]]:
    """Runs a preprocessing pipeline for a given config (multiple campigns).
    Yields tuples of (campign_metadata, dataframe) for every campign.
    """
    logging.info('Started preprocessing the following campigns: '
                 f'{[c.metadata.name for c in config.campigns]}')

    for campign_config in config.campigns:
        campign_name = campign_config.metadata.name

        logging.info(f'Loading data for campign {campign_name}')
        campign_data = load_campign_data(campign_config)

        logging.info('Enriching with geolocation')
        votes_df = campign_data.votes.df
        metadata_df = campign_data.metadata.df
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

        yield campign_config.metadata, df


@dataclasses.dataclass
class CampignDataAnalysis:
    """Holds aggregate information on a single campign data."""
    num_voters: int
    num_voted: int
    voting_ratio: float
    parties_votes: Mapping[str, int]
    missing_geo_location: pd.Series


def analyze_campign_data(campign_data: pd.DataFrame) -> CampignDataAnalysis:
    """Generate aggregate stats on campign data (in the final dataframe format)."""
    num_voters = campign_data['num_registered_voters'].sum()
    num_voted = campign_data['num_voted'].sum()
    parties_votes = dict(
        (key, sum(v[1] for v in values))
         for key, values in it.groupby(sorted(
             it.chain(*[d.items() for d in campign_data['parties_votes']])), key=lambda x: x[0])
    )
    missing_geo = campign_data[campign_data['lat'].isnull() | campign_data['lng'].isnull()]
    missing_geo_counts = missing_geo.fillna('NA').groupby(
        ['locality_name', 'location_name', 'address']).size().sort_values(ascending=False)

    return CampignDataAnalysis(
        num_voters=num_voters,
        num_voted=num_voted,
        voting_ratio=num_voted / num_voters,
        parties_votes=parties_votes,
        missing_geo_location=missing_geo_counts)
