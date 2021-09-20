"""Utilities for the preprocessing pipeline."""
from concurrent import futures
import dataclasses
import datetime
import pathlib
import re
from typing import Iterator, TypeVar, Sequence, Tuple

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
    logging.info(f'loading data for campign {config.metadata.name}...')
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
        metadata_df = campign_data.metadata.df
        metadata_df = enrich_metadata_with_geolocation(metadata_df)
        votes_df = campign_data.votes.df

        df = (
            votes_df
            .join(metadata_df
                  .drop(['ballot_id', 'locality_id', 'locality_name'], axis='columns')
                  ))
        yield campign_config.metadata, df
