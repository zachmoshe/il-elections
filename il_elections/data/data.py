"""Various data classes and utilities."""
import abc
import dataclasses
import datetime
import enum
import pathlib
from typing import Mapping, Union, Any

import geopandas as gpd
import pandas as pd
import pyproj


PROJ_UTM = 'EPSG:32636'  # UTM zone 36 (matches Israel)
PROJ_LNGLAT = 'EPSG:4326'

utm_to_lnglat = pyproj.Transformer.from_proj(
    pyproj.Proj(PROJ_UTM), pyproj.Proj(PROJ_LNGLAT), always_xy=True)
lnglat_to_utm = pyproj.Transformer.from_proj(
    pyproj.Proj(PROJ_LNGLAT), pyproj.Proj(PROJ_UTM), always_xy=True)


@dataclasses.dataclass(frozen=True)
class TypeAwareDataFrame(abc.ABC):
    """Represents pd.DataFrames with some awareness of their columns and dtypes.

    Child implementations will override the `_dataframe_dtypes` with a dtypes
    mapping. The `__post_init__()` will verify that columns and dtypes are correct.
    """
    df: pd.DataFrame

    @property
    @abc.abstractmethod
    def _dataframe_dtypes(self) -> Mapping[str, Union[str, Any]]:
        ...

    def __post_init__(self):
        missing_columns = self._dataframe_dtypes.keys() - set(self.df.columns)
        if missing_columns:
            raise ValueError(f'columns {missing_columns} are missing from dataframe.')
        unexpected_columns = set(self.df.columns) - self._dataframe_dtypes.keys()
        if unexpected_columns:
            raise ValueError(f'columns {unexpected_columns} are unexpected in the dataframe.')
        type_errors = []
        for col_name, col_dtype in self._dataframe_dtypes.items():
            if self.df.dtypes[col_name] != col_dtype:
                type_errors.append(
                    f'column {col_name} should be a "{col_dtype}". instead '
                    f'it is "{self.df.dtypes[col_name]}".')
        if type_errors:
            raise ValueError(', '.join(type_errors))


class BallotsMetadata(TypeAwareDataFrame):
    """Represents metadata information about a ballot (location, address, ...)."""
    _dataframe_dtypes = {
        'ballot_id': 'string',
        'locality_id': 'string',
        'locality_name': 'string',
        'location_name': 'string',
        'address': 'string'
    }

    def __post_init__(self):
        super().__post_init__()
        # Set index to locality_id+ballot_id
        self.df.set_index(self.df['locality_id'] + '-' + self.df['ballot_id'],
                          inplace=True)


class BallotsVotes(TypeAwareDataFrame):
    """Represents information about voting counts in a specific ballot."""
    _dataframe_dtypes = {
        'ballot_id': 'string',
        'locality_id': 'string',
        'locality_name': 'string',
        'num_registered_voters': 'int',
        'num_voted': 'int',
        'num_disqualified': 'int',
        'num_approved': 'int',
        'parties_votes': 'object'  # dict(party_id -> int)
    }

    def __post_init__(self):
        super().__post_init__()
        # Set index to locality_id+ballot_id
        self.df.set_index(self.df['locality_id'] + '-' + self.df['ballot_id'],
                          inplace=True)


@dataclasses.dataclass(frozen=True)
class CampaignMetadata:
    name: str
    date: datetime.date


@dataclasses.dataclass
class PreprocessedCampaignData:
    raw_votes: gpd.GeoDataFrame
    per_location: gpd.GeoDataFrame
    metadata: CampaignMetadata


class GisFile(enum.Enum):
    """Contains known GIS files locations."""
    ISR_ADM0 = pathlib.Path('data/gis/boundaries/geoBoundaries-ISR-ADM0-all.zip')
    ISR_ADM1 = pathlib.Path('data/gis/boundaries/geoBoundaries-ISR-ADM1-all.zip')
    ISR_ADM2 = pathlib.Path('data/gis/boundaries/geoBoundaries-ISR-ADM2-all.zip')
    PSE_ADM0 = pathlib.Path('data/gis/boundaries/geoBoundaries-PSE-ADM0-all.zip')
    PSE_ADM1 = pathlib.Path('data/gis/boundaries/geoBoundaries-PSE-ADM1-all.zip')
    ISR_WATERBODIES = pathlib.Path('data/gis/boundaries/Israel_water_bodies.kml')
