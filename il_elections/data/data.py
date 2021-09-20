"""Various data classes and utilities."""
import abc
import dataclasses
from typing import Mapping, Union, Any

import pandas as pd


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
