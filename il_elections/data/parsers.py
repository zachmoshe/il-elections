import abc
import camelot
import pathlib
import re
import tempfile
from typing import Sequence, TextIO, Union, Optional

import pandas as pd
from pandas.core.dtypes import dtypes

from . import data


class BallotsMetadataParser(abc.ABC):
    def parse(self, path: pathlib.Path) -> data.BallotsMetadata:
        ...

class BallotsVotesParser(abc.ABC):
    def parse(self, path: pathlib.Path) -> data.BallotsVotes:
        ...


class BallotsMetadataExcelParser(BallotsMetadataParser):
    """Parses ballots metadata from an Excel file format."""
    _BALLOT_ID_COLUMN = 'סמל קלפי'
    _LOCALITY_ID_COLUMN = 'סמל ישוב בחירות'
    _LOCALITY_NAME_COLUMN = 'שם ישוב בחירות'
    _LOCATION_NAME_COLUMN = 'מקום קלפי'
    _ADDRESS_COLUMN = 'כתובת קלפי'
    _COLUMNS_MAPPING = {
        'סמל קלפי': 'ballot_id',
        'סמל ישוב בחירות': 'locality_id',
        'שם ישוב בחירות': 'locality_name',
        'מקום קלפי': 'location_name',
        'כתובת קלפי': 'address',        
    }

    def parse(self, path: pathlib.Path) -> data.BallotsMetadata:
        # Pandas handles both file object and paths.
        dataframe = pd.read_excel(path)

        if any(col not in dataframe.columns for col in 
               (self._BALLOT_ID_COLUMN, self._LOCALITY_ID_COLUMN, self._LOCALITY_NAME_COLUMN,
               self._LOCATION_NAME_COLUMN, self._ADDRESS_COLUMN)):
            raise ValueError('Could not find all required columns in the given Excel file.')
        
        dataframe = (
            dataframe
            .loc[:, self._COLUMNS_MAPPING.keys()]
            .rename(self._COLUMNS_MAPPING, axis='columns')
        )
        # Excel stores all numbers as floats. Convert to int and then string to remove 
        # the additional '.0' suffix.
        dataframe['locality_id'] = dataframe['locality_id'].astype(int).astype('string')

        dataframe = dataframe.astype('string')
        for c in dataframe.columns:
            dataframe[c] = dataframe[c].str.strip()

        return data.BallotsMetadata(df=dataframe)


class BallotsMetadataPDFParser(BallotsMetadataParser):
    """Parses ballots metadata from PDF file format (used in the knesset-19 elections)."""
    _CSV_CACHED_SUFFIX = '.cached-csv'    
    _CSV_ENCODING = 'utf-8'

    _COLUMNS_MAPPING = {
        'למס\nיפלק': 'ballot_id',
        'בושי למס\nתוריחב': 'locality_id',
        'תוריחב בושי םש': 'locality_name',
        'יפלק םוקמ': 'location_name',
        'יפלק תבותכ': 'address',        
    }

    def _load_or_create_dataframe(self, path: pathlib.Path) -> pd.DataFrame:
        cached_file = path.with_suffix(self._CSV_CACHED_SUFFIX)
        if cached_file.exists():
            return pd.read_csv(cached_file, encoding=self._CSV_ENCODING)

        all_pages_results = camelot.read_pdf(str(path), pages='all')
        dataframes = []
        for res in all_pages_results:
            res.df.columns = res.df.iloc[0]
            res.df = res.df.iloc[1:]
            dataframes.append(res.df.astype('string'))
        dataframe = pd.concat(dataframes)
        dataframe.to_csv(cached_file, encoding=self._CSV_ENCODING)
        return dataframe

    @staticmethod
    def _clean_text(text):
        if pd.isna(text):
            return None

        # Remove all kind of \n's
        text, _  = re.subn('\s', ' ', text)
        # In case of ' " ' - delete the surrounding spaces
        text, _ = re.subn(' " ', '"', text)
        # Hebrew text is reversed
        text = text[::-1]
        return text.strip()

    def parse(self, path: pathlib.Path) -> data.BallotsMetadata:
        # Check if a cached CSV version of the PDF exists.
        # If it doesn't - parse the PDF (very long, ~20mins) and store it.
        dataframe = self._load_or_create_dataframe(path)

        dataframe = (
            dataframe
            .loc[:, self._COLUMNS_MAPPING.keys()]
            .rename(self._COLUMNS_MAPPING, axis='columns')
        )

        dataframe['location_name'] = (
            dataframe['location_name'].apply(self._clean_text))
        dataframe['locality_name'] = (
            dataframe['locality_name'].apply(self._clean_text))
        dataframe['address'] = dataframe['address'].apply(self._clean_text)

        return data.BallotsMetadata(df=dataframe.astype('string'))

_HEBREW_TO_ENGLISH_TRASCRIBE = {
     'א': 'a',
     'ב': 'b',
     'ג': 'g',
     'ד': 'd',
     'ה': 'h',
     'ו': 'v',
     'ז': 'z',
     'ח': 'H',
     'ט': 't',
     'י': 'i',
     'כ': 'k',
     'ך': 'k.',
     'ל': 'l',
     'מ': 'm',
     'ם': 'm.',
     'נ': 'n',
     'ן': 'n.',
     'ס': 's',
     'ע': 'A',
     'פ': 'p',
     'ף': 'p.',
     'צ': 'Z',
     'ץ': 'Z.',
     'ק': 'K',
     'ר': 'r',
     'ש': 'S',
     'ת': 'T',
}
assert len(_HEBREW_TO_ENGLISH_TRASCRIBE.values()) == len(_HEBREW_TO_ENGLISH_TRASCRIBE), 'heb->eng transcribe must be value-unique.'

def _heb_to_eng(text):
    return ''.join(_HEBREW_TO_ENGLISH_TRASCRIBE[c] for c in text)

class BallotsVotesFileParser(BallotsVotesParser):
    _PARTIES_VOTES_COLUMN_NAME = 'parties_votes'
    _COLUMNS_MAPPING = {
        'מספר קלפי': 'ballot_id',
        'סמל קלפי': 'ballot_id',
        'קלפי': 'ballot_id',  # Three ways of writing the 'ballot_id' column.
        'סמל ישוב': 'locality_id',
        'שם ישוב': 'locality_name',
        'שם ישוב ': 'locality_name',
        ' שם ישוב': 'locality_name',
        'בזב': 'num_registered_voters',
        'בז\'\'ב': 'num_registered_voters',  # Two ways of writing num_voters.
        'מצביעים': 'num_voted',
        'פסולים': 'num_disqualified',
        'כשרים': 'num_approved',
    }
    _IGNORED_COLUMNS = ('סמל ועדה', 'ברזל', 'ריכוז', 'שופט', 'ת. עדכון')

    def __init__(self, format: str, encoding: Optional[str] = None):
        if format not in ('excel', 'csv'):
            raise ValueError(f'Unsupported format "{format}".')
        self.format = format
        self.encoding = encoding

    def parse(self, path: pathlib.Path) -> data.BallotsVotes:
        if self.format == 'csv': 
            orig_dataframe = pd.read_csv(path, encoding=self.encoding)
        else:
            orig_dataframe = pd.read_excel(path)

        orig_dataframe = orig_dataframe.loc[:, ~orig_dataframe.columns.str.startswith('Unnamed: ')]
        orig_dataframe.drop(self._IGNORED_COLUMNS, axis='columns', errors='ignore', inplace=True)

        columns_mapping = {k: v for k, v in self._COLUMNS_MAPPING.items()
                           if k in orig_dataframe.columns}
        dataframe = (
            orig_dataframe
            .loc[:, columns_mapping.keys()]
            .rename(columns_mapping, axis='columns'))
        dataframe['ballot_id'] = dataframe['ballot_id'].astype('string')
        dataframe['locality_id'] = dataframe['locality_id'].astype('string')
        dataframe['locality_name'] = dataframe['locality_name'].astype('string')

        parties_columns = set(orig_dataframe.columns) - set(columns_mapping.keys()) - set(self._IGNORED_COLUMNS)
        parties_votes = (
            orig_dataframe.loc[:, parties_columns]
            .rename(_heb_to_eng, axis='columns')
            .apply(dict, axis='columns'))
        dataframe[self._PARTIES_VOTES_COLUMN_NAME] = parties_votes

        return data.BallotsVotes(df=dataframe)
    

_BALLOTS_VOTES_PARSER_REGISTRY = {
    'csv-windows': BallotsVotesFileParser(format='csv', encoding='cp1255'),
    'csv-utf8': BallotsVotesFileParser(format='csv', encoding='utf-8'),
    'excel': BallotsVotesFileParser(format='excel'),
}
def get_ballots_votes_parser(format_name: str) -> BallotsVotesParser:
    if format_name not in _BALLOTS_VOTES_PARSER_REGISTRY:
        raise ValueError(f'Unknown format `{format_name}` for BallotsVotesParser.')
    return _BALLOTS_VOTES_PARSER_REGISTRY[format_name]

_BALLOTS_METADATA_PARSER_REGISTRY = {
    'excel': BallotsMetadataExcelParser(),
    'pdf': BallotsMetadataPDFParser(),
}
def get_ballots_metadata_parser(format_name: str) -> BallotsMetadataParser:
    if format_name not in _BALLOTS_METADATA_PARSER_REGISTRY:
        raise ValueError(f'Unknown format `{format_name}` for BallotsMetadataParser.')
    return _BALLOTS_METADATA_PARSER_REGISTRY[format_name]
