"""Different file types parsers implementations."""
import pathlib
import re
from typing import Optional, Protocol

import pandas as pd

from il_elections.data import data


class BallotsMetadataParser(Protocol):
    """Protocol for ballots metadata parsers."""
    def parse(self, path: pathlib.Path) -> data.BallotsMetadata:
        ...

class BallotsVotesParser(Protocol):
    """Protocol for ballots votes parsers."""
    def parse(self, path: pathlib.Path) -> data.BallotsVotes:
        ...


class BallotsMetadataExcelParser:
    """Parses ballots metadata from an Excel file format."""
    _COLUMNS_MAPPING = {
        'סמל קלפי': 'ballot_id',
        'סמל ישוב בחירות': 'locality_id',
        'שם ישוב בחירות': 'locality_name',
        'מקום קלפי': 'location_name',
        'כתובת קלפי': 'address',
    }

    def parse(self, path: pathlib.Path) -> data.BallotsMetadata:
        """Parses an Excel file with ballots metadata."""
        # Pandas handles both file object and paths.
        dataframe = pd.read_excel(path)

        if set(self._COLUMNS_MAPPING.keys()) - set(dataframe.columns):
            raise ValueError('Could not find all required columns in the given Excel file.')

        dataframe = (
            dataframe
            .loc[:, self._COLUMNS_MAPPING.keys()]
            .rename(self._COLUMNS_MAPPING, axis='columns')
        )
        # Excel stores all numbers as floats. Convert to int and then string to remove
        # the additional '.0' suffix.
        dataframe['locality_id'] = dataframe['locality_id'].astype(int).astype('string')
        # ballot_id is a "real" float.
        dataframe['ballot_id'] = dataframe['ballot_id'].astype(float).astype('string')

        dataframe = dataframe.astype('string')
        for c in dataframe.columns:
            dataframe[c] = dataframe[c].str.strip()

        return data.BallotsMetadata(df=dataframe)


class BallotsMetadataPDFParser:
    """Parses ballots metadata from PDF file format (used in the knesset-19 elections)."""
    _EXCEL_CONVERTED_SUFFIX = '.xlsx'

    _COLUMNS_MAPPING = {
        'למס\nיפלק': 'ballot_id',
        'בושי למס\nתוריחב': 'locality_id',
        'תוריחב בושי םש': 'locality_name',
        'יפלק םוקמ': 'location_name',
        'יפלק תבותכ': 'address',
    }

    @staticmethod
    def _clean_text(text):
        if pd.isna(text):
            return None

        # Remove all kind of \n's
        text, _  = re.subn(r'\s', ' ', text)
        # In case of ' " ' - delete the surrounding spaces
        text, _ = re.subn(' " ', '"', text)
        # Hebrew text is reversed
        text = text[::-1]
        return text.strip()

    def parse(self, path: pathlib.Path) -> data.BallotsMetadata:
        """Parses PDF file with ballots metadata.
        (Notice that due to RTL parsing problems, this actualy relies on manual conversion of the
        file to Excel by the user. Hopefully this can be removed in the future.)
        """
        converted_file_path = path.parent / (path.name + self._EXCEL_CONVERTED_SUFFIX)
        if not converted_file_path.exists():
            raise ValueError('''
PDFParser expects to find an Excel format file for the required PDF.
Apparently Python PDF parsing packages don't handle well right-to-left text with parentheses in tables and mess up the whole structure.
Although called PDFParser, this parser will actually parse an XLSX format generated from that file. In order to convert PDF to Excel, use
'Export As' in Acrobat Reader.
            ''')

        dataframe = pd.read_excel(str(converted_file_path))
        # Remove the repeating table header in every page.
        dataframe = dataframe[dataframe['הדעו למס']!='הדעו למס']

        dataframe = (
            dataframe
            .loc[:, self._COLUMNS_MAPPING.keys()]
            .rename(self._COLUMNS_MAPPING, axis='columns')
        )

        dataframe['location_name'] = (
            dataframe['location_name'].apply(self._clean_text))
        dataframe['locality_name'] = (
            dataframe['locality_name'].apply(self._clean_text))
        dataframe['address'] = dataframe['address'].astype('string').apply(self._clean_text)
        # ballot_id might have non digits characters because of parsing problems.
        dataframe['ballot_id'] = (
            dataframe['ballot_id'].astype('string').str.replace('[^0-9.]', '', regex=True)
            .astype(float).astype('string'))

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
assert len(_HEBREW_TO_ENGLISH_TRASCRIBE.values()) == len(_HEBREW_TO_ENGLISH_TRASCRIBE),\
    'heb->eng transcribe must be value-unique.'

def _heb_to_eng(text):
    return ''.join(_HEBREW_TO_ENGLISH_TRASCRIBE[c] for c in text)

class BallotsVotesFileParser:
    """Parses ballots votes from Excel/CSV formats."""
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

    def __init__(self, format_name: str, encoding: Optional[str] = None):
        if format_name not in ('excel', 'csv'):
            raise ValueError(f'Unsupported format "{format_name}".')
        self.format_name = format_name
        self.encoding = encoding

    def parse(self, path: pathlib.Path) -> data.BallotsVotes:
        """Parses Excel or CSV files with ballots votes data."""
        if self.format_name == 'csv':
            orig_dataframe = pd.read_csv(path, encoding=self.encoding)
        else:
            orig_dataframe = pd.read_excel(path)

        orig_dataframe = orig_dataframe.loc[:, ~orig_dataframe.columns.str.startswith('Unnamed: ')]  # pylint: disable=no-member
        orig_dataframe.drop(self._IGNORED_COLUMNS, axis='columns', errors='ignore', inplace=True)

        columns_mapping = {k: v for k, v in self._COLUMNS_MAPPING.items()
                           if k in orig_dataframe.columns}
        dataframe = (
            orig_dataframe
            .loc[:, columns_mapping.keys()]
            .rename(columns_mapping, axis='columns'))
        dataframe['ballot_id'] = dataframe['ballot_id'].astype(float).astype('string')
        dataframe['locality_id'] = dataframe['locality_id'].astype('string')
        dataframe['locality_name'] = dataframe['locality_name'].astype('string')

        parties_columns = (set(orig_dataframe.columns) - set(columns_mapping.keys())
                           - set(self._IGNORED_COLUMNS))
        parties_votes = (
            orig_dataframe.loc[:, parties_columns]
            .rename(_heb_to_eng, axis='columns')
            .apply(dict, axis='columns'))
        dataframe[self._PARTIES_VOTES_COLUMN_NAME] = parties_votes

        return data.BallotsVotes(df=dataframe)


_BALLOTS_VOTES_PARSER_REGISTRY = {
    'csv-windows': BallotsVotesFileParser(format_name='csv', encoding='cp1255'),
    'csv-utf8': BallotsVotesFileParser(format_name='csv', encoding='utf-8'),
    'excel': BallotsVotesFileParser(format_name='excel'),
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
