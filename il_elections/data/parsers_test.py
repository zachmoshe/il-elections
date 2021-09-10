import pathlib
import unittest

from parameterized import parameterized

from il_elections.data import parsers

_BALLOT_METADATA_COLUMNS = ('ballot_id', 'locality_id', 'locality_name', 'location_name', 'address')
# NOTICE: Order of hebrew fields is reversed. For example, in the first row ירושלים is the 3rd column.
_EXCEL_BALLOTS_FILE_VALUES = (
    ('3.1', '3000',
     'ירושלים',
     'סמינר בית יעקב - עטרת חן',
     'מגן האלף,1'),
    ('157.3', '7400',
     'נתניה',
     'בי"ס אורות רש"י',
     'אריה לייב יפה,10'),
    ('57.0', '8700',
     'רעננה',
     'בי"ס זיו',
     'קלאוזנר,27'),
)
_EXCEL_BALLOTS_FILE_RECORDS = [
    dict(zip(_BALLOT_METADATA_COLUMNS, values)) for values in _EXCEL_BALLOTS_FILE_VALUES]


class BallotsMetadataExcelParserTest(unittest.TestCase):
    _TEST_FILE_PATH = pathlib.Path('data/tests/ballots_metadata_test_file.xlsx')

    def test_parses_file_correctly(self):
        results = parsers.BallotsMetadataExcelParser().parse(self._TEST_FILE_PATH)
        self.assertEqual(len(results.df), 3)
        self.assertEqual(results.df.to_dict('records'), _EXCEL_BALLOTS_FILE_RECORDS)
        

# Just the first 3 rows
_PDF_BALLOTS_FILE_VALUES = (
    ('1.0', '3000', 
    'ירושלים', 
    'ביה"ס בית יעקב הצפון  ( סנהדריה )',
    'מעגלי הרי"ם לוין 52,'),
    ('2.0', '3000',
    'ירושלים', 
    '"בי ס בית יעקב עזרת תורה',
    'אהלי יוסף 51,'),
    ('3.0', '3000', 
    'ירושלים', 
    'מרכז קהילתי פאני קפלן',
    'מגן האלף  1,')
)
_PDF_BALLOTS_FILE_RECORDS = [
    dict(zip(_BALLOT_METADATA_COLUMNS, values)) for values in _PDF_BALLOTS_FILE_VALUES]

class BallotsMetadataPDFParserTest(unittest.TestCase):
    _TEST_FILE_PATH = pathlib.Path('data/tests/ballots_metadata_test_file.pdf')
    
    def _delete_csv_cache(self):
        cache_file = self._TEST_FILE_PATH.with_suffix(
            parsers.BallotsMetadataPDFParser._CSV_CACHED_SUFFIX)
        cache_file.unlink(missing_ok=True)

    def setUp(self) -> None:
        super().setUp()
        self._delete_csv_cache()
    
    def tearDown(self) -> None:
        super().tearDown()
        self._delete_csv_cache()

    def test_parses_file_correctly(self):
        results = parsers.BallotsMetadataPDFParser().parse(self._TEST_FILE_PATH)
        self.assertEqual(len(results.df), 30)  # 2 pages of 15 ballots each.
        self.assertEqual(results.df.iloc[:3].to_dict('records'),
                         _PDF_BALLOTS_FILE_RECORDS)


_CSV_VOTES_FILE_RECORDS_FILE1 = [
    {'ballot_id': '159.0', 'locality_id': '7100', 'locality_name': 'אשקלון', 'num_registered_voters': 612,
     'num_voted': 455, 'num_disqualified': 0, 'num_approved': 455,
     'parties_votes': {'p.z': 0, 'vdAm.': 0, 'mHl': 189, 'r': 0, 'ir': 0, 'mrZ': 7, 'k': 0, 't': 20, 'rK': 0, 'n': 0, 'ni': 0, 'rp.': 0, 'KZ.': 0, 'iK': 0, 'ip.': 0, 'Zp.': 0, 'Ss': 41, 'K': 0, 'b': 37, 'iz': 5, 'nr': 0, 'rn': 1, 'Z.': 0, 'g': 1, 'kk.': 0, 'z': 0, 'amT': 11, 'Zi': 0, 'i': 0, 'l': 26, 'T': 30, 'Zk': 0, 'zZ.': 0, 'ph': 65, 'Kk.': 0, 'kn.': 22, 'in': 0, 'Am.': 0, 'Ki': 0}},
    {'ballot_id': '1.0', 'locality_id': '2008', 'locality_name': 'שדה יצחק', 'num_registered_voters': 426,
     'num_voted': 292, 'num_disqualified': 0, 'num_approved': 292,
     'parties_votes': {'p.z': 0, 'vdAm.': 1, 'mHl': 59, 'r': 2, 'ir': 0, 'mrZ': 22, 'k': 0, 't': 4, 'rK': 0, 'n': 0, 'ni': 0, 'rp.': 0, 'KZ.': 0, 'iK': 0, 'ip.': 0, 'Zp.': 0, 'Ss': 5, 'K': 0, 'b': 13, 'iz': 2, 'nr': 0, 'rn': 0, 'Z.': 1, 'g': 0, 'kk.': 0, 'z': 0, 'amT': 35, 'Zi': 0, 'i': 0, 'l': 13, 'T': 15, 'Zk': 0, 'zZ.': 0, 'ph': 77, 'Kk.': 0, 'kn.': 43, 'in': 0, 'Am.': 0, 'Ki': 0}},
    {'ballot_id': '260.1', 'locality_id': '9000', 'locality_name': 'באר שבע', 'num_registered_voters': 564,
     'num_voted': 405, 'num_disqualified': 1, 'num_approved': 404,
     'parties_votes': {'p.z': 0, 'vdAm.': 3, 'mHl': 170, 'r': 2, 'ir': 0, 'mrZ': 8, 'k': 0, 't': 23, 'rK': 0, 'n': 0, 'ni': 0, 'rp.': 0, 'KZ.': 0, 'iK': 0, 'ip.': 0, 'Zp.': 0, 'Ss': 13, 'K': 0, 'b': 47, 'iz': 4, 'nr': 0, 'rn': 0, 'Z.': 0, 'g': 0, 'kk.': 0, 'z': 0, 'amT': 13, 'Zi': 0, 'i': 0, 'l': 5, 'T': 25, 'Zk': 0, 'zZ.': 0, 'ph': 56, 'Kk.': 0, 'kn.': 30, 'in': 0, 'Am.': 5, 'Ki': 0}},
]
_CSV_VOTES_FILE_RECORDS_FILE2 = [
    {'ballot_id': '164.0', 'locality_id': '6100', 'locality_name': 'בני ברק', 'num_registered_voters': 694,
     'num_voted': 579, 'num_disqualified': 15, 'num_approved': 564,
     'parties_votes': {'ph': 1, 'mHl': 3, 'k.K': 0, 'mrZ': 0, 'g': 447, 'Kp.': 0, 'zi': 0, 'Ki': 0, 'p.n': 0, 'nk.': 0, 'zZ.': 0, 'iz': 0, 'amT': 0, 'Z.i': 0, 'p.k.': 0, 'Z.': 0, 'n.n': 0, 'ZK': 1, 'iZ.': 0, 'n.': 0, 'Ss': 106, 'nr': 0, 'k': 0, 'r': 0, 'zn': 0, 'z': 0, 'p.z': 0, 'nZ.': 0, 'Kn.': 0, 'p.Z.': 0, 'K': 0, 'Z.z': 0, 'p.i': 0, 'in.': 0, 'zk.': 0, 'n.k.': 0, 'i': 0, 'n': 0, 'vm.': 0, 'l': 0, 'dAm.': 0, 'nz': 0, 'tb': 6}},
]
class BallotsVotesCSVParserTest(unittest.TestCase):
    @parameterized.expand((
        ('file1', 'data/tests/ballots_votes_test_file1.csv', _CSV_VOTES_FILE_RECORDS_FILE1),
        ('file2', 'data/tests/ballots_votes_test_file2.csv', _CSV_VOTES_FILE_RECORDS_FILE2),
    ))
    def test_parses_file_correctly(self, _, path_str, expected_records):
        results = parsers.BallotsVotesFileParser(format='csv', encoding='cp1255').parse(pathlib.Path(path_str))
        self.assertEqual(results.df.to_dict('records'), expected_records)


if __name__ == '__main__':
    unittest.main()
