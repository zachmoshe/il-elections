# IL-Elections

This project's goal is to collect and analyze data from Israel's latest election campaigns. Data includes a raw votes count for every ballot (~10K) and enriched with a geo location for the ballot's address. The raw (enriched) voting data will be available as `Parquet` files in this repo (Parquet is easily loadable by Pandas and maintains Python structures (dicts) as values).

Data was obtained from the [Israeli Central Elections Committee](https://www.bechirot.gov.il/) for every campaign I could find data files for. Required data files contain the exported data for the vote counts in each ballot, and a metadata file with the ballots' addresses. Both are required for the analysis and come in different formats and encodings. The preprocessing pipeline deals with the
different formats, enriches the data to add the geo-location and generates the final data files.

Data is currently processed for the campaigns to the 18th-24th Knessets (roughly 2009-2021).

## Project structure and general information

*  `data`: Contains the raw data files. Both the raw files from the elections committee, some GIS files and some test files.
*  `docs`: Documentation files, mostly linked form here.
* `il_elections`: Main codebase. Contains the preprocessing pipeline and helpful utilities.
* `notebooks`: Useful notebooks and usage examples.
* `outputs`: **Contains the latest preprocessing output files**. Outputs are broken by campaigns and each campaign has a `data` file and a `metadata` file. See more on data format [here](#data-spec).

### Python versions notice

The main project runs with [`Python 3.9.7`](.python-version). Notice that the external Colab currently uses `Python 3.7` so while reading the output files as dataframes works, importing some code from the project iteself might not work properly. If you require deeper usage of the code and utilities, it's advised to use Colab in a locally-hosted mode, or use Jupyter notebook locally with the same venv.


## Data Spec

### Raw input data

#### Elections Comittee

Raw input files contain two files. These come in various formats but serve (basically) the same purpose. One is a file with a table of rows per ballots and columns that contain some aggregated data about that ballot (number of voters, how many actually voted?, how many were disqualified?, etc...) and a column per party with the raw count. The other file is a metadata file on the ballots, and contains the locality name (which could be a city or a small village), the location name (e.g. "XYZ school", or "XYZ mall") and the ballot's address (which is some times useless, like in cases of a very small village with basically no street names).

#### GIS files

Geo-spatial analysis required polygons of Israel itself, together with polygons for cities where
applicable. I used [geoBoundaries](https://www.geoboundaries.org/index.html#getdata) data which
saved me tons of times!

**Manual manipulation**:
Notice that due to Israel's complicated political situation, some of the territories where ballots
were stationed might or might not be considered by you as an Israeli teritorry. For this project's
sake, all ballots have to be covered by the polygon representing Israel, so I manually manipulated
both ISR and PSE files and created a polygon which consists of Israel's non-conflicted area, the
West Bank and the Golan heights, but without Gaza. I also had to manually remove Israel's two large
water bodies (the Kinneret and the Dead Sea).


### Processed data

The [preprocessing pipeline](il_elections/pipelines/preprocessing/preprocessing.py) reads data for all [configured campaigns](config/preprocessing_config.yaml), parses it and extracts the relevant information. It then uses [Google Geocode API](https://developers.google.com/maps/documentation/geocoding/overview) to match every ballot's location with a geolocation on a map.

Also, since parties change from one campaign to another, the voting data is stored as a Python dictionary to ensure flexibility. Analyzing the data, and expanding back to columns (if makes sense) should be done by the user.

The pipeline also stores a `metadata` file for every campaign which is a YAML file with a few fields.

### Processed files examples

#### outputs/preprocessing/knesset-*.data
|            |   ballot_id |   locality_id | locality_name   |   num_registered_voters |   num_voted |   num_disqualified |   num_approved | parties_votes                     | location_name        | address       |     lat |     lng |
|:-----------|------------:|--------------:|:----------------|------------------------:|------------:|-------------------:|---------------:|:----------------------------------|:---------------------|:--------------|--------:|--------:|
| 9400-3.1   |         3.1 |          9400 | יהודמונוסון     |                     589 |         404 |                  0 |            404 | {'kn.': 51, 'mHl': 114, 'ph': 73} | בי"ס תיכון מקיף יהוד | כהן רם        | 32.0393 | 34.8931 |
| 4000-153.0 |       153   |          4000 | חיפה            |                     485 |         234 |                  4 |            230 | {'kn.': 12, 'mHl': 103, 'ph': 20} | בי"ס נירים           | יהושפט המלך,6 | 32.8075 | 34.9614 |
| 6600-114.2 |       114.2 |          6600 | חולון           |                     645 |         327 |                  3 |            324 | {'kn.': 10, 'mHl': 119, 'ph': 36} | תיכון טומשין         | קדושי קהיר,14 | 32.0189 | 34.7628 |
| 1063-14.2  |        14.2 |          1063 | מעלותתרשיחא     |                     376 |         192 |                  3 |            189 | {'kn.': 2, 'mHl': 61, 'ph': 31}   | גן ורד               | מרווה,1       | 33.0206 | 35.2879 |
| 3000-203.0 |       203   |          3000 | ירושלים         |                     596 |         252 |                  1 |            251 | {'kn.': 5, 'mHl': 86, 'ph': 16}   | גן בר אילן           | שיריזלי,6     | 31.7828 | 35.2138 |

**Notice: For brevity purposes, `parties_votes` was trimmed to contain only 3 parties.**

#### outputs/preprocessing/knesset-*.metadata

```
date: 2021-03-23
name: knesset-24
```


### Loading as `geopandas.GeoDataFrame`

The raw data is stored as plain dataframes for greater flexibility but in case you wish to work with the data as a `GeoDataFrame`, the following utility loads both files, converts and aggregates the datafiles:

The CRS is `EPSG:32636` (UTM, zone 36).

```
from il_elections.utils import data_utils
preprocessed_data_folder = pathlib.Path('outputs/preprocessing')
gdf = data_utils.load_preprocessed_campaign_data(preprocessed_data_folder, '24')
```
It returns a `PreprocessedCampaignData` dataclass with the following attributes:

*  `raw_votes`: Contains the [raw votes table](#outputspreprocessingknesset-data) with a `Point` geometry from the lat/lng fields.
*  `per_location`: Contains an aggregation of ballots based on the geo-location (very common to have many ballots in the same school for example). All fields will have the reasonable aggregation (sum if possible, or a list with all values. parties_votes are summed per key):

|      |     lng |     lat |   num_ballots | ballot_id                 |   locality_id | locality_name     | location_name               | address            |   num_registered_voters |   num_voted |   num_disqualified |   num_approved | parties_votes                      | geometry                                    |
|-----:|--------:|--------:|--------------:|:--------------------------|--------------:|:------------------|:----------------------------|:-------------------|------------------------:|------------:|-------------------:|---------------:|:-----------------------------------|:--------------------------------------------|
| 1591 | 34.9026 | 31.9583 |             2 | ['5.0', '8.0']            |          7000 | לוד               | ['בי"ס אל-ראשדיה']          | ['הגיא,1']         |                    1119 |         462 |                 10 |            452 | {'kn.': 1, 'mHl': 20, 'ph': 5}     | POINT (679804.362800942 3537392.891052161)  |
|   24 | 34.4206 | 30.8864 |             1 | ['1.0']                   |          1195 | ניצנה קהילת חינוך | ['מועדון']                  | ['מרכז קליטה']     |                     229 |          63 |                  1 |             62 | {'kn.': 9, 'mHl': 8, 'ph': 8}      | POINT (635785.713139746 3417877.263769928)  |
|  319 | 34.7211 | 31.0452 |             2 | ['1.1', '1.2']            |           354 | רביבים            | ['מועדון', 'ספריה']         | ['רביבים']         |                     712 |         439 |                  2 |            437 | {'kn.': 54, 'mHl': 25, 'ph': 87}   | POINT (664239.4082131258 3435883.075058983) |
| 1512 | 34.892  | 32.0759 |             3 | ['212.0', '68.0', '70.0'] |          7900 | פתח תקווה         | ['ביה"ס אור חיה']           | ['שפרינצק,17']     |                    1753 |         992 |                 10 |            982 | {'kn.': 49, 'mHl': 360, 'ph': 144} | POINT (678579.1761500214 3550411.44936822)  |
| 2590 | 35.1052 | 32.6543 |             3 | ['11.0', '6.0', '8.0']    |           240 | יקנעם עילית       | ['בי"ס אורט-ליד אשכול פיס'] | ['שד יצחק רבין,4'] |                    1765 |         841 |                  6 |            835 | {'kn.': 66, 'mHl': 347, 'ph': 70}  | POINT (697438.8435160784 3614921.163022358) |

**Notice: For brevity purposes, `parties_votes` was trimmed to contain only 3 parties.**
In this dataframe, every location will appear only once.

*  `metadata`: A `CampaignMetadata` object (same fields as in the metadata file).


## [Setting up an environment](docs/environment_setup.md)
## [Parsing raw files](docs/parsing_raw_files.md)
## [Preprocessing Pipeline](docs/preprocessing_pipeline.md)
## [Geocoding and locally_memoize](docs/geocoding_and_locally_memoize.md)
