# Preprocessing Pipeline

The preprocessing pipeline iterates over [configured campaigns](/config/preprocessing_config.yaml), reads the provided data, enriches it with geolocation information and stores it in the `outputs` folder for easy use as Parquet files.

## preprocessing_config.yaml

Stores all campaigns, data locations and the relevant parser to use for each file. Notice that parsers are specified by their keys in the parsers registry (see [parsers.py](/il_elections/data/parsers.py) for the full list).

An example to a campaign configuration:

```
    - campaign_name: knesset-24
      campaign_date: !!timestamp 2021-03-23
      data:
        ballots_votes:
          filename: data/elections/knesset-24/expb-24.csv
          format: csv-windows
        ballots_metadata:
          filename: data/elections/knesset-24/kalpies_report_tofes_b_18.3.21-24.xlsx
          format: excel
```

## Enrichment strategy

First, matching between ballots in the votes dataframe and the metadata dataframe is done based on the `locality_id` and the `ballot_id`. Then, for every ballot we have three fields which might be relevant for the geocoding process:

*  `locality_name`: A locality could be a major city name (תל אביב), a small village name (נטף) or even places which are harder to place on a map (שבט אבו רובייעה).
*  `location_name`: A location is the name fo the actual place where the ballot was placed at. This again could range from names of schools or communal centers to meaningless locations like 'מכולת' in very small villages where it might makes sense...
* `address`: The string address as was parsed from the metadata file. In large cities this would normally be identifiable by Google. In smaller places, where actual addresses not always exist, this normally is equal to the location name.

In order to enrich a metadata dataframe with geolocation we roughly follow this procedure:

1. We iterate over ballots grouped by the locality.

    1. For every locality, we query the locality name alone to get the center point. We also try to find a matching bounding polygon in the ISR_ADM2 GIS file.

    1. We generate a list of ordered candidates for the address string from all three available fields (see [il_elections/preprocessing/preprocessing.py:_normalize_optional_addresses()](/il_elections/pipelines/preprocessing/preprocessing.py))

    1. For every ballot, we try to geocode addressed at the order they were provided. If Google couldn't find a match, or returned a point outside of the bounded polygon (or too dar away from the center, if no polygon was found), we skip it and go over to the next address candidate.

1. Sometimes, ballot IDs does not appear in the metadata file. In that case we use a hueristic to try and find the actual match and in case it doesn't help, we use the average point all ballots in the locality as the geopoint.

## Output format and location

*  `<campaign_name>.metadata`: A YAML file with the metadata (currently name and date).
* `<campaign_name>.data`: A Parquet file with the result dataframe as specified [here](/README.md#processed-data)

## Campaign analysis report

Every campaign will also print a summary of the generated data to allow spotting mistakes easily. This is not instead of manual inspection in Colab.

```
Analysis report for knesset-24:
==============================================
Total Num Voters: 6578084
Total Num Voted: 4436365 (67.44%)

Most votes parties:
+-----+---------+
| mHl | 1066892 |
+-----+---------+
| ph  |  614112 |
+-----+---------+
| Ss  |  316008 |
+-----+---------+
| kn. |  292257 |
+-----+---------+
| b   |  273836 |
+-----+---------+
Missing GeoLocations:
+---------------------------------------+---------+
| locality_name,location_name,address   |   count |
+=======================================+=========+
| ('מעטפות חיצוניות', 'NA', 'NA')       |     799 |
+---------------------------------------+---------+
```
