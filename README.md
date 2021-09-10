# IL-Elections

This project's goal is to collect and analyze data from Israel's last election campigns (and there's been a few lately) and play with that a little bit.

Other than my experiments, the processed dataframes will be available for everyone who wants to continue digging into this.

Data was obtained from the [Israeli Central Elections Committee](https://www.bechirot.gov.il/) for every campign I could find the exported data for the vote counts in each ballot, and a metadata file with ballot's addresses. This results with various file formats and encodings for the campigns to the 18th-24th Knessets (roughly 2009-2021). 

## [TBD] Input Data Files

- main fields in votes and ballots files. 
- different formats and parsers.
- config/preprocessing_config.yaml.

## [TBD] Preprocessing Pipeline

- geocoding. Google's API. Why locally_memoize and how to use it (ENV VAR)
- enriching "strategy".
- validating results?
- results format and location.


## Misc.

- [TBD] .env file (not in git)