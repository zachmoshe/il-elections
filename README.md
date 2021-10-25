# IL-Elections

This project's goal is to collect and analyze data from Israel's last election campaigns (and there's been a few lately) and play with that a little bit.

Other than my experiments, the processed dataframes will be available for everyone who wants to continue digging into this.

Data was obtained from the [Israeli Central Elections Committee](https://www.bechirot.gov.il/) for every campaign I could find the exported data for the vote counts in each ballot, and a metadata file with ballot's addresses. This results with various file formats and encodings for the campaigns to the 18th-24th Knessets (roughly 2009-2021).

## How to set up environment

- Install [pyenv](https://github.com/pyenv/pyenv#installation).
- Install Python. On MacOS, use this (handles some requirements and compiler paths):
```
bin/install_python_on_macos.sh
```
If not on MacOS install Python:
```
pyenv install $(cat .python-version)
```
- Create a virtualenv
```
python -m venv .venv
```
- Install [poetry](https://python-poetry.org/docs/#installation) (inside the virtual env).
- Install requirements:
```
poetry install
```


## [TBD] Input Data Files

- main fields in votes and ballots files.
- different formats and parsers. Mention PDF parsing problems any why we need to manually convert to XLSX
- config/preprocessing_config.yaml.
- Israel polygon - explain why joining Israel and West Bank. Download files from https://www.geoboundaries.org/index.html#getdata for "Israel (ADM0)" and "State of Palestine (ADM0)". mention data_utils.load_israel_polygon()


## [TBD] Preprocessing Pipeline

- geocoding. Google's API. Why locally_memoize and how to use it (ENV VAR)
- enriching "strategy".
- validating results?
- results format and location.


## Misc.

- [TBD] .env file (not in git)
