"""Runs the preprocessing pipeline.

The pipeline loads data for different campigns from different sources, aligns everything together,
enriches the data with GeoLocation fields and stores everything as metadata file and data file
which contains the dataframe with all relevent fields.
"""
import dataclasses
import pathlib
import shutil
import sys
import yaml

from absl import app
from absl import flags
from absl import logging
import pandas as pd
import tabulate

from il_elections.pipelines.preprocessing import preprocessing


_DEFAULT_OUTPUT_FOLDER = 'outputs/preprocessing'
_DEFAULT_CONFIG_FILE = 'config/preprocessing_config.yaml'

_FLAG_CONFIG_FILE = flags.DEFINE_string(
    'config_file', _DEFAULT_CONFIG_FILE, 'Config filename to use')
_FLAG_OVERRIDE = flags.DEFINE_bool(
    'override', False,
    'Whether to override the output folder if exists')
_FLAG_OUTPUT_FODLER = flags.DEFINE_string(
    'output_folder', _DEFAULT_OUTPUT_FOLDER,
    'Output folder for preprocessed results')
_FLAG_SINGLE_CAMPIGN = flags.DEFINE_string(
    'single_campign', None, 'Only run this campign from the config (None runs all)')


def _print_campign_data_analysis(campign_metadata: preprocessing.CampignMetadata,
                                 campign_data_analysis: preprocessing.CampignDataAnalysis):
    logging_parts = [
        f'''
Analysis report for {campign_metadata.name}:
==============================================
Total Num Voters: {campign_data_analysis.num_voters}
Total Num Voted: {campign_data_analysis.num_voted} ({campign_data_analysis.voting_ratio:2.2%})
''',
        'Most votes parties:',
        tabulate.tabulate(
            (pd.Series(campign_data_analysis.parties_votes, name='count')
             .sort_values(ascending=False).iloc[:5].to_frame()),
            tablefmt='grid', floatfmt='f'),
        'Missing GeoLocations:',
        tabulate.tabulate(
            campign_data_analysis.missing_geo_location.to_frame(),
            tablefmt='grid',
            headers=(','.join(campign_data_analysis.missing_geo_location.index.names),
                     'count')),
    ]
    logging.info('\n'.join([] + logging_parts))


def main(_):
    output_path = pathlib.Path(_FLAG_OUTPUT_FODLER.value)
    if output_path.exists():
        if _FLAG_OVERRIDE.value:
            shutil.rmtree(output_path)
        else:
            logging.error('Output folder already exists. '
                          'Override flag is set to false. Can\'t continue')
            sys.exit(1)
    output_path.mkdir(parents=True, exist_ok=True)

    config = preprocessing.PreprocessingConfig.from_yaml(
        _FLAG_CONFIG_FILE.value)

    single_campign = _FLAG_SINGLE_CAMPIGN.value
    if single_campign:
        campigns_by_name = {campign.metadata.name: campign for campign in config.campigns}
        if single_campign not in campigns_by_name:
            sys.exit(1)
        logging.info(f'Will only process campign "{single_campign}".')
        config = dataclasses.replace(config, campigns=[campigns_by_name[single_campign]])

    logging.info(f'Config loaded. Found {len(config.campigns)} campigns to preprocess.')
    preprocessed_data_iter = preprocessing.preprocess(config)

    for campign_metadata, campign_df in preprocessed_data_iter:
        logging.info(f'Got data for campign "{campign_metadata.name}". Storing to output folder.')
        metadata_filename = campign_metadata.name + '.metadata'
        data_filename = campign_metadata.name + '.data'

        # Dump metadata
        with open(output_path / metadata_filename, 'wt', encoding='utf8') as f:
            yaml.dump(dataclasses.asdict(campign_metadata), f, encoding='utf8')
        # Dump dataframe
        campign_df.to_parquet(output_path / data_filename)

        # Analyze campign data and print report
        campign_data_analysis = preprocessing.analyze_campign_data(campign_df)
        _print_campign_data_analysis(campign_metadata, campign_data_analysis)

if __name__ == '__main__':
    app.run(main)
