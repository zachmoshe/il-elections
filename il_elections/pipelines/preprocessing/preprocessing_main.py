"""Runs the preprocessing pipeline.

The pipeline loads data for different campaigns from different sources, aligns everything together,
enriches the data with GeoLocation fields and stores everything as metadata file and data file
which contains the dataframe with all relevent fields.
"""
import dataclasses
import pathlib
import sys
import yaml

from absl import app
from absl import flags
from absl import logging
import dotenv
import pandas as pd
import tabulate

from il_elections.data import data
from il_elections.pipelines.preprocessing import preprocessing

dotenv.load_dotenv()


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
_FLAG_SINGLE_CAMPAIGN = flags.DEFINE_string(
    'single_campaign', None, 'Only run this campaign from the config (None runs all)')


def _print_campaign_data_analysis(campaign_metadata: data.CampaignMetadata,
                                 campaign_data_analysis: preprocessing.CampaignDataAnalysis):
    logging_parts = [
        f'''
Analysis report for {campaign_metadata.name}:
==============================================
Total Num Voters: {campaign_data_analysis.num_voters}
Total Num Voted: {campaign_data_analysis.num_voted} ({campaign_data_analysis.voting_ratio:2.2%})
''',
        'Most votes parties:',
        tabulate.tabulate(
            (pd.Series(campaign_data_analysis.parties_votes, name='count')
             .sort_values(ascending=False).iloc[:5].to_frame()),
            tablefmt='grid', floatfmt='.0f'),
        'Missing GeoLocations:',
        tabulate.tabulate(
            campaign_data_analysis.missing_geo_location.to_frame(),
            tablefmt='grid',
            headers=(','.join(campaign_data_analysis.missing_geo_location.index.names),
                     'count')),
    ]
    logging.info('\n'.join([] + logging_parts))


def main(_):
    output_path = pathlib.Path(_FLAG_OUTPUT_FODLER.value)
    output_path.mkdir(parents=True, exist_ok=True)

    config = preprocessing.PreprocessingConfig.from_yaml(
        _FLAG_CONFIG_FILE.value)

    single_campaign = _FLAG_SINGLE_CAMPAIGN.value
    if single_campaign:
        campaigns_by_name = {campaign.metadata.name: campaign for campaign in config.campaigns}
        if single_campaign not in campaigns_by_name:
            sys.exit(1)
        logging.info(f'Will only process campaign "{single_campaign}".')
        config = dataclasses.replace(config, campaigns=[campaigns_by_name[single_campaign]])

    logging.info(f'Config loaded. Found {len(config.campaigns)} campaigns to preprocess.')
    preprocessed_data_iter = preprocessing.preprocess(config)

    for campaign_metadata, campaign_df in preprocessed_data_iter:
        logging.info(f'Got data for campaign "{campaign_metadata.name}". Storing to output folder.')
        metadata_path = output_path / (campaign_metadata.name + '.metadata')
        data_path = output_path / (campaign_metadata.name + '.data')

        if data_path.exists() or metadata_path.exists():
            if _FLAG_OVERRIDE.value:
                logging.info(f'Files for {campaign_metadata.name} exist. Overriding...')
                data_path.unlink()
                metadata_path.unlink()
            else:
                continue

        # Dump metadata
        with open(metadata_path, 'wt', encoding='utf8') as f:
            yaml.dump(dataclasses.asdict(campaign_metadata), f, encoding='utf8')
        # Dump dataframe
        campaign_df.to_parquet(data_path)

        # Analyze campaign data and print report
        campaign_data_analysis = preprocessing.analyze_campaign_data(campaign_df)
        _print_campaign_data_analysis(campaign_metadata, campaign_data_analysis)

def cli():
    app.run(main)

if __name__ == '__main__':
    print('Should run with `poetry run ...`')
