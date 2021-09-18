import dataclasses
import pathlib
import shutil
import yaml

from absl import app
from absl import flags
from absl import logging
import importlib_resources

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


def main(argv):
    output_path = pathlib.Path(_FLAG_OUTPUT_FODLER.value)
    if output_path.exists():
        if _FLAG_OVERRIDE.value:
            shutil.rmtree(output_path)
        else:
            logging.error('Output folder already exists. '
                          'Override flag is set to false. Can\'t continue')
            exit(1)
    output_path.mkdir(parents=True, exist_ok=True)
    
    config = preprocessing.PreprocessingConfig.from_yaml(
        _FLAG_CONFIG_FILE.value)
    logging.info(f'Config loaded. Found {len(config.campigns)} campigns to preprocess.')
    
    preprocessed_data_iter = preprocessing.preprocess(config)

    for campign_metadata, campign_df in preprocessed_data_iter:
        logging.info(f'Got data for campign "{campign_metadata.name}". Storing to output folder.')
        metadata_filename = campign_metadata.name + '.metadata'
        data_filename = campign_metadata.name + '.data'

        # Dump metadata
        with open(output_path / metadata_filename, 'wt') as f:
            yaml.dump(dataclasses.asdict(campign_metadata), f)
        # Dump dataframe
        campign_df.to_parquet(output_path / data_filename)


if __name__ == '__main__':
    app.run(main)
