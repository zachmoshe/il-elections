"""A script to enrich only a sprcific locality from a campaign for easier debugging."""
import pathlib

from absl import app
from absl import flags

from il_elections.pipelines.preprocessing import preprocessing

_FLAG_CAMPAIGN = flags.DEFINE_string('campaign', None, 'Run only this single campaign.')
_FLAG_LOCALITIES = flags.DEFINE_list('localities', None, 'Filter only these localities.')
FLAGS = flags.FLAGS

_DEFAULT_CONFIG_FILE = 'config/preprocessing_config.yaml'


def main(_):
    config = preprocessing.PreprocessingConfig.from_yaml(pathlib.Path(_DEFAULT_CONFIG_FILE))

    if _FLAG_CAMPAIGN.value:
        config = preprocessing.PreprocessingConfig(
            campaigns=[c for c in config.campaigns
                       if c.metadata.name == _FLAG_CAMPAIGN.value])
    print(config)

    for campaign_config in config.campaigns:
        # Load data
        data = preprocessing.load_raw_campaign_data(campaign_config)

        metadata = data.metadata.df
        if _FLAG_LOCALITIES.value:
            metadata = metadata.query('locality_id==@_FLAG_LOCALITIES.value')

        enriched_metadata = preprocessing.enrich_metadata_with_geolocation(metadata)
        print(enriched_metadata)


if __name__ == '__main__':
    app.run(main)
