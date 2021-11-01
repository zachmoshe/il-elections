"""Debug utilities to be used in Colab."""
import itertools as it
import pathlib

import folium

from il_elections.data import geodata_fetcher
from il_elections.pipelines.preprocessing import preprocessing
from il_elections.utils import plot_utils


_DEFAULT_CONFIG_FILE = 'config/preprocessing_config.yaml'


def debug_ballot_geodata_fetching(campaign_name, locality_id, ballot_id):
    """Plots a map with all addresses relevant to the given ballot."""
    config = preprocessing.PreprocessingConfig.from_yaml(pathlib.Path(_DEFAULT_CONFIG_FILE))
    campaign_config = [c for c in config.campaigns if c.metadata.name == campaign_name]
    if not campaign_config:
        raise ValueError(f'Can\'t find campaign "{campaign_name}" in '
                         f'"{_DEFAULT_CONFIG_FILE}"')
    campaign_config = campaign_config[0]

    data = preprocessing.load_raw_campaign_data(campaign_config)
    ballot_metadata = data.metadata.df[
        (data.metadata.df.locality_id == locality_id) &
        (data.metadata.df.ballot_id == ballot_id)
    ]
    if ballot_metadata.empty:
        raise ValueError(f'Can\'t find ballot "{ballot_id}" in locality "{locality_id}"')
    df = ballot_metadata.iloc[0]

    fetcher = geodata_fetcher.GeoDataFetcher()

    fetched_locations = [
        (idx, address, fetcher.fetch_geocode_data(address))
        for idx, address in enumerate(preprocessing._normalize_optional_addresses(  # pylint: disable=protected-access
            df['locality_name'], df['location_name'], df['address']))
    ]

    results = []
    for georesult, group in it.groupby(sorted(fetched_locations,
                                              key=lambda x: (x[2].longitude, x[2].latitude)),
                                       key=lambda x: x[2]):
        group = list(group)
        results.append((
            georesult,
            '<br>'.join(f'({idx}) {address}' for idx, address, _ in group),
            min(idx for idx, _, _ in group)
        ))

    colors = ['darkred', 'red', 'lightred', 'lightred']

    m = plot_utils.ilmap(plot_utils.Maps.ISRAEL)
    for geo, addresses, min_idx in results:
        m.add_child(folium.Marker(
            location=(geo.latitude, geo.longitude), icon=folium.Icon(color=colors[min_idx]),
            tooltip=addresses))

    m.fit_bounds([(geo.latitude, geo.longitude) for geo, _, _ in results])
    return m
