"""Fetches geo-location data for an address using Google GeoLocation API."""
import dataclasses
from typing import Optional, Sequence
import os

import googlemaps
import importlib_resources
import pandas as pd

import il_elections.data
from il_elections.utils import data_utils
from il_elections.utils import locally_memoize


_GEOCODING_API_KEY_ENV_VAR = 'GEOCODING_API_KEY'
_KNOWN_ADDRESSES_GEOLOCATIONS_FILENAME = 'known_addresses_geolocations.csv'


def _load_known_addresses_geolocations(filename):
    df = pd.read_csv(importlib_resources.files(il_elections.data) / filename,
                     names=['lat', 'lng', 'address'], comment='#')
    df['lat'] = df['lat'].astype(float)
    df['lng'] = df['lng'].astype(float)
    df['address'] = df['address'].astype('str').apply(data_utils.clean_hebrew_address)
    return df.set_index('address')

_KNOWN_ADDRESSES_GEOLOCATIONS = _load_known_addresses_geolocations(
    _KNOWN_ADDRESSES_GEOLOCATIONS_FILENAME)

@dataclasses.dataclass(frozen=True)
class GeoDataResults:
    longitude: float
    latitude: float

_CACHE_ONLY_MODE = os.environ.get('GEOFETCHER_CACHE_ONLY', '').lower() in ('1', 'true', 'yes')
@locally_memoize.locally_memoize(cache_only=_CACHE_ONLY_MODE, ignore_values=(None,))
def _geocode_address(address, api_key):
    gmaps = googlemaps.Client(api_key)
    try:
        return gmaps.geocode(address)
    except Exception:  # pylint: disable=broad-except
        return None


class GeoDataFetcher:
    """Uses Google geocoding API to provide geo location for addresses."""

    def __init__(self, geocoding_api_key: Optional[str] = None,
                 duplicate_known_addresses_with_prefixes: Sequence[str] = ()):
        self.api_key = geocoding_api_key or os.environ[_GEOCODING_API_KEY_ENV_VAR]
        if self.api_key is None:
            raise ValueError(
                'Geocoding API Key must be provided (`geocoding_api_key` argument or '
                f'the `{_GEOCODING_API_KEY_ENV_VAR}` environment variable).')

        # Client's init will throw a ValueError if API key is invalid. We're using it
        # just to validate on init.
        googlemaps.Client(self.api_key)

        known_with_prefixes = pd.DataFrame([
            data.rename(prefix + ' ' + index)
            for prefix in duplicate_known_addresses_with_prefixes
            for index, data in _KNOWN_ADDRESSES_GEOLOCATIONS.iterrows()])
        self.known_addresses = pd.concat((_KNOWN_ADDRESSES_GEOLOCATIONS, known_with_prefixes))


    def fetch_geocode_data(self, address: str) -> Optional[GeoDataResults]:
        """Fetches Google GeoLocation data for an address."""
        address = data_utils.clean_hebrew_address(address)

        if address in self.known_addresses.index:
            return GeoDataResults(
                longitude=self.known_addresses.loc[address]['lng'],
                latitude=self.known_addresses.loc[address]['lat'])
        results = _geocode_address(address, self.api_key)
        if results:
            # Take the first one. GMaps should only return one result except on
            # ambigious queries.
            results = results[0]
            return GeoDataResults(
                longitude=results['geometry']['location']['lng'],
                latitude=results['geometry']['location']['lat'])
        return None
