import dataclasses
from typing import Optional
import os 
import pandas as pd

import googlemaps
import importlib_resources

import il_elections.data
from il_elections.utils import locally_memoize


_GEOCODING_API_KEY_ENV_VAR = 'GEOCODING_API_KEY'
_KNOWN_ADDRESSES_GEOLOCATIONS_FILENAME = 'known_addresses_geolocations.csv'

def _load_known_addresses_geolocations(filename):
    df = pd.read_csv(importlib_resources.files(il_elections.data) / filename,
                     names=['lat', 'lng', 'address'], comment='#')
    df['lat'] = df['lat'].astype(float)
    df['lng'] = df['lng'].astype(float)
    return df.set_index('address')

_KNOWN_ADDRESSES_GEOLOCATIONS = _load_known_addresses_geolocations(_KNOWN_ADDRESSES_GEOLOCATIONS_FILENAME)

@dataclasses.dataclass(frozen=True)
class GeoDataResults:
    longitude: float
    latitude: float

_CACHE_ONLY_MODE = os.environ.get('GEOFETCHER_CACHE_ONLY', '').lower() in ('1', 'true', 'yes')
@locally_memoize.locally_memoize(cache_only=_CACHE_ONLY_MODE)
def _geocode_address(address, api_key):
    gmaps = googlemaps.Client(api_key)
    try: 
        return gmaps.geocode(address)
    except Exception:
        return None


class GeoDataFetcher:
    """Uses Google geocoding API to provide geo location for addresses."""

    def __init__(self, geocoding_api_key: Optional[str] = None):
        self.api_key = geocoding_api_key or os.environ[_GEOCODING_API_KEY_ENV_VAR]
        if self.api_key is None:
            raise ValueError(
                'Geocoding API Key must be provided (`geocoding_api_key` argument or '
                f'the `{_GEOCODING_API_KEY_ENV_VAR}` environment variable).')

        # Client's init will throw a ValueError if API key is invalid. We're using it
        # just to validate on init.
        googlemaps.Client(self.api_key)

    def fetch_geocode_data(self, address: str) -> Optional[GeoDataResults]:
        if address in _KNOWN_ADDRESSES_GEOLOCATIONS.index:
            return GeoDataResults(
                longitude=_KNOWN_ADDRESSES_GEOLOCATIONS.loc[address]['lng'],
                latitude=_KNOWN_ADDRESSES_GEOLOCATIONS.loc[address]['lat'])
        else:
            results = _geocode_address(address, self.api_key)
            if results:
                # Take the first one. GMaps should only return one result except on
                # ambigious queries.
                results = results[0]
                return GeoDataResults(
                    longitude=results['geometry']['location']['lng'],
                    latitude=results['geometry']['location']['lat'])
            else:
                return None
