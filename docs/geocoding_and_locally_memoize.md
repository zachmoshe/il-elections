# Geocoding

Geocoding is the process of converting string addresses into points on a map. In this project we use [Google's Geocoding API](https://developers.google.com/maps/documentation/geocoding/overview).

**Notice that this service is not free but has a $200 mothly free-tier which is enough for our purposes**.

If you intend to run the preprocessing pipeline from scratch (rather than just use the stored output files), you'll have to [issue a Google API for the GeoCoder](https://developers.google.com/maps/documentation/geocoding/get-api-key). Once you obtain a working API key, put it in your `.env` file and the GeoCoder will automatically pick it up from the environment variable.

```
GEOCODING_API_KEY=MY_API_KEY
```

Since every request to the geocoding service costs money (and some time), when re-running the pipeline for small fixes that are not related to the addresses enrichment it would be a waste to keep calling the actual service. We use [locally_memoize](/il_elections/utils/locally_memoize.py) to serve results from a local cache and avoid that.

# `locally_memoize`

`locally_memoize` is a utility that wraps any function and will memoize the return values (per input) in a local folder instead of in memory. That keeps results between runs and avoids more complex mechanisms. Using `locally_memoize` is easy and is basically just wrapping the core function (that actually performs the service API call) with a decorator:

```
@locally_memoize.locally_memoize()
def _geocode_address(address, api_key):
    return googlemaps.Client(api_key).geocode(address)
```

Notice that all arguments to the method (`_geocode_address`) will be serialized and hashed to serve as the cache key, so avoid passing complex objects there.

In order to protect accidental runs from using the actual service because of a bug, an environment variable can be set and enforce results to be returned only from the local cache (or raise an exception). This makes sure that you will not be charged unless you specify that this is a run that may use the service. Add the following to `.env` to set that:

```
GEOFETCHER_CACHE_ONLY=true
```