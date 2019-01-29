import aiohttp
import hashlib
import json
from functools import wraps

_API_BASE_URL = "https://data.orleans-metropole.fr"


def _compute_cache_key(cache_key, segmentations_keys, dimensions):
    if not isinstance(segmentations_keys, list) or len(segmentations_keys) <= 0:
        return cache_key

    _d = { k: v for k,v in dimensions.items() if v and k in segmentations_keys }

    if not bool(_d):
        return cache_key

    return f"{cache_key}{hashlib.sha224(json.dumps(_d).encode('utf-8')).hexdigest()}"


def cache(cache_key, segmentation_keys=None, expiration=300):
    def cache_decorator(f):
        @wraps(f)
        async def wrapper(*args, **kwargs):
            _key = _compute_cache_key(cache_key, segmentation_keys, kwargs)
            cached_value = await kwargs["redis"].get(_key)

            if cached_value:
                decoded_value = json.loads(cached_value)
                print(f"@cache(hit): {_key}")
                return decoded_value

            result = await f(*args, **kwargs)

            print(f"@cache(set): {_key}")
            await kwargs["redis"].setex(_key, expiration, json.dumps(result))
            return result
        return wrapper
    return cache_decorator


@cache("parking_availabilities", segmentation_keys=["query", "offset", "limit"])
async def fetch_parking_availabilities(query=None, redis=None, offset=None, limit=None):
    url = f"{_API_BASE_URL}/api/records/1.0/search/?dataset=mobilite-places-disponibles-parkings-en-temps-reel"
    if query:
        url += f"&q={query}"
    if offset:
        url += f"&start={offset}"
    if limit:
        url += f"&rows={limit}"

    async with aiohttp.ClientSession() as session:
        async with session.get(url) as r:
            json_body = await r.json()

    # json_body = {'nhits': 23, 'parameters': {'dataset': ['mobilite-places-disponibles-parkings-en-temps-reel'], 'timezone': 'UTC', 'rows': 10, 'format': 'json'}, 'records': [{'datasetid': 'mobilite-places-disponibles-parkings-en-temps-reel', 'recordid': '65ed9c1561cf4d2c5236856db171a1206095bcf6', 'fields': {'name': 'Les Halles Charpenterie', 'dispo': 306, 'datetime': '2019-01-11T07:01:03+00:00', 'disponibilite': 63.09278350515464, 'coords': [47.8984343, 1.9080055], 'total': 485, 'id': 'P6'}, 'geometry': {'type': 'Point', 'coordinates': [1.9080055, 47.8984343]}, 'record_timestamp': '2019-01-11T06:01:00+00:00'}, {'datasetid': 'mobilite-places-disponibles-parkings-en-temps-reel', 'recordid': 'e604de8fe1f71ac003d992ba2ea650bcf089a6a3', 'fields': {'name': 'Cheval Rouge', 'dispo': 155, 'datetime': '2019-01-11T07:01:03+00:00', 'disponibilite': 62.0, 'coords': [47.89954, 1.90269], 'total': 250, 'id': 'P9'}, 'geometry': {'type': 'Point', 'coordinates': [1.90269, 47.89954]}, 'record_timestamp': '2019-01-11T06:01:00+00:00'}, {'datasetid': 'mobilite-places-disponibles-parkings-en-temps-reel', 'recordid': 'ca8dad4be260e0ce68d7696e53110462bebf88d4', 'fields': {'name': 'Gare', 'dispo': 35, 'datetime': '2019-01-11T07:01:03+00:00', 'disponibilite': 35.714285714285715, 'coords': [47.9099708, 1.9056237], 'total': 98, 'id': 'P3'}, 'geometry': {'type': 'Point', 'coordinates': [1.9056237, 47.9099708]}, 'record_timestamp': '2019-01-11T06:01:00+00:00'}, {'datasetid': 'mobilite-places-disponibles-parkings-en-temps-reel', 'recordid': 'b2f448a41d8f327e62dd61864bb884b74a847909', 'fields': {'name': 'Pompidou', 'dispo': 116, 'datetime': '2019-01-11T07:01:03+00:00', 'disponibilite': 70.3030303030303, 'coords': [47.8967, 1.8533], 'total': 165, 'id': 'P19'}, 'geometry': {'type': 'Point', 'coordinates': [1.8533, 47.8967]}, 'record_timestamp': '2019-01-11T06:01:00+00:00'}, {'datasetid': 'mobilite-places-disponibles-parkings-en-temps-reel', 'recordid': '39ecda2929d6cfa29d20942ae5c007530aecac83', 'fields': {'name': "Pont de l'Europe", 'dispo': 105, 'datetime': '2019-01-11T07:01:03+00:00', 'disponibilite': 90.51724137931035, 'coords': [47.898689, 1.876181], 'total': 116, 'id': 'P17'}, 'geometry': {'type': 'Point', 'coordinates': [1.876181, 47.898689]}, 'record_timestamp': '2019-01-11T06:01:00+00:00'}, {'datasetid': 'mobilite-places-disponibles-parkings-en-temps-reel', 'recordid': 'ce7a571c9eecd3e01cd67514968e7d797b024127', 'fields': {'name': 'Hôtel de ville', 'dispo': 227, 'datetime': '2019-01-11T07:01:03+00:00', 'disponibilite': 49.45533769063181, 'coords': [47.903311, 1.910044], 'total': 459, 'id': 'P12'}, 'geometry': {'type': 'Point', 'coordinates': [1.910044, 47.903311]}, 'record_timestamp': '2019-01-11T06:01:00+00:00'}, {'datasetid': 'mobilite-places-disponibles-parkings-en-temps-reel', 'recordid': '2898ca8a38ff17fcc02aad1cc7f4deca1d0dddf5', 'fields': {'name': 'Clos du Hameau', 'dispo': 0, 'datetime': '2019-01-11T07:01:03+00:00', 'disponibilite': 0.0, 'coords': [47.911395, 1.974918], 'total': 0, 'id': 'P21'}, 'geometry': {'type': 'Point', 'coordinates': [1.974918, 47.911395]}, 'record_timestamp': '2019-01-11T06:01:00+00:00'}, {'datasetid': 'mobilite-places-disponibles-parkings-en-temps-reel', 'recordid': 'e8ed737118d6b01e1d3bb33246a504a40927cbe8', 'fields': {'name': 'Patinoire', 'dispo': 195, 'datetime': '2019-01-11T07:01:03+00:00', 'disponibilite': 44.93087557603687, 'coords': [47.9031887, 1.8933123], 'total': 434, 'id': 'P4'}, 'geometry': {'type': 'Point', 'coordinates': [1.8933123, 47.9031887]}, 'record_timestamp': '2019-01-11T06:01:00+00:00'}, {'datasetid': 'mobilite-places-disponibles-parkings-en-temps-reel', 'recordid': '4ab8aaf25a49cc57b86486d09e2e035e0d277594', 'fields': {'name': 'Martroi', 'dispo': 88, 'datetime': '2019-01-11T07:01:03+00:00', 'disponibilite': 30.555555555555557, 'coords': [47.902851, 1.903546], 'total': 288, 'id': 'P13'}, 'geometry': {'type': 'Point', 'coordinates': [1.903546, 47.902851]}, 'record_timestamp': '2019-01-11T06:01:00+00:00'}, {'datasetid': 'mobilite-places-disponibles-parkings-en-temps-reel', 'recordid': 'ed3170779defd910bec4fde52fed50426881df33', 'fields': {'name': 'Médiathèque', 'dispo': 223, 'datetime': '2019-01-11T07:01:03+00:00', 'disponibilite': 54.390243902439025, 'coords': [47.9066841, 1.8999696], 'total': 410, 'id': 'P1'}, 'geometry': {'type': 'Point', 'coordinates': [1.8999696, 47.9066841]}, 'record_timestamp': '2019-01-11T06:01:00+00:00'}]}

    return json_body


@cache("tram_stations", segmentation_keys=["query", "offset", "limit"])
async def fetch_tram_stations(query=None, redis=None, offset=None, limit=None):
    url = f"{_API_BASE_URL}/api/records/1.0/search/?dataset=mobilitetram_stations&facet=tram"
    if query:
        url += f"&q={query}"
    if offset:
        url += f"&start={offset}"
    if limit:
        url += f"&rows={limit}"

    async with aiohttp.ClientSession() as session:
        async with session.get(url) as r:
            json_body = await r.json()

    # json_body = {"nhits":50,"parameters":{"dataset":["mobilitetram_stations"],"timezone":"UTC","rows":10,"format":"json","facet":["tram"]},"records":[{"datasetid":"mobilitetram_stations","recordid":"a314379b50571ea1fa8c6f9e804363bd219bece3","fields":{"geo_shape":{"type":"Point","coordinates":[1.9025135649038312,47.901265552414955]},"tram":"B","nom_statio":"DE GAULLE","geo_point_2d":[47.901265552414955,1.9025135649038312]},"geometry":{"type":"Point","coordinates":[1.9025135649038312,47.901265552414955]},"record_timestamp":"2018-12-13T15:18:46+00:00"},{"datasetid":"mobilitetram_stations","recordid":"72b3be5dc9ded46463d35f5b86dafda382f54449","fields":{"geo_shape":{"type":"Point","coordinates":[1.911273457256876,47.90949726795272]},"tram":"B","nom_statio":"EUGENE VIGNAT","geo_point_2d":[47.90949726795272,1.911273457256876]},"geometry":{"type":"Point","coordinates":[1.911273457256876,47.90949726795272]},"record_timestamp":"2018-12-13T15:18:46+00:00"},{"datasetid":"mobilitetram_stations","recordid":"f26ec156218d3f609396ae5710e321c1b3e9ff4d","fields":{"geo_shape":{"type":"Point","coordinates":[1.91295635158663,47.92923750852926]},"tram":"A","nom_statio":"BUSTIERE","geo_point_2d":[47.92923750852926,1.91295635158663]},"geometry":{"type":"Point","coordinates":[1.91295635158663,47.92923750852926]},"record_timestamp":"2018-12-13T15:18:46+00:00"},{"datasetid":"mobilitetram_stations","recordid":"e19f9c6c30afa508984b87c44571e15ea0b0dade","fields":{"geo_shape":{"type":"Point","coordinates":[1.919404052208939,47.92809770901405]},"tram":"A","nom_statio":"LAMBALLE","geo_point_2d":[47.92809770901405,1.919404052208939]},"geometry":{"type":"Point","coordinates":[1.919404052208939,47.92809770901405]},"record_timestamp":"2018-12-13T15:18:46+00:00"},{"datasetid":"mobilitetram_stations","recordid":"5546fc2e7eefb52e7a21f83bdaa55a5795890dec","fields":{"geo_shape":{"type":"Point","coordinates":[1.91233165117739,47.869215589393775]},"tram":"A","nom_statio":"ZENITH-PARC DES EXPOSITIONS","geo_point_2d":[47.869215589393775,1.91233165117739]},"geometry":{"type":"Point","coordinates":[1.91233165117739,47.869215589393775]},"record_timestamp":"2018-12-13T15:18:46+00:00"},{"datasetid":"mobilitetram_stations","recordid":"35a10541cbaad9b5f7fc1f4cb086e78b9523160b","fields":{"geo_shape":{"type":"Point","coordinates":[1.925345770039137,47.849724690588815]},"tram":"A","nom_statio":"LORETTE","geo_point_2d":[47.849724690588815,1.925345770039137]},"geometry":{"type":"Point","coordinates":[1.925345770039137,47.849724690588815]},"record_timestamp":"2018-12-13T15:18:46+00:00"},{"datasetid":"mobilitetram_stations","recordid":"0313fc19bff81ef13706a1d554f9e7234a56a506","fields":{"geo_shape":{"type":"Point","coordinates":[1.9289631389461601,47.831591430121826]},"tram":"A","nom_statio":"BOLIERE","geo_point_2d":[47.831591430121826,1.9289631389461601]},"geometry":{"type":"Point","coordinates":[1.9289631389461601,47.831591430121826]},"record_timestamp":"2018-12-13T15:18:46+00:00"},{"datasetid":"mobilitetram_stations","recordid":"4371be02552725e3b72422d0185d5e635d79bb74","fields":{"geo_shape":{"type":"Point","coordinates":[1.9459476546882302,47.91024146060798]},"tram":"B","nom_statio":"GAUDIER BRZESKA","geo_point_2d":[47.91024146060798,1.9459476546882302]},"geometry":{"type":"Point","coordinates":[1.9459476546882302,47.91024146060798]},"record_timestamp":"2018-12-13T15:18:46+00:00"},{"datasetid":"mobilitetram_stations","recordid":"aeb67a66bfe88c263ecfc9ee0f3949858544da5b","fields":{"geo_shape":{"type":"Point","coordinates":[1.931303211294127,47.84701934453634]},"tram":"A","nom_statio":"UNIVERSITE CHATEAU","geo_point_2d":[47.84701934453634,1.931303211294127]},"geometry":{"type":"Point","coordinates":[1.931303211294127,47.84701934453634]},"record_timestamp":"2018-12-13T15:18:46+00:00"},{"datasetid":"mobilitetram_stations","recordid":"31deab17b115161683beeb5ac2cf2fcc7ecfc456","fields":{"geo_shape":{"type":"Point","coordinates":[1.866883936359932,47.90181203210111]},"tram":"B","nom_statio":"MARTIN LUTHER KING","geo_point_2d":[47.90181203210111,1.866883936359932]},"geometry":{"type":"Point","coordinates":[1.866883936359932,47.90181203210111]},"record_timestamp":"2018-12-13T15:18:46+00:00"}],"facet_groups":[{"name":"tram","facets":[{"name":"A","path":"A","count":25,"state":"displayed"},{"name":"B","path":"B","count":25,"state":"displayed"}]}]}

    return json_body


@cache("orleans_cantons", segmentation_keys=["offset", "limit"])
async def fetch_orleans_cantons(redis=None, offset=None, limit=None):
    url = f"{_API_BASE_URL}/api/records/1.0/search/?dataset=administratif_adm_canton&facet=circonscri&facet=codcomm"
    if offset:
        url += f"&start={offset}"
    if limit:
        url += f"&rows={limit}"

    async with aiohttp.ClientSession() as session:
        async with session.get(url) as r:
            json_body = await r.json()

    return json_body


@cache("orleans_towns", segmentation_keys=["offset", "limit"])
async def fetch_orleans_towns(redis=None, offset=None, limit=None):
    url = f"{_API_BASE_URL}/api/records/1.0/search/?dataset=administratif_adm_com_agglo"
    if offset:
        url += f"&start={offset}"
    if limit:
        url += f"&rows={limit}"

    async with aiohttp.ClientSession() as session:
        async with session.get(url) as r:
            json_body = await r.json()

    return json_body


@cache("orleans_districts", segmentation_keys=["query", "offset", "limit"])
async def fetch_orleans_districts(query=None, redis=None, offset=None, limit=None):
    url = f"{_API_BASE_URL}/api/records/1.0/search/?dataset=administratif_adm_quartier&facet=nom"
    if query:
        url += f"&q={query}"
    if offset:
        url += f"&start={offset}"
    if limit:
        url += f"&rows={limit}"

    async with aiohttp.ClientSession() as session:
        async with session.get(url) as r:
            json_body = await r.json()

    return json_body


@cache("orleans_townhalls", segmentation_keys=["query", "offset", "limit"])
async def fetch_orleans_townhalls(query=None, redis=None, offset=None, limit=None):
    url = f"{_API_BASE_URL}/api/records/1.0/search/?dataset=administratif_adm_mairie_quartier"
    if query:
        url += f"&q={query}"
    if offset:
        url += f"&start={offset}"
    if limit:
        url += f"&rows={limit}"

    async with aiohttp.ClientSession() as session:
        async with session.get(url) as r:
            json_body = await r.json()

    return json_body
