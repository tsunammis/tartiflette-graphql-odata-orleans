import aiohttp
from functools import wraps
import json

_API_BASE_URL = "https://data.orleans-metropole.fr"

def cache(cache_key):
    def cache_decorator(f):
        @wraps(f)
        async def wrapper(*args, **kwargs):
            cached_value = await kwargs["redis"].get(cache_key)

            if cached_value:
                decoded_value = json.loads(cached_value)
                print(f"return cache: {cache_key}")
                return decoded_value

            result = await f(*args, **kwargs)

            print(f"set cache: {cache_key}")
            await kwargs["redis"].setex(cache_key, 300, json.dumps(result))
            return result
        return wrapper
    return cache_decorator


@cache("parking_availabilities")
async def fetch_parking_availabilities(query=None, redis=None):
    # sess = aiohttp.ClientSession

    url = f"{_API_BASE_URL}/api/records/1.0/search/?dataset=mobilite-places-disponibles-parkings-en-temps-reel"
    if query:
        url += f"&q={query}"

    # async with aiohttp.ClientSession() as session:
    #     async with session.get(url) as r:
    #         json_body = await r.json()
    #         print(json_body)

    result = {'nhits': 23, 'parameters': {'dataset': ['mobilite-places-disponibles-parkings-en-temps-reel'], 'timezone': 'UTC', 'rows': 10, 'format': 'json'}, 'records': [{'datasetid': 'mobilite-places-disponibles-parkings-en-temps-reel', 'recordid': '65ed9c1561cf4d2c5236856db171a1206095bcf6', 'fields': {'name': 'Les Halles Charpenterie', 'dispo': 306, 'datetime': '2019-01-11T07:01:03+00:00', 'disponibilite': 63.09278350515464, 'coords': [47.8984343, 1.9080055], 'total': 485, 'id': 'P6'}, 'geometry': {'type': 'Point', 'coordinates': [1.9080055, 47.8984343]}, 'record_timestamp': '2019-01-11T06:01:00+00:00'}, {'datasetid': 'mobilite-places-disponibles-parkings-en-temps-reel', 'recordid': 'e604de8fe1f71ac003d992ba2ea650bcf089a6a3', 'fields': {'name': 'Cheval Rouge', 'dispo': 155, 'datetime': '2019-01-11T07:01:03+00:00', 'disponibilite': 62.0, 'coords': [47.89954, 1.90269], 'total': 250, 'id': 'P9'}, 'geometry': {'type': 'Point', 'coordinates': [1.90269, 47.89954]}, 'record_timestamp': '2019-01-11T06:01:00+00:00'}, {'datasetid': 'mobilite-places-disponibles-parkings-en-temps-reel', 'recordid': 'ca8dad4be260e0ce68d7696e53110462bebf88d4', 'fields': {'name': 'Gare', 'dispo': 35, 'datetime': '2019-01-11T07:01:03+00:00', 'disponibilite': 35.714285714285715, 'coords': [47.9099708, 1.9056237], 'total': 98, 'id': 'P3'}, 'geometry': {'type': 'Point', 'coordinates': [1.9056237, 47.9099708]}, 'record_timestamp': '2019-01-11T06:01:00+00:00'}, {'datasetid': 'mobilite-places-disponibles-parkings-en-temps-reel', 'recordid': 'b2f448a41d8f327e62dd61864bb884b74a847909', 'fields': {'name': 'Pompidou', 'dispo': 116, 'datetime': '2019-01-11T07:01:03+00:00', 'disponibilite': 70.3030303030303, 'coords': [47.8967, 1.8533], 'total': 165, 'id': 'P19'}, 'geometry': {'type': 'Point', 'coordinates': [1.8533, 47.8967]}, 'record_timestamp': '2019-01-11T06:01:00+00:00'}, {'datasetid': 'mobilite-places-disponibles-parkings-en-temps-reel', 'recordid': '39ecda2929d6cfa29d20942ae5c007530aecac83', 'fields': {'name': "Pont de l'Europe", 'dispo': 105, 'datetime': '2019-01-11T07:01:03+00:00', 'disponibilite': 90.51724137931035, 'coords': [47.898689, 1.876181], 'total': 116, 'id': 'P17'}, 'geometry': {'type': 'Point', 'coordinates': [1.876181, 47.898689]}, 'record_timestamp': '2019-01-11T06:01:00+00:00'}, {'datasetid': 'mobilite-places-disponibles-parkings-en-temps-reel', 'recordid': 'ce7a571c9eecd3e01cd67514968e7d797b024127', 'fields': {'name': 'Hôtel de ville', 'dispo': 227, 'datetime': '2019-01-11T07:01:03+00:00', 'disponibilite': 49.45533769063181, 'coords': [47.903311, 1.910044], 'total': 459, 'id': 'P12'}, 'geometry': {'type': 'Point', 'coordinates': [1.910044, 47.903311]}, 'record_timestamp': '2019-01-11T06:01:00+00:00'}, {'datasetid': 'mobilite-places-disponibles-parkings-en-temps-reel', 'recordid': '2898ca8a38ff17fcc02aad1cc7f4deca1d0dddf5', 'fields': {'name': 'Clos du Hameau', 'dispo': 0, 'datetime': '2019-01-11T07:01:03+00:00', 'disponibilite': 0.0, 'coords': [47.911395, 1.974918], 'total': 0, 'id': 'P21'}, 'geometry': {'type': 'Point', 'coordinates': [1.974918, 47.911395]}, 'record_timestamp': '2019-01-11T06:01:00+00:00'}, {'datasetid': 'mobilite-places-disponibles-parkings-en-temps-reel', 'recordid': 'e8ed737118d6b01e1d3bb33246a504a40927cbe8', 'fields': {'name': 'Patinoire', 'dispo': 195, 'datetime': '2019-01-11T07:01:03+00:00', 'disponibilite': 44.93087557603687, 'coords': [47.9031887, 1.8933123], 'total': 434, 'id': 'P4'}, 'geometry': {'type': 'Point', 'coordinates': [1.8933123, 47.9031887]}, 'record_timestamp': '2019-01-11T06:01:00+00:00'}, {'datasetid': 'mobilite-places-disponibles-parkings-en-temps-reel', 'recordid': '4ab8aaf25a49cc57b86486d09e2e035e0d277594', 'fields': {'name': 'Martroi', 'dispo': 88, 'datetime': '2019-01-11T07:01:03+00:00', 'disponibilite': 30.555555555555557, 'coords': [47.902851, 1.903546], 'total': 288, 'id': 'P13'}, 'geometry': {'type': 'Point', 'coordinates': [1.903546, 47.902851]}, 'record_timestamp': '2019-01-11T06:01:00+00:00'}, {'datasetid': 'mobilite-places-disponibles-parkings-en-temps-reel', 'recordid': 'ed3170779defd910bec4fde52fed50426881df33', 'fields': {'name': 'Médiathèque', 'dispo': 223, 'datetime': '2019-01-11T07:01:03+00:00', 'disponibilite': 54.390243902439025, 'coords': [47.9066841, 1.8999696], 'total': 410, 'id': 'P1'}, 'geometry': {'type': 'Point', 'coordinates': [1.8999696, 47.9066841]}, 'record_timestamp': '2019-01-11T06:01:00+00:00'}]}

    return result