import collections

def transform_to_graph(api_result):
    if not api_result or "records" not in api_result\
        or not isinstance(api_result["records"], collections.Iterable):
        return {}

    return [apply_transformer(p) for p in api_result["records"]]


def apply_transformer(item):
    if not item or "datasetid" not in item:
        return None

    if item["datasetid"] == "mobilite-places-disponibles-parkings-en-temps-reel":
        return parkings_transformer(item)
    elif item["datasetid"] == "mobilitetram_stations":
        return tram_stations_transformer(item)
    elif item["datasetid"] == "administratif_adm_canton":
        return orleans_cantons_transformer(item)
    elif item["datasetid"] == "administratif_adm_com_agglo":
        return orleans_towns_transformer(item)
    elif item["datasetid"] == "administratif_adm_quartier":
        return orleans_districts_transformer(item)
    elif item["datasetid"] == "administratif_adm_mairie_quartier":
        return orleans_townhalls_transformer(item)

    raise Exception(f"datasetid {item['datasetid']} not supported")

    
def parkings_transformer(item):
    return {
        "id": item["fields"]["id"],
        "name": item["fields"]["name"],
        "parkingSpace": {
            "total": item["fields"]["total"],
            "currentlyAvailable": item["fields"]["dispo"]
        },
        "location": {
            "latitude": item["fields"]["coords"][0],
            "longitude": item["fields"]["coords"][1]
        }
    }


def tram_stations_transformer(item):
    return {
        "id": item["recordid"],
        "name": item["fields"]["nom_statio"],
        "line": item["fields"]["tram"],
        "location": {
            "latitude": item["fields"]["geo_point_2d"][0],
            "longitude": item["fields"]["geo_point_2d"][1]
        }
    }


def orleans_cantons_transformer(item):
    return {
        "id": item["recordid"],
        "name": item["fields"]["nom"],
        "district": item["fields"]["circonscri"],
        "location": {
            "latitude": item["fields"]["geo_point_2d"][0],
            "longitude": item["fields"]["geo_point_2d"][1]
        }
    }


def orleans_towns_transformer(item):
    return {
        "id": item["recordid"],
        "name": item["fields"]["nom"],
        "location": {
            "latitude": item["fields"]["geo_point_2d"][0],
            "longitude": item["fields"]["geo_point_2d"][1]
        }
    }


def orleans_districts_transformer(item):
    return {
        "id": item["recordid"],
        "name": item["fields"]["nom"],
        "wording": item["fields"]["libelle"],
        "location": {
            "latitude": item["fields"]["geo_point_2d"][0],
            "longitude": item["fields"]["geo_point_2d"][1]
        }
    }


def orleans_townhalls_transformer(item):
    return {
        "id": item["recordid"],
        "name": item["fields"]["nom"],
        "location": {
            "latitude": item["fields"]["geo_point_2d"][0],
            "longitude": item["fields"]["geo_point_2d"][1]
        }
    }