import collections

from tartiflette import Resolver

from open_data_orleans.manager import fetch_parking_availabilities


@Resolver("Mobility.parkings")
async def resolver_hello(parent, args, ctx, info):
    parking_availabilities = await fetch_parking_availabilities(
        redis = ctx["app"]["redis"]
    )

    if not parking_availabilities or "records" not in parking_availabilities\
        or not isinstance(parking_availabilities["records"], collections.Iterable):
        return {}

    return [{
        "id": p["fields"]["id"],
        "name": p["fields"]["name"],
        "parkingSpace": {
            "total": p["fields"]["total"],
            "currentlyAvailable": p["fields"]["dispo"]
        },
        "location": {
            "latitude": p["fields"]["coords"][0],
            "longitude": p["fields"]["coords"][1]
        }
    } for p in parking_availabilities["records"]]

    