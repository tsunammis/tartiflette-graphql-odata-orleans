import collections

from tartiflette import Resolver

from open_data_orleans.manager import fetch_parking_availabilities
from open_data_orleans.manager import fetch_tram_stations
from open_data_orleans.manager import fetch_orleans_cantons
from open_data_orleans.manager import fetch_orleans_towns
from open_data_orleans.manager import fetch_orleans_districts
from open_data_orleans.manager import fetch_orleans_townhalls
from open_data_orleans.manager import fetch_public_events
from open_data_orleans.transformers.orleans_metropole import transform_to_graph


@Resolver("Mobility.parkings")
async def resolver_mobility_parkings(parent, args, ctx, info):
    parking_availabilities = await fetch_parking_availabilities(
        redis = ctx["app"]["redis"],
        query = args.get("query"),
        offset = args.get("offset"),
        limit = args.get("limit"),
    )

    return transform_to_graph(parking_availabilities)


@Resolver("Mobility.tramStations")
async def resolver_mobility_tram_stations(parent, args, ctx, info):
    tram_stations = await fetch_tram_stations(
        redis = ctx["app"]["redis"],
        query = args.get("query"),
        offset = args.get("offset"),
        limit = args.get("limit"),
    )

    return transform_to_graph(tram_stations)


@Resolver("Orleans.cantons")
async def resolver_orleans_cantons(parent, args, ctx, info):
    orleans_cantons = await fetch_orleans_cantons(
        redis = ctx["app"]["redis"],
        offset = args.get("offset"),
        limit = args.get("limit"),
    )

    return transform_to_graph(orleans_cantons)


@Resolver("Orleans.towns")
async def resolver_orleans_towns(parent, args, ctx, info):
    orleans_towns = await fetch_orleans_towns(
        redis = ctx["app"]["redis"],
        offset = args.get("offset"),
        limit = args.get("limit"),
    )

    return transform_to_graph(orleans_towns)


@Resolver("Orleans.districts")
async def resolver_orleans_districts(parent, args, ctx, info):
    orleans_districts = await fetch_orleans_districts(
        redis = ctx["app"]["redis"],
        query = args.get("query"),
        offset = args.get("offset"),
        limit = args.get("limit"),
    )

    return transform_to_graph(orleans_districts)
    

@Resolver("Orleans.townHalls")
async def resolver_orleans_townhalls(parent, args, ctx, info):
    orleans_townhalls = await fetch_orleans_townhalls(
        redis = ctx["app"]["redis"],
        query = args.get("query"),
        offset = args.get("offset"),
        limit = args.get("limit"),
    )

    return transform_to_graph(orleans_townhalls)
    

@Resolver("Query.publicEvents")
async def resolver_public_events(parent, args, ctx, info):
    public_events = await fetch_public_events(
        redis = ctx["app"]["redis"],
        query = args.get("query"),
        offset = args.get("offset"),
        limit = args.get("limit"),
    )

    return transform_to_graph(public_events)
