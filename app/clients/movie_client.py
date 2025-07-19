import asyncio
from typing import List, Tuple
import httpx
from ..schemas.movies_schemas import MovieResponse, MovieSearchParams
from ..utils.utils_movies_client import (
    fetch_genres,
    get_search_results,
    discover_by_filters,
    get_popular,
    map_to_movie,
    matches,
)


async def search_tmdb(params: MovieSearchParams) -> List[MovieResponse]:
    """
    Search for movies or TV series based on the provided parameters
    This function acts as a wrapper to handle different search scenarios:
    - Title-only search
    - Title with filters (genre, actors)
    - Filters-only search
    - Popular fallback when no specific criteria are provided

    :param params: MovieSearchParams object containing search criteria
    :return: List of MovieResponse objects matching the search criteria
    """
    async with httpx.AsyncClient(timeout=10.0) as client:
        is_series = params.type == 'series'
        has_title = bool(params.title)
        has_filters = any([params.genre, params.actors, params.type])

        if has_title and not has_filters:
            return await _search_by_title_only(client, params, is_series)
        elif has_title and has_filters:
            return await _search_by_title_with_filters(client, params, is_series)
        elif has_filters:
            return await _search_by_filters_only(client, params, is_series)
        else:
            return await _get_popular_fallback(client, params)


async def _search_by_title_only(
    client: httpx.AsyncClient,
    params: MovieSearchParams,
    is_series: bool
) -> List[MovieResponse]:
    """
    Perform a title-only search for movies or TV series.
    If no type is specified, search both movies and TV series

    :param client: HTTP client for making API requests
    :param params: MovieSearchParams object containing the title.
    :param is_series: Boolean indicating if the search is for series.
    :return: List of MovieResponse objects matching the title.
    """
    endpoints = ['tv', 'movie'] if not params.type else [
        'tv'] if is_series else ['movie']
    movies: List[MovieResponse] = []
    for ep in endpoints:
        results, endpoint = await get_search_results(
            client, params.title, ep == 'tv'
        )
        genres = await fetch_genres(client, ep == 'tv')
        movies += await asyncio.gather(*[
            map_to_movie(item, endpoint, genres, params, client)
            for item in results
        ])
    return movies


async def _search_by_title_with_filters(
    client: httpx.AsyncClient,
    params: MovieSearchParams,
    is_series: bool
) -> List[MovieResponse]:
    """
    Perform a search by title with additional filters (genre, actors)
    Use the approprite endpoints depending on wehter the type is movie or series

    :param client: HTTP client for making API requests
    :param params: MovieSearchParams object containing title and filters
    :param is_series: Boolean indicating if the search is for series
    :return: List of MovieResponse objects matching the criteria
    """
    if params.actors and is_series:
        data, endpoint = await discover_by_filters(
            client, params.genre, params.actors, is_series
        )
    else:
        data, endpoint = await get_search_results(
            client, params.title, is_series
        )
    genres = await fetch_genres(client, is_series)
    mapped = await asyncio.gather(*[
        map_to_movie(item, endpoint, genres, params, client)
        for item in data
    ])
    return [m for m in mapped if matches(m, params)]


async def _search_by_filters_only(
    client: httpx.AsyncClient,
    params: MovieSearchParams,
    is_series: bool
) -> List[MovieResponse]:
    """
    Perform a search using only filters (genre, actors) without a title

    :param client: HTTP client for making API requests
    :param params: MovieSearchParams object containing filters.
    :param is_series: Boolean indicating if the search is for series
    :return: List of MovieResponse objects matching the filters.
    """
    data, endpoint = await discover_by_filters(
        client, params.genre, params.actors, is_series
    )
    genres = await fetch_genres(client, is_series)
    mapped = await asyncio.gather(*[
        map_to_movie(item, endpoint, genres, params, client)
        for item in data
    ])
    return [m for m in mapped if matches(m, params)]


async def _get_popular_fallback(
    client: httpx.AsyncClient,
    params: MovieSearchParams
) -> List[MovieResponse]:
    """
    Retrieve popular movies and TV series as a fallback when no specific search
    criteria are provided.

    :param client: HTTP client for making API requests.
    :param params: MovieSearchParams object (unused in this function)
    :return: List of up to 20 popular MovieResponse objects
    """
    movie_pop, tv_pop = await asyncio.gather(
        get_popular(client, False),
        get_popular(client, True)
    )
    raw: List[Tuple[dict, str]] = [
        (i, 'movie') for i in movie_pop
    ] + [
        (i, 'tv') for i in tv_pop
    ]
    genres_map = {
        'movie': await fetch_genres(client, False),
        'tv': await fetch_genres(client, True)
    }
    results = await asyncio.gather(*[
        map_to_movie(item, t, genres_map[t], params, client)
        for item, t in raw
    ])
    if not params.title:
        results.sort(key=lambda m: m.title)
    return results[:20]
