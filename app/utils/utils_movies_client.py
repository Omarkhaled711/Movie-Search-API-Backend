import json
import httpx
import redis.asyncio as redis
from typing import Dict, List, Optional, Tuple
from ..config import settings
from ..schemas.movies_schemas import MovieResponse, MovieSearchParams

# Redis client
_redis = redis.from_url(
    settings.REDIS_URL, encoding="utf-8", decode_responses=True)
TMDB_API_KEY = settings.TMDB_API_KEY
OMDB_API_KEY = settings.OMDB_API_KEY
BASE_URL = 'https://api.themoviedb.org/3'
OMDB_BASE_URL = 'http://www.omdbapi.com/'

CACHE_TTL_POPULAR = 600    # 10 minutes
CACHE_TTL_OMDB = 3600      # 1 hour
CACHE_TTL_SEARCH = 3600    # 1 hour


async def fetch_genres(
    client: httpx.AsyncClient,
    is_series: bool
) -> Dict[int, str]:
    """
    Fetch and cache TMDB genres mapping for movies or TV series.

    :param client: HTTP client for making API requests.
    :param is_series: Boolean indicating if genres are for TV series (True) or movies (False).
    :return: Dictionary mapping genre IDs to their names.
    """
    endpoint = 'tv' if is_series else 'movie'
    key = f"genres:{endpoint}"
    cached = await _redis.get(key)
    if cached:
        return {int(k): v for k, v in json.loads(cached).items()}

    resp = await client.get(
        f"{BASE_URL}/genre/{endpoint}/list",
        params={'api_key': TMDB_API_KEY}
    )
    resp.raise_for_status()
    genres = resp.json().get('genres', [])
    mapping = {g['id']: g['name'] for g in genres}
    await _redis.set(key, json.dumps(mapping))
    return mapping


async def get_search_results(
    client: httpx.AsyncClient,
    title: str,
    is_series: bool
) -> Tuple[List[dict], str]:
    """
    Search TMDB for movies or TV series by title.

    :param client: HTTP client for making API requests.
    :param title: Title to search for.
    :param is_series: Boolean indicating if searching for TV series (True) or movies (False).
    :return: Tuple containing the list of search results and the endpoint used.
    """
    endpoint = 'tv' if is_series else 'movie'
    resp = await client.get(
        f"{BASE_URL}/search/{endpoint}",
        params={'api_key': TMDB_API_KEY, 'query': title, 'page': 1}
    )
    resp.raise_for_status()
    return resp.json().get('results', []), endpoint


async def discover_by_filters(
    client: httpx.AsyncClient,
    genre: Optional[str],
    actors: Optional[str],
    is_series: bool
) -> Tuple[List[dict], str]:
    """
    Discover movies or TV shows based on genre and/or actors.

    :param client: HTTP client for making API requests.
    :param genre: Optional genre name to filter by.
    :param actors: Optional actor name to filter by.
    :param is_series: Boolean indicating if discovering TV series (True) or movies (False).
    :return: Tuple containing the list of discovered items and the endpoint used.
    """
    if is_series:
        return await _discover_tv_by_filters(client, genre, actors)
    else:
        return await _discover_movie_by_filters(client, genre, actors)


async def _discover_movie_by_filters(
    client: httpx.AsyncClient,
    genre: Optional[str],
    actors: Optional[str]
) -> Tuple[List[dict], str]:
    """
    Discover movies by genre and/or actors using the /discover/movie endpoint.

    :param client: HTTP client for making API requests.
    :param genre: Optional genre name to filter by.
    :param actors: Optional actor name to filter by.
    :return: Tuple containing the list of discovered movies and the endpoint.
    """
    endpoint = 'movie'
    query = {'api_key': TMDB_API_KEY, 'page': 1}

    if genre:
        genres = await fetch_genres(client, False)
        gid = next(
            (str(i) for i, n in genres.items() if n.lower() == genre.lower()), None
        )
        if gid:
            query['with_genres'] = gid

    if actors:
        p = await client.get(
            f"{BASE_URL}/search/person",
            params={'api_key': TMDB_API_KEY, 'query': actors, 'page': 1}
        )
        p.raise_for_status()
        people = p.json().get('results', [])
        if people:
            query['with_cast'] = str(people[0].get('id'))

    resp = await client.get(f"{BASE_URL}/discover/{endpoint}", params=query)
    resp.raise_for_status()
    return resp.json().get('results', []), endpoint


async def _discover_tv_by_filters(
    client: httpx.AsyncClient,
    genre: Optional[str],
    actors: Optional[str]
) -> Tuple[List[dict], str]:
    """
    Discover TV shows by genre and/or actors.
    If actors are provided, use /person/{person_id}/tv_credits.
    If only genre is provided, use /discover/tv with with_genres.

    :param client: HTTP client for making API requests.
    :param genre: Optional genre name to filter by.
    :param actors: Optional actor name to filter by.
    :return: Tuple containing the list of discovered TV shows and the endpoint.
    """
    endpoint = 'tv'

    if actors:
        # Find the person ID
        p = await client.get(
            f"{BASE_URL}/search/person",
            params={'api_key': TMDB_API_KEY, 'query': actors, 'page': 1}
        )
        p.raise_for_status()
        people = p.json().get('results', [])
        if not people:
            return [], endpoint

        person_id = people[0].get('id')
        # Get TV credits for the person
        credits_resp = await client.get(
            f"{BASE_URL}/person/{person_id}/tv_credits",
            params={'api_key': TMDB_API_KEY}
        )
        credits_resp.raise_for_status()
        credits = credits_resp.json().get('cast', [])

        if genre:
            # Filter credits by genre
            genres = await fetch_genres(client, True)
            genre_id = next((i for i, n in genres.items()
                            if n.lower() == genre.lower()), None)

            if genre_id:
                credits = [
                    show for show in credits if genre_id in show.get('genre_ids', [])]

        return credits, endpoint

    else:
        # No actors, use /discover/tv with genre if provided
        query = {'api_key': TMDB_API_KEY, 'page': 1}
        if genre:
            genres = await fetch_genres(client, True)
            gid = next(
                (str(i) for i, n in genres.items()
                 if n.lower() == genre.lower()), None
            )
            if gid:
                query['with_genres'] = gid

        resp = await client.get(f"{BASE_URL}/discover/{endpoint}", params=query)
        resp.raise_for_status()
        return resp.json().get('results', []), endpoint


async def get_popular(
    client: httpx.AsyncClient,
    is_series: bool
) -> List[dict]:
    """
    Get popular movies or TV series from TMDB, with caching.

    :param client: HTTP client for making API requests.
    :param is_series: Boolean indicating if fetching popular TV series (True) or movies (False).
    :return: List of popular items.
    """
    endpoint = 'tv' if is_series else 'movie'
    key = f"popular:{endpoint}"
    cached = await _redis.get(key)
    if cached:
        return json.loads(cached)

    resp = await client.get(
        f"{BASE_URL}/{endpoint}/popular",
        params={'api_key': TMDB_API_KEY, 'page': 1}
    )
    resp.raise_for_status()
    items = resp.json().get('results', [])
    await _redis.set(key, json.dumps(items), ex=CACHE_TTL_POPULAR)
    return items


async def _fetch_credits(
    client: httpx.AsyncClient,
    media_type: str,
    tmdb_id: int
) -> List[str]:
    """
    Fetch the cast (actors) for a movie or TV series from TMDB.

    :param client: HTTP client for making API requests.
    :param media_type: 'movie' or 'tv'.
    :param tmdb_id: TMDB ID of the movie or TV series.
    :return: List of actor names.
    """
    resp = await client.get(
        f"{BASE_URL}/{media_type}/{tmdb_id}/credits",
        params={'api_key': TMDB_API_KEY}
    )
    resp.raise_for_status()
    cast = resp.json().get('cast', [])
    return [c.get('name') for c in cast if c.get('name')]


async def _get_imdb_id(
    client: httpx.AsyncClient,
    media_type: str,
    tmdb_id: int
) -> Optional[str]:
    """
    Get the IMDB ID for a movie or TV series from TMDB.

    :param client: HTTP client for making API requests.
    :param media_type: 'movie' or 'tv'.
    :param tmdb_id: TMDB ID of the movie or TV series.
    :return: IMDB ID if available, else None.
    """
    resp = await client.get(
        f"{BASE_URL}/{media_type}/{tmdb_id}",
        params={'api_key': TMDB_API_KEY}
    )
    if resp.status_code == 200:
        return resp.json().get('imdb_id')
    return None


async def _fetch_omdb_data(
    client: httpx.AsyncClient,
    imdb_id: str
) -> Optional[dict]:
    """
    Fetch movie or TV series data from OMDB using IMDB ID.

    :param client: HTTP client for making API requests.
    :param imdb_id: IMDB ID of the movie or TV series.
    :return: Dictionary of OMDB data if successful, else None.
    """
    resp = await client.get(
        OMDB_BASE_URL, params={'apikey': OMDB_API_KEY, 'i': imdb_id}
    )
    data = resp.json()
    if resp.status_code == 200 and data.get('Response') == 'True':
        return data
    return None


async def map_to_movie(
    item: dict,
    media_type: str,
    genres: Dict[int, str],
    params: MovieSearchParams,
    client: httpx.AsyncClient
) -> MovieResponse:
    """
    Map TMDB item data to a MovieResponse object, enriching with OMDB data if available.

    :param item: Dictionary containing TMDB item data.
    :param media_type: 'movie' or 'tv'.
    :param genres: Dictionary mapping genre IDs to names.
    :param params: MovieSearchParams object for additional context.
    :param client: HTTP client for making API requests.
    :return: MovieResponse object.
    """
    tmdb_id = item.get('id')
    title = item.get('title') or item.get('name') or ''
    date = item.get('release_date') or item.get('first_air_date') or ''
    year = int(date.split('-')[0]) if date else None

    genre_list = [genres.get(g)
                  for g in item.get('genre_ids', []) if genres.get(g)]
    actors = await _fetch_credits(client, media_type, tmdb_id)

    imdb_id = await _get_imdb_id(client, media_type, tmdb_id)
    omdb = await _fetch_omdb_data(client, imdb_id) if imdb_id else None

    if omdb:
        director = omdb.get('Director')
        runtime = omdb.get('Runtime')
        plot = omdb.get('Plot')
        poster = (
            f"https://image.tmdb.org/t/p/w500{item['poster_path']}"
            if item.get('poster_path') else omdb.get('Poster')
        )
        ratings = {r['Source']: r['Value'] for r in omdb.get('Ratings', [])}
        source = 'Merged'
        title = title or omdb.get('Title')
        year = year or (int(omdb.get('Year')) if omdb.get('Year') else None)
    else:
        director = None
        runtime = None
        plot = None
        poster = (
            f"https://image.tmdb.org/t/p/w500{item['poster_path']}"
            if item.get('poster_path') else None
        )
        ratings = {}
        source = 'TMDB'

    return MovieResponse(
        id=imdb_id or str(tmdb_id),
        title=title,
        year=year,
        type=params.type or ('series' if media_type == 'tv' else 'movie'),
        genres=genre_list,
        actors=actors,
        director=director,
        runtime=runtime,
        plot=plot,
        poster_url=poster,
        ratings=ratings,
        source=source
    )


def matches(
    movie: MovieResponse,
    params: MovieSearchParams
) -> bool:
    """
    Check if a movie matches the given search parameters.

    :param movie: MovieResponse object to check.
    :param params: MovieSearchParams object containing search criteria.
    :return: True if the movie matches all criteria, else False.
    """
    if params.type and movie.type != params.type:
        return False
    if params.genre and not any(g.lower() == params.genre.lower() for g in movie.genres):
        return False
    if params.actors and not any(params.actors.lower() in a.lower() for a in movie.actors):
        return False
    return True
