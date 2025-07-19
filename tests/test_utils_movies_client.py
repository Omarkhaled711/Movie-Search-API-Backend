import pytest
import json
from app.utils.utils_movies_client import (
    fetch_genres,
    _discover_movie_by_filters,
    _discover_tv_by_filters,
    map_to_movie,
    get_search_results,
)
from app.schemas.movies_schemas import MovieSearchParams
from httpx import HTTPStatusError, Response


class DummyClient:
    def __init__(self, responses):
        # responses: dict of url to FakeResp
        self.responses = responses

    async def get(self, url, params=None):
        return self.responses.get(url)


@pytest.fixture
def dummy_client():
    # a basic dummy_client that returns empty JSON 200
    class Dummy:
        async def get(self, *args, **kwargs):
            class FakeResp:
                status_code = 200
                def raise_for_status(self): pass
                def json(self): return {}
            return FakeResp()
    return Dummy()


# --- fetch_genres tests ---


@pytest.mark.asyncio
async def test_fetch_genres_cache_hit(monkeypatch):
    # simulate redis cache hit
    from app.utils import utils_movies_client as uclient

    cached = json.dumps({"10": "Horror", "20": "Comedy"})

    class FakeRedis:
        async def get(self, key):
            return cached

        async def set(self, key, value, ex=None):
            raise AssertionError("set should not be called on cache hit")
    monkeypatch.setattr(uclient, "_redis", FakeRedis())

    # client.get should not be called
    dummy = DummyClient({})
    genres = await fetch_genres(dummy, is_series=False)
    assert genres == {10: "Horror", 20: "Comedy"}


@pytest.mark.asyncio
async def test_fetch_genres_cache_miss(monkeypatch):
    from app.utils import utils_movies_client as uclient
    # simulate cache miss

    class FakeRedis:
        async def get(self, key):
            return None

        async def set(self, key, value, ex=None):
            # record the mapping written
            # OM keys are strings
            assert json.loads(value) == {"1": "Action"}
    monkeypatch.setattr(uclient, "_redis", FakeRedis())

    # fake TMDB response
    class FakeResp:
        status_code = 200
        def raise_for_status(self): pass
        def json(self): return {"genres": [{"id": 1, "name": "Action"}]}
    # patch client.get
    dummy = DummyClient({f"{uclient.BASE_URL}/genre/movie/list": FakeResp()})
    genres = await fetch_genres(dummy, is_series=False)
    assert genres == {1: "Action"}

# --- error propagation ---


@pytest.mark.asyncio
async def test_get_search_results_http_error(dummy_client):
    # stub to raise status
    class BadResp:
        status_code = 500

        def raise_for_status(self):
            raise HTTPStatusError("Error", request=None, response=None)

        def json(self): return {}

    async def fake_get(*args, **kwargs):
        return BadResp()
    client = DummyClient({})
    client.get = fake_get
    with pytest.raises(HTTPStatusError):
        await get_search_results(client, "x", is_series=False)

# --- discover_by_filters: movie branch ---


@pytest.mark.asyncio
async def test_discover_movie_by_filters_genre_only(monkeypatch):
    # stub fetch_genres
    from app.utils import utils_movies_client as uclient

    async def fake_fetch_genres(client, is_series):
        return {5: "Drama"}
    monkeypatch.setattr(uclient, "fetch_genres", fake_fetch_genres)

    # fake client.get for discover
    class FakeResp:
        status_code = 200
        def raise_for_status(self): pass
        def json(self): return {"results": [{"id": 7}]}
    dummy = DummyClient({f"{uclient.BASE_URL}/discover/movie": FakeResp()})
    results, endpoint = await _discover_movie_by_filters(dummy, genre="Drama", actors=None)
    assert endpoint == "movie"
    assert isinstance(results, list) and results[0]["id"] == 7


@pytest.mark.asyncio
async def test_discover_movie_by_filters_actors_only(monkeypatch):
    from app.utils import utils_movies_client as uclient
    # stub person search

    class PersonResp:
        status_code = 200
        def raise_for_status(self): pass
        def json(self): return {"results": [{"id": 42}]}

    class DiscResp:
        status_code = 200
        def raise_for_status(self): pass
        def json(self): return {"results": [{"id": 8}]}
    dummy = DummyClient({
        f"{uclient.BASE_URL}/search/person": PersonResp(),
        f"{uclient.BASE_URL}/discover/movie": DiscResp()
    })
    res, ep = await _discover_movie_by_filters(dummy, genre=None, actors="Someone")
    assert ep == "movie"
    assert res[0]["id"] == 8

# --- discover_by_filters: series branch ---


@pytest.mark.asyncio
async def test_discover_tv_by_filters_genre_only(monkeypatch):
    from app.utils import utils_movies_client as uclient
    # stub genre fetcher

    async def fake_fetch_genres(client, is_series):
        return {99: "Documentary"}
    monkeypatch.setattr(uclient, "fetch_genres", fake_fetch_genres)

    # stub TV discover endpoint
    class FakeResp:
        status_code = 200
        def raise_for_status(self): pass
        def json(self): return {"results": [{"id": 77}]}

    dummy = DummyClient({f"{uclient.BASE_URL}/discover/tv": FakeResp()})
    res, ep = await _discover_tv_by_filters(dummy, genre="Documentary", actors=None)
    assert ep == "tv"
    assert res[0]["id"] == 77


@pytest.mark.asyncio
async def test_discover_tv_by_filters_actors_only(monkeypatch):
    from app.utils import utils_movies_client as uclient

    class PersonResp:
        status_code = 200
        def raise_for_status(self): pass
        def json(self): return {"results": [{"id": 314}]}

    class CreditResp:
        status_code = 200
        def raise_for_status(self): pass
        def json(self): return {"cast": [{"id": 271}]}  # mocked TV show result

    dummy = DummyClient({
        f"{uclient.BASE_URL}/search/person": PersonResp(),
        f"{uclient.BASE_URL}/person/314/tv_credits": CreditResp()
    })

    results, endpoint = await _discover_tv_by_filters(dummy, genre=None, actors="Some Actor")
    assert endpoint == "tv"
    assert results[0]["id"] == 271


@pytest.mark.asyncio
async def test_discover_tv_by_filters_genre_and_actors(monkeypatch):
    from app.utils import utils_movies_client as uclient

    # Stub fetch_genres to map ID -> name
    async def fake_fetch_genres(client, is_series):
        return {99: "Documentary", 55: "Sci-Fi"}
    monkeypatch.setattr(uclient, "fetch_genres", fake_fetch_genres)

    # Stub person search
    class PersonResp:
        status_code = 200
        def raise_for_status(self): pass
        def json(self): return {"results": [{"id": 1234}]}

    # Stub tv_credits
    class CreditsResp:
        status_code = 200
        def raise_for_status(self): pass

        def json(self): return {
            "cast": [
                {"id": 7, "genre_ids": [99]},
                {"id": 9, "genre_ids": [123]},
            ]
        }

    dummy = DummyClient({
        f"{uclient.BASE_URL}/search/person": PersonResp(),
        f"{uclient.BASE_URL}/person/1234/tv_credits": CreditsResp()
    })

    results, endpoint = await uclient._discover_tv_by_filters(
        dummy,
        genre="Documentary",
        actors="Test Actor"
    )

    assert endpoint == "tv"
    assert isinstance(results, list)
    assert len(results) == 1
    assert results[0]["id"] == 7


# --- map_to_movie tests ---


@pytest.mark.asyncio
async def test_map_to_movie_no_imdb(monkeypatch, dummy_client):
    # stub credits and imdb lookup so no imdb
    from app.utils import utils_movies_client as uclient
    async def fake_credits(c, t, i): return []
    async def fake_get_imdb(c, t, i): return None
    monkeypatch.setattr(uclient, "_fetch_credits", fake_credits)
    monkeypatch.setattr(uclient, "_get_imdb_id", fake_get_imdb)
    # prepare a TMDB item without poster_path
    item = {"id": 100, "title": "X",
            "release_date": "2000-01-01", "genre_ids": [], }
    m = await map_to_movie(item, "movie", {}, MovieSearchParams(), dummy_client)
    assert m.source == "TMDB"
    assert m.id == str(100)


@pytest.mark.asyncio
async def test_map_to_movie_with_omdb(monkeypatch, dummy_client):
    from app.utils import utils_movies_client as uclient
    # stub credits
    async def fake_credits(c, t, i): return ["Actor A"]
    async def fake_get_imdb(c, t, i): return "tt123"

    async def fake_omdb(c, i):
        return {
            "Response": "True",
            "Director": "Dir Name",
            "Runtime": "120 min",
            "Plot": "Plot here",
            "Poster": "OMDBPOSTER",
            "Ratings": [{"Source": "SourceA", "Value": "9/10"}],
            "Year": "1999",
            "Title": "Override Title"
        }
    monkeypatch.setattr(uclient, "_fetch_credits", fake_credits)
    monkeypatch.setattr(uclient, "_get_imdb_id", fake_get_imdb)
    monkeypatch.setattr(uclient, "_fetch_omdb_data", fake_omdb)
    # item with poster_path
    item = {"id": 101, "title": None, "first_air_date": "",
            "genre_ids": [], "poster_path": "/p.jpg"}
    m = await map_to_movie(item, "movie", {}, MovieSearchParams(), dummy_client)
    assert m.source == "Merged"
    assert m.title == "Override Title"
    assert m.director == "Dir Name"
    assert m.runtime == "120 min"
    assert m.plot == "Plot here"
    assert m.ratings == {"SourceA": "9/10"}
    # TMDB poster takes precedence
    assert m.poster_url.endswith("/p.jpg")
