import pytest
import asyncio

import app.clients.movie_client as mc
import app.utils.utils_movies_client as uclient

from fastapi.testclient import TestClient
import app.main as main
from app.schemas.movies_schemas import MovieSearchParams, MovieResponse


@pytest.fixture
def dummy_client():
    class Dummy:
        async def get(self, *args, **kwargs):
            class FakeResp:
                status_code = 200
                def raise_for_status(self): pass
                def json(self): return {}
            return FakeResp()
    return Dummy()


@pytest.fixture
def event_loop():
    loop = asyncio.get_event_loop()
    yield loop
    loop.close()


def test_matches_strict_type_and_genre_and_actors():
    movie = MovieResponse(
        id="1", title="Test Movie", year=2020,
        type="movie", genres=["Action"],
        actors=["Alice Smith", "Bob Jones"],
        director=None, runtime=None,
        plot=None, poster_url=None,
        ratings={}, source="TMDB"
    )
    params = MovieSearchParams(
        title=None, genre="Action", actors="Alice", type="movie"
    )
    assert uclient.matches(movie, params) is True


def test_matches_fails_on_type_mismatch():
    movie = MovieResponse(
        id="2", title="Test", year=2021,
        type="series", genres=["Drama"], actors=[],
        director=None, runtime=None,
        plot=None, poster_url=None,
        ratings={}, source="TMDB"
    )
    params = MovieSearchParams(
        title=None, genre=None, actors=None, type="movie"
    )
    assert uclient.matches(movie, params) is False


@pytest.mark.asyncio
async def test_get_search_results_returns_list_and_endpoint(dummy_client):
    async def fake_get(*args, **kwargs):
        class FakeResp:
            status_code = 200
            def raise_for_status(self): pass
            def json(self): return {"results": [{"id": 123, "title": "Foo"}]}
        return FakeResp()

    dummy_client.get = fake_get
    results, endpoint = await uclient.get_search_results(dummy_client, "Foo", is_series=False)
    assert endpoint == "movie"
    assert results[0]["title"] == "Foo"


@pytest.mark.asyncio
async def test_search_by_title_only_calls_map_to_movie(monkeypatch, dummy_client):
    params = MovieSearchParams(
        title="Bar", genre=None, actors=None, type="movie")
    # now restrict to movie only:
    params = MovieSearchParams(
        title="Bar", genre=None, actors=None, type="movie")

    async def fake_get_search_results(client, title, is_series):
        return ([{"id": 1, "title": "Bar"}], "movie")

    async def fake_fetch_genres(client, is_series):
        return {1: "Action"}

    async def fake_map_to_movie(item, endpoint, genres, params, client):
        return MovieResponse(
            id="1", title="Bar", year=2020,
            type="movie", genres=["Action"], actors=[],
            director=None, runtime=None, plot=None,
            poster_url=None, ratings={}, source="TMDB"
        )

    monkeypatch.setattr(mc, "get_search_results", fake_get_search_results)
    monkeypatch.setattr(mc, "fetch_genres",        fake_fetch_genres)
    monkeypatch.setattr(mc, "map_to_movie",        fake_map_to_movie)

    movies = await mc._search_by_title_only(dummy_client, params, is_series=False)
    assert len(movies) == 1
    assert movies[0].title == "Bar"

# --- Unit tests for title + filters branch ------------------------------


@pytest.mark.asyncio
async def test_search_by_title_with_filters_filters_out_nonmatching(monkeypatch, dummy_client):
    params = MovieSearchParams(
        title="Baz", genre="Comedy", actors="Alice", type="movie")

    # discover_by_filters should only be used when actors+series; for movie+actors,
    # code will call get_search_results, then fetch_genres, then map, then matches().
    async def fake_get_search_results(client, title, is_series):
        # imagine two results, one matching Alice, one not
        return (
            [
                {"id": 1, "title": "BazA"},  # will map to actor "Alice"
                {"id": 2, "title": "BazB"},  # no Alice
            ],
            "movie"
        )

    async def fake_fetch_genres(client, is_series):
        return {1: "Comedy"}

    async def fake_map_to_movie(item, endpoint, genres, params, client):
        # for id==1 include Alice, for id==2 include Bob
        actors = ["Alice Smith"] if item["id"] == 1 else ["Bob Jones"]
        return MovieResponse(
            id=str(item["id"]),
            title=item["title"],
            year=2000,
            type="movie",
            genres=["Comedy"],
            actors=actors,
            director=None, runtime=None, plot=None,
            poster_url=None, ratings={}, source="TMDB"
        )

    monkeypatch.setattr(mc, "get_search_results", fake_get_search_results)
    monkeypatch.setattr(mc, "fetch_genres", fake_fetch_genres)
    monkeypatch.setattr(mc, "map_to_movie", fake_map_to_movie)

    movies = await mc._search_by_title_with_filters(dummy_client, params, is_series=False)
    # only the first item should survive matches()
    assert len(movies) == 1
    assert movies[0].id == "1"


# --- Unit tests for filters-only branch ----------------------------------

@pytest.mark.asyncio
async def test_search_by_filters_only_uses_discover_and_filters(monkeypatch, dummy_client):
    params = MovieSearchParams(
        title=None, genre="Drama", actors=None, type="series")

    async def fake_discover_by_filters(client, genre, actors, is_series):
        # return two shows, one with matching genre, one without
        return (
            [
                {"id": 10, "genre_ids": [5]},  # assume genre 5 is Drama
                {"id": 20, "genre_ids": [1]},  # not Drama
            ],
            "tv"
        )

    async def fake_fetch_genres(client, is_series):
        return {5: "Drama", 1: "Comedy"}

    async def fake_map_to_movie(item, endpoint, genres, params, client):
        return MovieResponse(
            id=str(item["id"]), title="X", year=2001,
            type="series", genres=[genres[g] for g in item["genre_ids"]],
            actors=[], director=None, runtime=None, plot=None,
            poster_url=None, ratings={}, source="TMDB"
        )

    monkeypatch.setattr(mc, "discover_by_filters", fake_discover_by_filters)
    monkeypatch.setattr(mc, "fetch_genres",         fake_fetch_genres)
    monkeypatch.setattr(mc, "map_to_movie",         fake_map_to_movie)

    shows = await mc._search_by_filters_only(dummy_client, params, is_series=True)
    assert len(shows) == 1
    assert shows[0].genres == ["Drama"]


# --- Unit tests for popular-fallback branch ------------------------------

@pytest.mark.asyncio
async def test_get_popular_fallback_combines_movies_and_tv(monkeypatch, dummy_client):
    params = MovieSearchParams(title=None, genre=None, actors=None, type=None)

    async def fake_get_popular(client, is_series):
        # return one item per type
        return [{"id": 100}] if not is_series else [{"id": 200}]

    async def fake_fetch_genres(client, is_series):
        return {}

    async def fake_map_to_movie(item, t, genres, params, client):
        return MovieResponse(
            id=str(item["id"]), title=f"Title{item['id']}",
            year=1990, type=("series" if t == "tv" else "movie"),
            genres=[], actors=[], director=None, runtime=None,
            plot=None, poster_url=None, ratings={}, source="TMDB"
        )

    monkeypatch.setattr(mc, "get_popular",     fake_get_popular)
    monkeypatch.setattr(mc, "fetch_genres",    fake_fetch_genres)
    monkeypatch.setattr(mc, "map_to_movie",    fake_map_to_movie)

    top = await mc._get_popular_fallback(dummy_client, params)
    # when no title, result list is sorted alphabetically by title:
    assert [m.title for m in top] == ["Title100", "Title200"]


# --- Integration test against FastAPI endpoint --------------------------

@pytest.fixture
def client(monkeypatch):
    # stub out the `search_tmdb` in tripklik.main
    async def fake_search_tmdb(params):
        return [
            MovieResponse(
                id="42",
                title="Life of Pi",
                year=2012,
                type="movie",
                genres=["Adventure"],
                actors=[],
                director=None,
                runtime=None,
                plot=None,
                poster_url=None,
                ratings={},
                source="TMDB"
            )
        ]

    monkeypatch.setattr(main, "search_tmdb", fake_search_tmdb)
    return TestClient(main.app)


def test_search_movies_endpoint(client):
    resp = client.get("/movies/search", params={"title": "whatever"})
    assert resp.status_code == 200

    data = resp.json()
    # should now be exactly your fake response
    assert isinstance(data, list)
    assert data == [{
        "id": "42",
        "title": "Life of Pi",
        "year": 2012,
        "type": "movie",
        "genres": ["Adventure"],
        "actors": [],
        "director": None,
        "runtime": None,
        "plot": None,
        "poster_url": None,
        "ratings": {},
        "source": "TMDB"
    }]

# --- More unit‐tests for movie_client logic ------------------------------


@pytest.mark.asyncio
async def test_search_by_title_with_filters_for_series_uses_discover(monkeypatch, dummy_client):
    """
    When params.type='series' and params.actors is provided,
    _search_by_title_with_filters must call discover_by_filters(),
    then map & filter via matches().
    """
    # set up series search params
    params = MovieSearchParams(
        title="Show", genre="Sci‑Fi", actors="Jane", type="series")

    # fake discover_by_filters returns two items, only one with 'Jane' in actor list
    async def fake_discover(client, genre, actors, is_series):
        return (
            [
                {"id": 11, "genre_ids": [9]},  # we'll map to include Jane
                {"id": 22, "genre_ids": [9]},  # no Jane
            ],
            "tv"
        )

    async def fake_fetch_genres(client, is_series):
        return {9: "Sci‑Fi"}

    async def fake_map(item, endpoint, genres, params_in, client):
        # id=11 gets Jane, id=22 gets Bob
        actor_list = ["Jane Doe"] if item["id"] == 11 else ["Bob Smith"]
        return MovieResponse(
            id=str(item["id"]),
            title=f"Show{item['id']}",
            year=2021,
            type="series",
            genres=["Sci‑Fi"],
            actors=actor_list,
            director=None, runtime=None, plot=None,
            poster_url=None, ratings={}, source="TMDB"
        )

    monkeypatch.setattr(mc, "discover_by_filters", fake_discover)
    monkeypatch.setattr(mc, "fetch_genres",          fake_fetch_genres)
    monkeypatch.setattr(mc, "map_to_movie",          fake_map)

    out = await mc._search_by_title_with_filters(dummy_client, params, is_series=True)
    # only the item with Jane should survive matches()
    assert len(out) == 1
    assert out[0].id == "11"


@pytest.mark.asyncio
async def test_search_by_filters_only_for_movie(monkeypatch, dummy_client):
    """
    When there is no title but there are filters, _search_by_filters_only
    must call discover_by_filters() once.
    """
    params = MovieSearchParams(
        title=None, genre="Horror", actors=None, type="movie")

    async def fake_discover(client, genre, actors, is_series):
        return ([{"id": 5, "genre_ids": [7]}], "movie")

    async def fake_fetch_genres(client, is_series):
        return {7: "Horror"}

    async def fake_map(item, endpoint, genres, params_in, client):
        return MovieResponse(
            id=str(item["id"]),
            title="Spooky",
            year=1980,
            type="movie",
            genres=["Horror"],
            actors=[],
            director=None, runtime=None, plot=None,
            poster_url=None, ratings={}, source="TMDB"
        )

    monkeypatch.setattr(mc, "discover_by_filters", fake_discover)
    monkeypatch.setattr(mc, "fetch_genres",          fake_fetch_genres)
    monkeypatch.setattr(mc, "map_to_movie",          fake_map)

    out = await mc._search_by_filters_only(dummy_client, params, is_series=False)
    assert len(out) == 1
    assert out[0].title == "Spooky"


@pytest.mark.asyncio
async def test_search_tmdb_with_title_but_no_type_uses_title_only_branch(monkeypatch, dummy_client):
    """
    If the user supplies only a title (type=None), search_tmdb()
    should still call _search_by_title_only under the hood.
    """
    from app.clients import movie_client as mc
    from app.schemas.movies_schemas import MovieSearchParams

    params = MovieSearchParams(
        title="Just A Title", genre=None, actors=None, type=None)

    called = {}

    async def fake_by_title(client, p, is_series):
        called['branch'] = '_search_by_title_only'
        return []
    monkeypatch.setattr(mc, "_search_by_title_only",     fake_by_title)
    monkeypatch.setattr(mc, "_search_by_title_with_filters", lambda *a,
                        **k: (_ for _ in ()).throw(AssertionError("wrong branch")))
    monkeypatch.setattr(mc, "_search_by_filters_only", lambda *a,
                        **k: (_ for _ in ()).throw(AssertionError("wrong branch")))
    monkeypatch.setattr(mc, "_get_popular_fallback", lambda *a,
                        **k: (_ for _ in ()).throw(AssertionError("wrong branch")))

    await mc.search_tmdb(params)
    assert called.get('branch') == '_search_by_title_only'

# --- Error‐handling and validation integration tests ----------------------


def test_search_endpoint_invalid_type_param_returns_422():
    """
    Pydantic should reject any type not in ('movie','series').
    """
    client = TestClient(main.app)
    resp = client.get("/movies/search", params={"type": "not-a-type"})
    assert resp.status_code == 422
    # response JSON has 'detail' explaining the validation error
    assert any(err["loc"] == ["query", "type"]
               for err in resp.json()["detail"])


def test_search_endpoint_third_party_error_is_502(monkeypatch):
    """
    If search_tmdb raises, the endpoint should return 502 with our ErrorResponse.
    """
    async def boom(params):
        raise RuntimeError("TMDB is down")
    # patch the name that main actually calls
    monkeypatch.setattr(main, "search_tmdb", boom)

    client = TestClient(main.app)
    resp = client.get("/movies/search", params={})
    assert resp.status_code == 502
    body = resp.json()
    assert "TMDB service error" in body["detail"]
