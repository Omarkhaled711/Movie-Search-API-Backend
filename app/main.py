from fastapi import FastAPI, HTTPException, Depends
from .schemas.movies_schemas import MovieSearchParams, MovieResponse, ErrorResponse
from .clients.movie_client import search_tmdb
from typing import List

app = FastAPI()


@app.get('/movies/search', response_model=List[MovieResponse], responses={502: {'model': ErrorResponse}})
async def search_movies(params: MovieSearchParams = Depends()):
    try:
        movies = await search_tmdb(params)
    except Exception as e:
        raise HTTPException(
            status_code=502, detail=f"TMDB service error: {str(e)}")
    return movies
