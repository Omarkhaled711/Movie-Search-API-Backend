from typing import List, Optional, Literal, Dict
from pydantic import BaseModel


class MovieSearchParams(BaseModel):
    title: Optional[str] = None
    actors: Optional[str] = None
    type: Optional[Literal['movie', 'series']] = None
    genre: Optional[str] = None


class MovieResponse(BaseModel):
    id: str
    title: str
    year: Optional[int]
    type: Literal['movie', 'series']
    genres: List[str]
    actors: List[str]
    director: Optional[str]
    runtime: Optional[str]
    plot: Optional[str]
    poster_url: Optional[str]
    ratings: Optional[Dict[str, str]]
    source: Literal['TMDB', 'Merged']


class ErrorResponse(BaseModel):
    code: int
    message: str
