from typing import List

from pydantic import BaseModel

##############################
# tournament
##############################


class SportSchema(BaseModel):
    name: str
    slug: str
    id: int


class CategorySchema(BaseModel):
    name: str
    id: int
    slug: str
    sport: SportSchema


class TournamentSchema(BaseModel):
    id: int
    name: str
    slug: str
    category: CategorySchema


class TournamentData(BaseModel):
    tournament: TournamentSchema


##############################
# seasons
##############################


class SeasonSchema(BaseModel):
    name: str
    id: int
    year: str


class SeasonsListSchema(BaseModel):
    seasons: List[SeasonSchema]
