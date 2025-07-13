from pydantic import BaseModel


class SportSchema(BaseModel):
    name: str
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
