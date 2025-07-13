from pydantic import BaseModel


class SportSchema(BaseModel):
    name: str
    id: int


class CategorySchema(BaseModel):
    name: str
    slug: str
    sport: SportSchema
    id: int


class TournamentSchema(BaseModel):
    id: int
    name: str
    slug: str
    category: CategorySchema


class TournamentData(BaseModel):
    tournament: TournamentSchema
