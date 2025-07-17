"""
Data models for 35mm Paris.
Simple Pydantic models for data validation.
"""

from pydantic import BaseModel, ConfigDict, Field, field_validator


class MovieData(BaseModel):
    """Movie data from Allocine API."""

    model_config = ConfigDict(populate_by_name=True)

    title: str
    originalTitle: str | None = Field(None, alias="originalTitle")
    synopsis: str | None = Field(None, alias="synopsisFull")
    poster_url: str | None = Field(None, alias="urlPoster")
    runtime: str | None = None
    director: str | None = None
    languages: list[dict | str] | None = None
    has_dvd_release: bool = Field(False, alias="hasDvdRelease")
    is_premiere: bool = Field(False, alias="isPremiere")
    weekly_outing: bool = Field(False, alias="weeklyOuting")

    @field_validator("title")
    @classmethod
    def title_not_empty(cls, v):
        if not v or not v.strip():
            raise ValueError("Title cannot be empty")
        return v.strip()

    @field_validator("originalTitle", mode="before")
    @classmethod
    def original_title_default(cls, v, info):
        """If no original title, use the main title."""
        if not v:
            return info.data.get("title", "")
        return v


class Director(BaseModel):
    """Director information."""

    first_name: str
    last_name: str

    @field_validator("first_name", "last_name")
    @classmethod
    def names_not_empty(cls, v):
        if not v or not v.strip():
            raise ValueError("Name cannot be empty")
        return v.strip()


class Language(BaseModel):
    """Language information."""

    code: str
    label: str | None = None

    @field_validator("label", mode="before")
    @classmethod
    def label_default(cls, v, info):
        """If no label, use code as label."""
        return v or info.data.get("code", "")


class Cinema(BaseModel):
    """Cinema information."""

    id: str
    name: str
    address: str | None = None
    city: str | None = None
    zipcode: str | None = None


class Screening(BaseModel):
    """Movie screening information."""

    movie_id: int
    cinema_id: str
    date: str  # Format: YYYY-MM-DD
    starts_at: str | None = None  # Format: HH:MM
    version: str | None = Field(None, alias="diffusion_version")
