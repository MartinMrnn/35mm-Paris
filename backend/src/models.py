"""
Data models for 35mm Paris.
Simple Pydantic models for data validation.
"""
from typing import Optional, List, Union
from pydantic import BaseModel, Field, validator


class MovieData(BaseModel):
    """Movie data from Allocine API."""
    title: str
    originalTitle: Optional[str] = Field(None, alias="originalTitle")
    synopsis: Optional[str] = Field(None, alias="synopsisFull")
    poster_url: Optional[str] = Field(None, alias="urlPoster")
    runtime: Optional[str] = None
    director: Optional[str] = None
    languages: Optional[List[Union[dict, str]]] = None
    has_dvd_release: bool = Field(False, alias="hasDvdRelease")
    is_premiere: bool = Field(False, alias="isPremiere")
    weekly_outing: bool = Field(False, alias="weeklyOuting")
    
    class Config:
        populate_by_name = True
    
    @validator('title')
    def title_not_empty(cls, v):
        if not v or not v.strip():
            raise ValueError('Title cannot be empty')
        return v.strip()
    
    @validator('originalTitle', pre=True)
    def original_title_default(cls, v, values):
        """If no original title, use the main title."""
        if not v:
            return values.get('title', '')
        return v


class Director(BaseModel):
    """Director information."""
    first_name: str
    last_name: str
    
    @validator('first_name', 'last_name')
    def names_not_empty(cls, v):
        if not v or not v.strip():
            raise ValueError('Name cannot be empty')
        return v.strip()


class Language(BaseModel):
    """Language information."""
    code: str
    label: Optional[str] = None
    
    @validator('label', pre=True, always=True)
    def label_default(cls, v, values):
        """If no label, use code as label."""
        return v or values.get('code', '')


class Cinema(BaseModel):
    """Cinema information."""
    id: str
    name: str
    address: Optional[str] = None
    city: Optional[str] = None
    zipcode: Optional[str] = None


class Screening(BaseModel):
    """Movie screening information."""
    movie_id: int
    cinema_id: str
    date: str  # Format: YYYY-MM-DD
    starts_at: Optional[str] = None  # Format: HH:MM
    version: Optional[str] = Field(None, alias="diffusion_version")