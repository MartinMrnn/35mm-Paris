"""
Tests for database insertion logic.
"""
import pytest
from datetime import datetime
from unittest.mock import Mock, patch, MagicMock

from db.insert_logic import (
    parse_runtime,
    generate_movie_id,
    generate_director_id,
    _parse_directors,
    _parse_languages,
    cinema_id_to_int,
    movie_exists,
    insert_movie,
    insert_screening
)
from models import MovieData, Director, Language


class TestParseRuntime:
    """Test runtime parsing functionality."""
    
    def test_parse_runtime_hours_and_minutes(self):
        assert parse_runtime("1h 30min") == 90
        assert parse_runtime("2h 45min") == 165
    
    def test_parse_runtime_only_hours(self):
        assert parse_runtime("2h") == 120
        assert parse_runtime("3h") == 180
    
    def test_parse_runtime_only_minutes(self):
        assert parse_runtime("45min") == 45
        assert parse_runtime("90min") == 90
    
    def test_parse_runtime_invalid(self):
        assert parse_runtime(None) == 0
        assert parse_runtime("") == 0
        assert parse_runtime("invalid") == 0
    
    def test_parse_runtime_already_int(self):
        assert parse_runtime(120) == 120


class TestGenerateIds:
    """Test ID generation functions."""
    
    def test_generate_movie_id_consistent(self):
        """Same inputs should generate same ID."""
        id1 = generate_movie_id("The Matrix", "The Matrix", 136)
        id2 = generate_movie_id("The Matrix", "The Matrix", 136)
        assert id1 == id2
        assert 0 < id1 < 100_000_000
    
    def test_generate_movie_id_different_movies(self):
        """Different movies should have different IDs."""
        id1 = generate_movie_id("The Matrix", "The Matrix", 136)
        id2 = generate_movie_id("Inception", "Inception", 148)
        assert id1 != id2
    
    def test_generate_movie_id_whitespace_handling(self):
        """Whitespace should be normalized."""
        id1 = generate_movie_id("  The Matrix  ", "The Matrix", 136)
        id2 = generate_movie_id("The Matrix", "The Matrix", 136)
        assert id1 == id2
    
    def test_generate_director_id_consistent(self):
        """Same director should have same ID."""
        id1 = generate_director_id("Christopher", "Nolan")
        id2 = generate_director_id("Christopher", "Nolan")
        assert id1 == id2
        assert 0 < id1 < 100_000_000
    
    def test_cinema_id_to_int_numeric(self):
        """Numeric string IDs should convert to int."""
        assert cinema_id_to_int("12345") == 12345
        assert cinema_id_to_int("99999") == 99999
    
    def test_cinema_id_to_int_alphanumeric(self):
        """Alphanumeric IDs should be hashed consistently."""
        id1 = cinema_id_to_int("P3757")
        id2 = cinema_id_to_int("P3757")
        assert id1 == id2
        assert isinstance(id1, int)


class TestParseHelpers:
    """Test parsing helper functions."""
    
    def test_parse_directors_single(self):
        directors = _parse_directors("Christopher Nolan")
        assert len(directors) == 1
        assert directors[0].first_name == "Christopher"
        assert directors[0].last_name == "Nolan"
    
    def test_parse_directors_multiple(self):
        directors = _parse_directors("Joel Coen | Ethan Coen")
        assert len(directors) == 2
        assert directors[0].first_name == "Joel"
        assert directors[0].last_name == "Coen"
        assert directors[1].first_name == "Ethan"
        assert directors[1].last_name == "Coen"
    
    def test_parse_directors_empty(self):
        assert _parse_directors(None) == []
        assert _parse_directors("") == []
        assert _parse_directors("Unknown Director") == []
    
    def test_parse_directors_invalid_format(self):
        # Names without space should be skipped
        directors = _parse_directors("Madonna | Christopher Nolan")
        assert len(directors) == 1
        assert directors[0].first_name == "Christopher"
    
    def test_parse_languages_dict_format(self):
        languages = _parse_languages([
            {"code": "fr", "label": "Français"},
            {"code": "en", "label": "English"}
        ])
        assert len(languages) == 2
        assert languages[0].code == "fr"
        assert languages[0].label == "Français"
    
    def test_parse_languages_string_format(self):
        languages = _parse_languages(["fr", "en"])
        assert len(languages) == 2
        assert languages[0].code == "fr"
        assert languages[0].label == "fr"  # Label defaults to code
    
    def test_parse_languages_empty(self):
        assert _parse_languages(None) == []
        assert _parse_languages([]) == []
    
    def test_parse_languages_mixed_format(self):
        languages = _parse_languages([
            {"code": "fr", "label": "Français"},
            "en",
            {"code": "", "label": "Empty"},  # Should be filtered out
            None  # Should be skipped
        ])
        assert len(languages) == 2
        assert languages[0].code == "fr"
        assert languages[1].code == "en"


class TestDatabaseOperations:
    """Test database operations with mocked Supabase."""
    
    @patch('db.insert_logic.supabase')
    def test_movie_exists_true(self, mock_supabase):
        """Test when movie exists in database."""
        mock_response = Mock()
        mock_response.data = [{"id": 12345}]
        mock_supabase.table.return_value.select.return_value.eq.return_value.execute.return_value = mock_response
        
        assert movie_exists(12345) is True
        mock_supabase.table.assert_called_with("movies")
    
    @patch('db.insert_logic.supabase')
    def test_movie_exists_false(self, mock_supabase):
        """Test when movie doesn't exist."""
        mock_response = Mock()
        mock_response.data = []
        mock_supabase.table.return_value.select.return_value.eq.return_value.execute.return_value = mock_response
        
        assert movie_exists(12345) is False
    
    @patch('db.insert_logic.supabase')
    def test_movie_exists_error(self, mock_supabase):
        """Test error handling in movie_exists."""
        mock_supabase.table.return_value.select.return_value.eq.return_value.execute.side_effect = Exception("DB Error")
        
        assert movie_exists(12345) is False
    
    @patch('db.insert_logic.supabase')
    @patch('db.insert_logic.movie_exists')
    @patch('db.insert_logic._insert_directors')
    @patch('db.insert_logic._insert_languages')
    def test_insert_movie_new(self, mock_languages, mock_directors, mock_exists, mock_supabase):
        """Test inserting a new movie."""
        mock_exists.return_value = False
        mock_supabase.table.return_value.insert.return_value.execute.return_value = Mock()
        
        movie_data = {
            "title": "The Matrix",
            "originalTitle": "The Matrix",
            "synopsis": "A computer hacker learns about the true nature of reality",
            "poster_url": "http://example.com/matrix.jpg",
            "runtime": "136min",
            "director": "Lana Wachowski | Lilly Wachowski",
            "languages": [{"code": "en", "label": "English"}],
            "hasDvdRelease": True,
            "isPremiere": False,
            "weeklyOuting": False
        }
        
        movie_id = insert_movie(movie_data)
        
        assert movie_id is not None
        assert movie_id > 0
        mock_supabase.table.assert_called_with("movies")
        mock_directors.assert_called_once()
        mock_languages.assert_called_once()
    
    @patch('db.insert_logic.supabase')
    @patch('db.insert_logic.movie_exists')
    def test_insert_movie_already_exists(self, mock_exists, mock_supabase):
        """Test when movie already exists."""
        mock_exists.return_value = True
        
        movie_data = {"title": "The Matrix", "runtime": "136min"}
        movie_id = insert_movie(movie_data)
        
        assert movie_id is None
        mock_supabase.table.assert_not_called()
    
    @patch('db.insert_logic.supabase')
    @patch('db.insert_logic.cinema_id_to_int')
    def test_insert_screening_success(self, mock_cinema_id, mock_supabase):
        """Test successful screening insertion."""
        mock_cinema_id.return_value = 12345
        mock_response = Mock()
        mock_response.data = []  # No existing screening
        mock_supabase.table.return_value.select.return_value.eq.return_value.eq.return_value.eq.return_value.eq.return_value.execute.return_value = mock_response
        
        screening_data = {
            "date": "2025-07-08",
            "time": "14:30:00",
            "version": "VF"
        }
        
        result = insert_screening(screening_data, movie_id=67890, cinema_id="P3757")
        
        assert result is True
        mock_supabase.table.assert_called_with("screenings")
    
    @patch('db.insert_logic.supabase')
    def test_insert_screening_duplicate(self, mock_supabase):
        """Test when screening already exists."""
        mock_response = Mock()
        mock_response.data = [{"id": 1}]  # Existing screening
        mock_supabase.table.return_value.select.return_value.eq.return_value.eq.return_value.eq.return_value.eq.return_value.execute.return_value = mock_response
        
        screening_data = {
            "date": "2025-07-08",
            "time": "14:30:00"
        }
        
        result = insert_screening(screening_data, movie_id=67890, cinema_id="P3757")
        
        assert result is False
    
    @patch('db.insert_logic.supabase')
    def test_insert_screening_datetime_format(self, mock_supabase):
        """Test parsing datetime format from API."""
        mock_response = Mock()
        mock_response.data = []
        mock_supabase.table.return_value.select.return_value.eq.return_value.eq.return_value.eq.return_value.eq.return_value.execute.return_value = mock_response
        
        screening_data = {
            "date": "2025-07-08",
            "time": "2025-07-08T14:30:00+02:00"  # Full datetime with timezone
        }
        
        result = insert_screening(screening_data, movie_id=67890, cinema_id="P3757")
        
        # Check that the time was properly extracted
        call_args = mock_supabase.table.return_value.insert.call_args[0][0]
        assert call_args["starts_at"] == "14:30:00"


@pytest.fixture
def sample_movie_data():
    """Fixture providing sample movie data."""
    return {
        "title": "Dune: Part Two",
        "originalTitle": "Dune: Part Two",
        "synopsis": "Paul Atreides unites with Chani and the Fremen...",
        "poster_url": "http://example.com/dune2.jpg",
        "runtime": "2h 46min",
        "director": "Denis Villeneuve",
        "languages": [
            {"code": "en", "label": "English"},
            {"code": "fr", "label": "Français"}
        ],
        "hasDvdRelease": False,
        "isPremiere": True,
        "weeklyOuting": False
    }


@pytest.fixture
def sample_cinema_data():
    """Fixture providing sample cinema data."""
    return {
        "id": "P3757",
        "name": "UGC Gobelins",
        "address": "66 avenue des Gobelins",
        "city": "Paris",
        "zipcode": "75013"
    }