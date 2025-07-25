"""
Tests for database insertion logic.
Updated to test BIGINT support for cinema IDs.
"""

import pytest

from db.insert_logic import (
    cinema_id_to_bigint,
    cinema_id_to_int,
    generate_circuit_id,
    generate_director_id,
    generate_movie_id,
    parse_directors,
    parse_languages,
    parse_runtime,
)


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
        assert 0 < id1 < 2_000_000_000  # Safe INT range

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
        assert 0 < id1 < 2_000_000_000  # Safe INT range

    def test_generate_circuit_id_consistent(self):
        """Same circuit should have same ID."""
        id1 = generate_circuit_id("circuit-81002")
        id2 = generate_circuit_id("circuit-81002")
        assert id1 == id2
        assert 0 < id1 < 100_000_000  # Circuit IDs stay in smaller range

    def test_cinema_id_to_int_is_alias_for_bigint(self):
        """cinema_id_to_int should be an alias for cinema_id_to_bigint."""
        test_id = "P3757"
        assert cinema_id_to_int(test_id) == cinema_id_to_bigint(test_id)

    def test_cinema_id_to_bigint_numeric(self):
        """Numeric string IDs should convert directly."""
        assert cinema_id_to_bigint("12345") == 12345
        assert cinema_id_to_bigint("99999") == 99999

    def test_cinema_id_to_bigint_large_numeric(self):
        """Large numeric IDs that exceed INT range."""
        large_id = "9876543210"  # Dépasse INT max (2,147,483,647)
        result = cinema_id_to_bigint(large_id)
        assert result == 9876543210
        assert isinstance(result, int)

    def test_cinema_id_to_bigint_alphanumeric(self):
        """Alphanumeric IDs should be hashed consistently."""
        id1 = cinema_id_to_bigint("P3757")
        id2 = cinema_id_to_bigint("P3757")
        assert id1 == id2
        assert isinstance(id1, int)
        assert 0 < id1 < 9_000_000_000_000_000_000  # BIGINT range

    def test_cinema_id_to_bigint_already_int(self):
        """Already int values should be returned as-is."""
        assert cinema_id_to_bigint(12345) == 12345
        assert cinema_id_to_bigint(9876543210) == 9876543210


class TestParseHelpers:
    """Test parsing helper functions."""

    def test_parse_directors_single(self):
        directors = parse_directors("Christopher Nolan")
        assert len(directors) == 1
        assert directors[0].first_name == "Christopher"
        assert directors[0].last_name == "Nolan"

    def test_parse_directors_multiple(self):
        directors = parse_directors("Joel Coen | Ethan Coen")
        assert len(directors) == 2
        assert directors[0].first_name == "Joel"
        assert directors[0].last_name == "Coen"
        assert directors[1].first_name == "Ethan"
        assert directors[1].last_name == "Coen"

    def test_parse_directors_empty(self):
        assert parse_directors(None) == []
        assert parse_directors("") == []
        assert parse_directors("Unknown Director") == []

    def test_parse_directors_invalid_format(self):
        # Names without space should be skipped
        directors = parse_directors("Madonna | Christopher Nolan")
        assert len(directors) == 1
        assert directors[0].first_name == "Christopher"

    def test_parse_languages_dict_format(self):
        languages = parse_languages(
            [{"code": "fr", "label": "Français"}, {"code": "en", "label": "English"}]
        )
        assert len(languages) == 2
        assert languages[0].code == "fr"
        assert languages[0].label == "Français"

    def test_parse_languages_string_format(self):
        languages = parse_languages(["fr", "en"])
        assert len(languages) == 2
        assert languages[0].code == "fr"
        assert languages[0].label == "fr"  # Label defaults to code

    def test_parse_languages_empty(self):
        assert parse_languages(None) == []
        assert parse_languages([]) == []

    def test_parse_languages_mixed_format(self):
        languages = parse_languages(
            [
                {"code": "fr", "label": "Français"},
                "en",
                {"code": "", "label": "Empty"},  # Should be filtered out
                None,  # Should be skipped
            ]
        )
        assert len(languages) == 2
        assert languages[0].code == "fr"
        assert languages[1].code == "en"


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
            {"code": "fr", "label": "Français"},
        ],
        "hasDvdRelease": False,
        "isPremiere": True,
        "weeklyOuting": False,
    }


@pytest.fixture
def sample_cinema_data():
    """Fixture providing sample cinema data."""
    return {
        "id": "P3757",
        "name": "UGC Gobelins",
        "address": "66 avenue des Gobelins",
        "city": "Paris",
        "zipcode": "75013",
    }
