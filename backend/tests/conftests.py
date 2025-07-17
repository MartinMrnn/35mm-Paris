"""
Pytest configuration and shared fixtures.
"""

import os
import sys
from pathlib import Path

# Add src to Python path for imports
src_path = Path(__file__).parent.parent / "src"
sys.path.insert(0, str(src_path))

# Set test environment variables
os.environ["SUPABASE_URL"] = "https://test.supabase.co"
os.environ["SUPABASE_KEY"] = "test-key-12345"
os.environ["LOG_LEVEL"] = "DEBUG"
os.environ["LOG_FORMAT"] = "text"  # Better for test output


from unittest.mock import Mock

import pytest


@pytest.fixture
def mock_supabase_client():
    """Mock Supabase client for testing."""
    mock_client = Mock()

    # Setup common table mocks
    mock_table = Mock()
    mock_table.select.return_value = mock_table
    mock_table.insert.return_value = mock_table
    mock_table.upsert.return_value = mock_table
    mock_table.eq.return_value = mock_table
    mock_table.execute.return_value = Mock(data=[])

    mock_client.table.return_value = mock_table

    return mock_client


@pytest.fixture
def mock_allocine_api():
    """Mock AllocineAPI for testing."""
    mock_api = Mock()

    # Sample movie data
    mock_api.get_movies.return_value = [
        {
            "title": "Test Movie",
            "originalTitle": "Test Movie",
            "runtime": "1h 30min",
            "director": "Test Director",
            "languages": [{"code": "fr", "label": "Français"}],
            "hasDvdRelease": False,
            "isPremiere": False,
            "weeklyOuting": False,
        }
    ]

    # Sample showtime data
    mock_api.get_showtime.return_value = [
        {
            "title": "Test Movie",
            "showtimes": [
                {"startsAt": "2025-07-08T14:30:00", "diffusionVersion": "VF"}
            ],
        }
    ]

    # Sample cinema data
    mock_api.get_cinema.return_value = [
        {"id": "P0001", "name": "Test Cinema", "address": "123 Test Street"}
    ]

    return mock_api
