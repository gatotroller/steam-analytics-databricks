# tests/test_steam_api_client.py
import datetime
from utils.steam_api_client import get_logical_shift


class TestGetLogicalShift:
    """
    get_logical_shift returns the 'logical day' of an extraction.
    Steam refreshes its catalog at 16:00 UTC, so:
    - Anything extracted BEFORE 16:00 UTC belongs to the previous logical day
    - Anything extracted AT or AFTER 16:00 UTC belongs to the current logical day
    """

    def test_just_before_cutoff_returns_previous_day(self):
        # 15:59:59 UTC on April 28 → belongs to April 27
        dt = datetime.datetime(2026, 4, 28, 15, 59, 59, tzinfo=datetime.timezone.utc)
        assert get_logical_shift(dt) == datetime.date(2026, 4, 27)

    def test_exactly_at_cutoff_returns_current_day(self):
        # 16:00:00 UTC on April 28 → belongs to April 28
        # This is the boundary case - critical to nail down behavior
        dt = datetime.datetime(2026, 4, 28, 16, 0, 0, tzinfo=datetime.timezone.utc)
        assert get_logical_shift(dt) == datetime.date(2026, 4, 28)

    def test_just_after_cutoff_returns_current_day(self):
        dt = datetime.datetime(2026, 4, 28, 16, 0, 1, tzinfo=datetime.timezone.utc)
        assert get_logical_shift(dt) == datetime.date(2026, 4, 28)

    def test_midnight_returns_previous_day(self):
        # 00:00 UTC is well before 16:00, belongs to the day before
        dt = datetime.datetime(2026, 4, 28, 0, 0, 0, tzinfo=datetime.timezone.utc)
        assert get_logical_shift(dt) == datetime.date(2026, 4, 27)

    def test_late_night_returns_current_day(self):
        # 23:59 UTC is after 16:00, belongs to the current day
        dt = datetime.datetime(2026, 4, 28, 23, 59, 0, tzinfo=datetime.timezone.utc)
        assert get_logical_shift(dt) == datetime.date(2026, 4, 28)

    def test_handles_month_boundary(self):
        # Edge case: 03:00 UTC on May 1 → belongs to April 30
        dt = datetime.datetime(2026, 5, 1, 3, 0, 0, tzinfo=datetime.timezone.utc)
        assert get_logical_shift(dt) == datetime.date(2026, 4, 30)

    def test_handles_year_boundary(self):
        # Edge case: 03:00 UTC on Jan 1 → belongs to Dec 31 of previous year
        dt = datetime.datetime(2026, 1, 1, 3, 0, 0, tzinfo=datetime.timezone.utc)
        assert get_logical_shift(dt) == datetime.date(2025, 12, 31)