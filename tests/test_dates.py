from datetime import date

import pytest

from energy_pipeline.pipeline import iter_dates


def test_date_range():
    dates = list(
        iter_dates(
            date(2026, 11, 1),
            date(2026, 11, 3)
        )
    )

    assert dates == [
        "2026-11-01",
        "2026-11-02",
        "2026-11-03"
    ]

def test_invalid_date_range():
    with pytest.raises(ValueError):
        list(
            iter_dates(
                date(2026, 11, 5),
                date(2026, 11, 1)
            )
        )