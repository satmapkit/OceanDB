import pytest

from OceanDB.index_experiment import run_scenario_with_timeout


@pytest.mark.unit
def test_scenario_timeout_must_be_positive():
    with pytest.raises(ValueError, match="timeout_seconds must be positive"):
        run_scenario_with_timeout(None, None, timeout_seconds=0)
