from typing import get_args

import pytest

from OceanDB.along_track_missions import MISSION_DEFINITIONS, Mission

pytestmark = pytest.mark.unit


def test_all_missions_defined():
    assert set(get_args(Mission)) == set(MISSION_DEFINITIONS.keys())
