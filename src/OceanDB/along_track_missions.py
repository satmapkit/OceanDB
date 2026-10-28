"""Shared mission identities for MY 1 Hz along-track data.

Mission phases follow the Copernicus DUACS L3 Product User Manual, Issue 2.0
(July 2026).
"""

from dataclasses import dataclass
from typing import Literal

Mission = Literal[
    "al",
    "alg",
    "c2",
    "c2n",
    "e1",
    "e1g",
    "e2",
    "en",
    "enn",
    "g2",
    "h2a",
    "h2ag",
    "h2b",
    "j1",
    "j1g",
    "j1n",
    "j2",
    "j2g",
    "j2n",
    "j3",
    "j3g",
    "j3n",
    "s3a",
    "s3b",
    "s6a",
    "swon",
    "swonc",
    "tp",
    "tpn",
]


@dataclass(frozen=True)
class MissionDefinition:
    """Describe a canonical mission's Copernicus identity.

    :param shortname: Mission component of the Copernicus dataset ID, e.g. 'j2n'.
    :param longname: Human-readable mission name and details.
    :param dataset_suffix: Suffix following PT1S in the dataset ID.
    :param default_version: Catalogue release used when no version is requested.
    """

    shortname: str
    longname: str
    dataset_suffix: str = ""
    default_version: str = "202411"


MISSION_DEFINITIONS: dict[str, MissionDefinition] = {
    "al": MissionDefinition("al", "Saral/AltiKa"),
    "alg": MissionDefinition("alg", "Saral/AltiKa on its geodetic orbit (only for dt)"),
    "c2": MissionDefinition("c2", "Cryosat-2"),
    "c2n": MissionDefinition("c2n", "Cryosat-2 on its new orbit"),
    "e1": MissionDefinition("e1", "ERS-1 (only for dt)"),
    "e1g": MissionDefinition("e1g", "ERS-1 geodetic phase (only for dt)"),
    "e2": MissionDefinition("e2", "ERS-2 (only for dt)"),
    "en": MissionDefinition("en", "Envisat (only for dt)"),
    "enn": MissionDefinition("enn", "Envisat on its new orbit (only for dt)"),
    "g2": MissionDefinition("g2", "GFO (only for dt)"),
    "h2a": MissionDefinition("h2a", "HaiYang-2A (only for dt)"),
    "h2ag": MissionDefinition("h2ag", "HaiYang-2A on its geodetic orbit (only for dt)"),
    "h2b": MissionDefinition("h2b", "HaiYang-2B"),
    "j1": MissionDefinition("j1", "Jason-1 (only for dt)"),
    "j1g": MissionDefinition("j1g", "Jason-1 on its geodetic orbit (only for dt)"),
    "j1n": MissionDefinition("j1n", "Jason-1 on its new orbit (only for dt)"),
    "j2": MissionDefinition("j2", "OSTM/Jason-2 (only for dt)"),
    "j2g": MissionDefinition("j2g", "OSTM/Jason-2 on its long repeat orbit (LRO)"),
    "j2n": MissionDefinition("j2n", "OSTM/Jason-2 on its interleaved orbit"),
    "j3": MissionDefinition("j3", "Jason-3"),
    "j3g": MissionDefinition(
        "j3g",
        "Jason-3 on its long-repeat orbit",
        dataset_suffix="-i",
        default_version="202506",
    ),
    "j3n": MissionDefinition("j3n", "Jason-3 on its new (interleaved) orbit"),
    "s3a": MissionDefinition("s3a", "Sentinel-3A"),
    "s3b": MissionDefinition("s3b", "Sentinel-3B"),
    "s6a": MissionDefinition(
        "s6a-lr", "Sentinel-6A with LRM mode measurement"
    ),  # note that s6a's ID is "s6a-lr"
    "swon": MissionDefinition("swon", "SWOT-nadir"),
    "swonc": MissionDefinition(
        "swonc", "SWOT-nadir (CALVAL phase with 1 day cycle, only for dt)"
    ),
    "tp": MissionDefinition("tp", "TOPEX/Poseidon (only for dt)"),
    "tpn": MissionDefinition("tpn", "TOPEX/Poseidon on its new orbit (only for dt)"),
}
