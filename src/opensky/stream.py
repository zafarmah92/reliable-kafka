"""
###################################################
Polls the free OpenSky API for live aircraft data, one area of the
world at a time so it doesn't pull in too much at once.
###################################################
"""

import random
import time
from collections.abc import Iterator
from typing import Any
import requests
from common.logger import get_logger

logger = get_logger(__name__)

STATES_URL = "https://opensky-network.org/api/states/all"
# Anonymous OpenSky access is limited to ~400 requests/day. An average
# interval of half this max (~5 min) keeps daily calls comfortably under
# that quota; the old 3-min max averaged ~960 calls/day, well over it.
DEFAULT_MAX_POLL_SECONDS = 600

# Continental US bounding box - keeps each poll to a manageable number of
# aircraft instead of the whole planet.
DEFAULT_BBOX = {"lamin": 24.5, "lomin": -125.0, "lamax": 49.5, "lomax": -66.5}

FIELDS = [
    "icao24", "callsign", "origin_country", "time_position", "last_contact",
    "longitude", "latitude", "baro_altitude", "on_ground", "velocity",
    "true_track", "vertical_rate", "sensors", "geo_altitude", "squawk",
    "spi", "position_source",
]


class OpenSkyStream:
    def __init__(self, max_poll_seconds: float = DEFAULT_MAX_POLL_SECONDS, bbox: dict[str, float] = DEFAULT_BBOX) -> None:
        self.max_poll_seconds: float = max_poll_seconds
        self.bbox: dict[str, float] = bbox

    def events(self) -> Iterator[dict[str, Any]]:
        """Yield one dict per aircraft state vector, forever."""
        while True:
            # --- if the API call fails, wait and try again instead of crashing ---
            try:
                response = requests.get(STATES_URL, params=self.bbox, timeout=15)
                response.raise_for_status()
                payload = response.json()
            except requests.exceptions.RequestException as e:
                delay = random.uniform(0, self.max_poll_seconds)
                logger.error(f"OpenSky API request failed, will retry in {delay:.0f}s: {e}")
                time.sleep(delay)
                continue

            # --- each state is a plain list, so match it up with FIELDS by position ---
            for state in payload.get("states") or []:
                event = dict(zip(FIELDS, state))
                if event.get("callsign"):
                    event["callsign"] = event["callsign"].strip() or None
                yield event
            time.sleep(random.uniform(0, self.max_poll_seconds))
