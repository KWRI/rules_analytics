from typing import NamedTuple

PACKAGE_VERSION = "2.2.0"

MAX_BITS_ABS_64 = 63
MAX_BITS_ABS_53 = 52

TEST_WORKER_IP = "68.96.44.141"

APP_NAME = "eim-slp-tools"

class FiftyThreeID(NamedTuple):
    timestamp_in_millis: int
    worker_id: int
    sequence: int


class SixtyFourID(NamedTuple):
    timestamp_in_millis: int
    worker_id: int
    sequence: int

