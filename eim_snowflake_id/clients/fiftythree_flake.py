import logging
import os

from eim_snowflake_id.clients.base import BaseSingleton
from eim_snowflake_id.constants import FiftyThreeID, MAX_BITS_ABS_53
from eim_snowflake_id.kw_exceptions import InvalidUnixTimestamp, InvalidFlakeID
from eim_snowflake_id.core import FiftyThreeFlake
from eim_snowflake_id.utils import get_worker_id_by_ip

logger = logging.getLogger(__name__)


class FiftyThreeFlakeClient(BaseSingleton):
    """Singleton class with to generate FiftyThree bit snowflake identifier.

    Note:
        This client does **not** guarantee uniqueness of IDs in a multiprocessing
        workflow (e.g., multiple processes on the same host, like Apache Beam), because it does not
        incorporate the process ID into the worker ID generation.
        ID collisions.
    """

    def __new__(cls, worker_id: int | None = None):
        if cls.instance is None:
            cls.instance = super(FiftyThreeFlakeClient, cls).__new__(cls)
            # Keeping track of the PID where the instance was created to verify later whether a new instance
            # is being initialized within the same OS process.
            cls.instance_pid = os.getpid()
        return cls.instance

    def __init__(self, worker_id: int | None = None):
        """
        Args:
            worker_id: Integer or None.
            Initialize the worker id to an integer value that is
            unique across all the VM/containers.
            if initialized with None then builtin method to generate
            worker_id will be used.
            The caller can implement custom logic to set worker_id
        """
        logger.debug("Init FiftyThreeFlakeClient client")

        self.worker_bits = FiftyThreeFlake._WORKER_ID_BITS
        if worker_id is None:
            worker_id = self.generate_worker_id(self.worker_bits)

        self.fifty_three_client = FiftyThreeFlake(worker_id=worker_id)

    def generate_flake_id(self) -> int:
        """Method to generate FiftyThree Flake ID"""
        return self.fifty_three_client.get_id()

    def generate_flake_id_by_ts(self, timestamp: int) -> int:
        """Method to generate FiftyThree Flake ID by timestamp
        Args:
            timestamp: unix timestamp in milliseconds.
        """
        if (
            (timestamp is not None)
            and (isinstance(timestamp, int))
            and not (isinstance(timestamp, bool))
            and (timestamp > 0)
        ):
            return self.fifty_three_client.get_id(timestamp=timestamp)

        raise InvalidUnixTimestamp(message=f"Timestamp is not a positive integer. {timestamp=}")

    def parse_flake_id(self, flake_id: int) -> FiftyThreeID:
        """Method to parse FiftyThree Flake ID
        Args:
            flake_id: 53-bit positive integer.
        """
        if (
            (flake_id is not None)
            and (isinstance(flake_id, int))
            and not (isinstance(flake_id, bool))
            and (flake_id > 0)
            and (flake_id.bit_length() <= MAX_BITS_ABS_53)
        ):
            return self.fifty_three_client.parse_id(flake_id=flake_id)

        raise InvalidFlakeID(message=f"Flake ID is not a {MAX_BITS_ABS_53 + 1}-bit positive integer. {flake_id=}")

    def _disconnect(self):
        pass

    @classmethod
    def generate_worker_id(cls, worker_bits: int) -> int:
        """
        Generates a worker ID using either the IP address.

        Args:
            worker_bits (int): Number of bits allocated for the worker ID.

        Returns:
            int: Generated worker ID.
        """
        return get_worker_id_by_ip(worker_bits=worker_bits)
