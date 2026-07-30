import logging
import os

from eim_snowflake_id.clients.base import BaseSingleton
from eim_snowflake_id.constants import SixtyFourID, MAX_BITS_ABS_64
from eim_snowflake_id.kw_exceptions import InvalidUnixTimestamp, InvalidFlakeID
from eim_snowflake_id.core import SixtyFourFlake
from eim_snowflake_id.utils import generate_worker_by_ip_process_id, get_worker_id_by_ip

logger = logging.getLogger(__name__)


class SixtyFourFlakeClient(BaseSingleton):
    """Singleton class with to generate SixtyFour bit snowflake identifier."""

    def __new__(cls, worker_id: int | None = None, use_pid_in_worker_id: bool = False):
        if cls.instance is None:
            cls.instance = super(SixtyFourFlakeClient, cls).__new__(cls)
            # Keeping track of the PID where the instance was created to verify later whether a new instance
            # is being initialized within the same OS process.
            cls.instance_pid = os.getpid()
        return cls.instance

    def __init__(self, worker_id: int | None = None, use_pid_in_worker_id: bool = False):
        """
        Initializes the SixtyFourFlakeClient.

        Args:
            worker_id (int | None): Unique worker ID across all VM/container processes.
                If provided, it will be used as-is. If None, the worker ID will be generated
                based on the host IP and optionally the process ID.
            use_pid_in_worker_id (bool): If True and worker_id is not provided, the process ID
                will be included when generating the worker ID.
        """
        logger.debug("Init SixtyFourFlakeClient client")
        self._use_pid_in_worker_id = use_pid_in_worker_id
        self.worker_bits = SixtyFourFlake._WORKER_ID_BITS
        if worker_id is None:
            worker_id = self.generate_worker_id(self.worker_bits, self._use_pid_in_worker_id)

        self.sixty_four_client = SixtyFourFlake(worker_id=worker_id)

    def generate_flake_id(self) -> int:
        """Generates a new 64-bit Snowflake ID.

        Returns:
            int: A unique 64-bit identifier.
        """
        return self.sixty_four_client.get_id()

    def generate_flake_id_by_ts(self, timestamp: int) -> int:
        """Generates a 64-bit Snowflake ID based on a given timestamp.

        Args:
            timestamp (int): Unix timestamp in milliseconds.

        Returns:
            int: A unique 64-bit identifier based on the given timestamp.

        Raises:
            InvalidUnixTimestamp: If the timestamp is not a positive integer.
        """
        if (
            (timestamp is not None)
            and (isinstance(timestamp, int))
            and not (isinstance(timestamp, bool))
            and (timestamp > 0)
        ):
            return self.sixty_four_client.get_id(timestamp=timestamp)

        raise InvalidUnixTimestamp(message=f"Timestamp is not a positive integer. {timestamp=}")

    def parse_flake_id(self, flake_id: int) -> SixtyFourID:
        """Parses a 64-bit Snowflake ID into its components.

        Args:
            flake_id (int): A 64-bit positive integer.

        Returns:
            SixtyFourID: Parsed details of the flake ID.

        Raises:
            InvalidFlakeID: If the input is not a valid 64-bit Snowflake ID.
        """
        if (
            (flake_id is not None)
            and (isinstance(flake_id, int))
            and not (isinstance(flake_id, bool))
            and (flake_id > 0)
            and (flake_id.bit_length() <= MAX_BITS_ABS_64)
        ):
            return self.sixty_four_client.parse_id(flake_id=flake_id)

        raise InvalidFlakeID(message=f"Flake ID is not a {MAX_BITS_ABS_64 + 1}-bit positive integer. {flake_id=}")

    def _disconnect(self):
        pass

    @classmethod
    def generate_worker_id(cls, worker_bits: int, use_pid_in_worker_id: bool = False) -> int:
        """
        Generates a worker ID using either the IP address or a combination of IP address and process ID.

        Args:
            worker_bits (int): Number of bits allocated for the worker ID.
            use_pid_in_worker_id (bool): If True, include the process ID in the worker ID.

        Returns:
            int: Generated worker ID.
        """
        if use_pid_in_worker_id:
            return generate_worker_by_ip_process_id(worker_bits=worker_bits)
        return get_worker_id_by_ip(worker_bits=worker_bits)

    @classmethod
    def _bootstrap_instance_args(cls) -> tuple:
        """Returns the current instance’s state needed to reinitialize on PID change."""
        if cls.instance is not None:
            return (), {"use_pid_in_worker_id": cls.instance._use_pid_in_worker_id}
        return (), {}  # Default fallback
