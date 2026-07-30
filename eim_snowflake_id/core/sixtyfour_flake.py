import logging
import threading
import time

from eim_snowflake_id.constants import SixtyFourID
from eim_snowflake_id.utils import extract_bits
from eim_snowflake_id.kw_exceptions import (
    InvalidAlgorithmConfig,
    InvalidWorkerID,
    InvalidSystemClock,
)

logger = logging.getLogger(__name__)


class SixtyFourFlake:
    """Use SixtyFourFlakeClient class to have access to the feature of this class."""

    # Friday, January 1, 2010 12:00:00 AM GMT, It's the base reference epoch value.
    __SIXTY_FOUR_EPOCH = 1262304000000
    __TOTAL_SIXTY_FOUR_BITS = 64
    __TIMESTAMP_BITS = 41
    _WORKER_ID_BITS = 16
    _SEQUENCE_BITS = 7

    def __init__(self, worker_id: int):
        """Initialize SixtyFourFlake client.

        Args:
            worker_id: a unique process/worker id for entire environment number.
        """
        self._worker_id = worker_id

        self._max_worker_id = -1 ^ (-1 << self._WORKER_ID_BITS)
        self._worker_id_shift = self._SEQUENCE_BITS
        self._timestamp_left_shift = self._SEQUENCE_BITS + self._WORKER_ID_BITS
        self._sequence_mask = -1 ^ (-1 << self._SEQUENCE_BITS)

        # Variables used at runtime
        self._sequence = 0
        self._last_timestamp = -1

        # Validate worker_id and total bits
        self._validate_worker_id()
        self._validate_total_bits()

        # Thread lock for race condition
        self._lock = threading.Lock()

        logger.debug(
            f"SixtyFour worker starting, {self._worker_id=}."
            f"{self._timestamp_left_shift=}, {self._WORKER_ID_BITS=}, {self._sequence_mask=}"
        )

    def _validate_worker_id(self):
        """Validate worker_id"""
        if self._worker_id > self._max_worker_id or self._worker_id < 0:
            raise InvalidWorkerID(message=f"worker_id cannot be greater than {self._max_worker_id} or less than 0")

    def _validate_total_bits(self):
        """Validate total number of bits for the algorith, it should be 64"""
        if self.__TIMESTAMP_BITS + self._WORKER_ID_BITS + self._SEQUENCE_BITS > self.__TOTAL_SIXTY_FOUR_BITS:
            raise InvalidAlgorithmConfig(
                message=f"Total number of bits exceeds 64 bits. {self.__TIMESTAMP_BITS=}, "
                f"{self._WORKER_ID_BITS=}, {self._SEQUENCE_BITS=}"
            )

    @staticmethod
    def _generate_time() -> int:
        """Function to generate epoch time as milliseconds
        Returns: Millisecond time in epoch
        """
        time_in_seconds = time.time()
        time_in_milliseconds = time_in_seconds * 1000

        # trim the microsecond digits, and return time in millisecond
        return int(time_in_milliseconds)

    def _wait_till_next_millis(self, last_timestamp: int) -> int:
        """Function that acts as sleep. Loop till next millisecond. Used for rollover.
        Args:
            last_timestamp: the timestamp on which sequence collision happens
        Returns:
            Next millisecond time in epoch
        """
        timestamp = self._generate_time()
        while timestamp <= last_timestamp:
            timestamp = self._generate_time()

        return timestamp

    def _assemble_id(self, timestamp: int, sequence: int, worker_id: int) -> int:
        """Assemble 64 flake ID
        Args:
            timestamp: the current timestamp which need to be part of ID
            sequence: the current sequence which need to be part of ID
            worker_id: the worker id which need to be part of ID

        Returns:
            Sixty four bit flake long integer id
        """
        return (
            ((timestamp - self.__SIXTY_FOUR_EPOCH) << self._timestamp_left_shift)
            | (worker_id << self._worker_id_shift)
            | sequence
        )

    def _next_id(self) -> int:
        """Generate nex 64-bit ID

        Returns:
            a new snowflake id which is 64 bit in size
        """
        with self._lock:
            timestamp = self.get_timestamp()
            sequence = self._sequence
            last_timestamp = self._last_timestamp
            worker_id = self._worker_id

            logger.debug(f"Start creating 64 flake ID: {worker_id=}, {last_timestamp=}, {timestamp=}, " f"{sequence=}")

            if last_timestamp > timestamp:
                logger.warning(f"The clock is moving backwards. {timestamp=} {last_timestamp=}")
                raise InvalidSystemClock(
                    message=f"The clock is moving backwards. {worker_id=}, {timestamp=}, {last_timestamp=}"
                )

            if last_timestamp == timestamp:
                logger.debug(f"Timestamps are the same, increase sequence: {sequence=}")
                sequence = (sequence + 1) & self._sequence_mask
                if sequence == 0:
                    timestamp = self._wait_till_next_millis(last_timestamp)
                    logger.debug(
                        f"New sequence is started, waited for next millis: {sequence=} "
                        f"{last_timestamp=} {timestamp=}"
                    )
            else:
                sequence = 0

            self._last_timestamp = timestamp
            self._sequence = sequence

            # It is required to keep actual ID generation inside the Lock block to guarantee ID uniqueness.
            new_id = self._assemble_id(timestamp=timestamp, sequence=sequence, worker_id=worker_id)

            logger.debug(
                f"Created 64 flake ID - {new_id}. {worker_id=}, {last_timestamp=}, {timestamp=}, " f"{sequence=}"
            )
        return new_id

    def _id_from_timestamp(self, user_timestamp: int) -> int:
        """Function to test the class and create FlakeID in controlled fashion.

        Args:
            user_timestamp: unix timestamp in milliseconds. useful for testing.

        Returns:
            a custom flake ID based on the provided timestamp
        """
        timestamp = user_timestamp
        custom_sequence = 0
        custom_id = self._assemble_id(timestamp=timestamp, sequence=custom_sequence, worker_id=self._worker_id)
        logger.debug(
            f"Created 64 flake ID using provided timestamp - {custom_id}. {self._worker_id=}, {self._last_timestamp=}, "
            f"{timestamp=}, {self._sequence=}, {custom_sequence=}"
        )
        return custom_id

    def get_timestamp(self) -> int:
        """Get current epoch time in milliseconds
        Returns:
            current timestamp in milliseconds
        """
        return self._generate_time()

    def get_id(self, timestamp: int | None = None) -> int:
        """Get 64-bit snowflake ID
        Args:
            timestamp: unix timestamp in milliseconds. useful for testing.

        Returns:
            newly generated snowflake ID (64-bit)
        """
        if timestamp is not None:
            new_id = self._id_from_timestamp(user_timestamp=timestamp)
        else:
            new_id = self._next_id()
        return new_id

    def parse_id(self, flake_id: int) -> SixtyFourID:
        """Parse the flake ID
        Args:
            flake_id: 64-bit flake ID

        Returns:
            SixtyFourID tuple which contains timestamp, worker_id, _sequence
        """
        extract_timestamp = extract_bits(data=flake_id, shift=self._timestamp_left_shift, length=self.__TIMESTAMP_BITS)
        timestamp_in_millis = self.__SIXTY_FOUR_EPOCH + extract_timestamp
        worker_id = extract_bits(data=flake_id, shift=self._worker_id_shift, length=self._WORKER_ID_BITS)
        sequence = extract_bits(data=flake_id, shift=0, length=self._SEQUENCE_BITS)
        return SixtyFourID(timestamp_in_millis, worker_id, sequence)
