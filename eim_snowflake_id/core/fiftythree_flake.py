import logging
import threading
import time
from typing import Optional

from eim_snowflake_id.constants import FiftyThreeID
from eim_snowflake_id.utils import extract_bits
from eim_snowflake_id.kw_exceptions import InvalidAlgorithmConfig, InvalidWorkerID, InvalidSystemClock

logger = logging.getLogger(__name__)


class FiftyThreeFlake:
    """Use FiftyThreeFlakeClient class to have access to the feature of this class."""

    # Fri, 1 Jan 2010 00:00:00.000 GMT, It's the base reference epoch value.
    __FIFTY_THREE_EPOCH = 1262304000000
    __TOTAL_FIFTY_THREE_BITS = 53
    __TIMESTAMP_BITS = 41
    _WORKER_ID_BITS = 11
    _SEQUENCE_BITS = 1

    def __init__(self, worker_id: int):
        """
        :param worker_id: a unique process/worker id for entire environment number.
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
            f"FiftyThree worker starting, {self._worker_id=}."
            f"{self._timestamp_left_shift=}, {self._WORKER_ID_BITS=}, {self._sequence_mask=}"
        )

    def _validate_worker_id(self):
        """
        Validate worker_id
        :return:
        """
        if self._worker_id > self._max_worker_id or self._worker_id < 0:
            raise InvalidWorkerID(message=f"worker_id cannot be greater than {self._max_worker_id} or less than 0")

    def _validate_total_bits(self):
        """
        Validate total number of bits for the algorith, it should be 53
        :return:
        """
        if self.__TIMESTAMP_BITS + self._WORKER_ID_BITS + self._SEQUENCE_BITS > self.__TOTAL_FIFTY_THREE_BITS:
            raise InvalidAlgorithmConfig(
                message=f"Total number of bits exceeds 53 bits. {self.__TIMESTAMP_BITS=}, "
                f"{self._WORKER_ID_BITS=}, {self._SEQUENCE_BITS=}"
            )

    def _generate_time(self) -> int:
        """
        Function to generate epoch time as millisecond
        :return:
        """
        time_in_seconds = time.time()
        time_in_milliseconds = time_in_seconds * 1000

        # trim the microsecond digits and return time in millisecond
        return int(time_in_milliseconds)

    def _wait_till_next_millis(self, last_timestamp: int) -> int:
        """
        Function that acts as sleep. Loop till next millisecond. Used for rollover.
        :param last_timestamp: the timestamp on which sequence collision happens
        :return:
        """
        timestamp = self._generate_time()
        while timestamp <= last_timestamp:
            timestamp = self._generate_time()

        return timestamp

    def _assemble_id(self, timestamp: int, sequence: int, worker_id: int):
        """
        Assemble fifty-three flake ID
        Args:
            timestamp: the current timestamp which need to be part of ID
            sequence: the current sequence which need to be part of ID
            worker_id: the worker id which need to be part of ID

        Returns:
            Fifty three bit flake long integer id
        """
        return (
            ((timestamp - self.__FIFTY_THREE_EPOCH) << self._timestamp_left_shift)
            | (worker_id << self._worker_id_shift)
            | sequence
        )

    def _next_id(self) -> int:
        """
        :return: a new snowflake id which is 53 bit in size
        """
        with self._lock:
            timestamp = self.get_timestamp()
            sequence = self._sequence
            last_timestamp = self._last_timestamp
            worker_id = self._worker_id

            logger.debug(
                f"Start creating 53 flake ID: {self._worker_id=}, {last_timestamp=}, {timestamp=}, " f"{sequence=}"
            )

            if last_timestamp > timestamp:
                logger.warning(f"The clock is moving backwards. {timestamp=} {last_timestamp=}")
                raise InvalidSystemClock(
                    message=f"The clock is moving backwards. {self._worker_id=}, {timestamp=}, {last_timestamp=}"
                )

            if last_timestamp == timestamp:
                logger.debug(f"Timestamps are the same, increase sequence: {sequence=}")
                sequence = (sequence + 1) & self._sequence_mask
                if sequence == 0:
                    timestamp = self._wait_till_next_millis(last_timestamp)
                    logger.debug(
                        f"New sequence is started, waited for next millis: {sequence=} {last_timestamp=} "
                        f"{timestamp=}"
                    )
            else:
                sequence = 0

            self._last_timestamp = timestamp
            self._sequence = sequence

            # It is required to keep actual ID generation inside the Lock block to guarantee ID uniqueness.
            new_id = self._assemble_id(timestamp=timestamp, sequence=sequence, worker_id=worker_id)
            logger.debug(
                f"Created 53 flake ID = {new_id}. {self._worker_id=}, {last_timestamp=}, {timestamp=}, {sequence=}"
            )
        return new_id

    def _id_from_timestamp(self, user_timestamp: int):
        """
        Function to test the class and create FlakeID in controlled fashion.
        Not intended to be used for PRODUCTION use.
        :param user_timestamp: unix timestamp in milliseconds. useful for testing.
        :return: a custom flake ID based on the provided timestamp
        """
        timestamp = user_timestamp
        custom_sequence = 0
        custom_id = self._assemble_id(timestamp=timestamp, sequence=custom_sequence, worker_id=self._worker_id)
        logger.debug(
            f"Created 53 flake ID using provided timestamp - {custom_id}. {self._worker_id=}, {self._last_timestamp=}, "
            f"{timestamp=}, {self._sequence=}, {custom_sequence=}"
        )
        return custom_id

    def get_worker_id(self) -> int:
        """
        :return: return worker_id
        """
        return self._worker_id

    def get_timestamp(self) -> int:
        """
        :return: current timestamp in milliseconds
        """
        return self._generate_time()

    def get_id(self, timestamp: Optional[int] = None) -> int:
        """
        :param timestamp: unix timestamp in milliseconds. useful for testing.
        :return: newly generated snowflake ID (53-bit)
        """
        if timestamp is not None:
            new_id = self._id_from_timestamp(user_timestamp=timestamp)
        else:
            new_id = self._next_id()
        return new_id

    def parse_id(self, flake_id: int) -> FiftyThreeID:
        """
        :param flake_id: 53-bit flake ID
        :return: FiftyThreeID tuple which contains timestamp, worker_id, _sequence
        """
        extract_timestamp = extract_bits(data=flake_id, shift=self._timestamp_left_shift, length=self.__TIMESTAMP_BITS)
        timestamp_in_millis = self.__FIFTY_THREE_EPOCH + extract_timestamp
        worker_id = extract_bits(data=flake_id, shift=self._worker_id_shift, length=self._WORKER_ID_BITS)
        sequence = extract_bits(data=flake_id, shift=0, length=self._SEQUENCE_BITS)
        return FiftyThreeID(timestamp_in_millis, worker_id, sequence)
