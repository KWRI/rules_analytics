import socket
import os


def extract_bits(data: int, shift: int, length: int) -> int:
    """
    Extract a portion of a bit string. Similar to substr().
    :param data: flake id
    :param shift: number of bit to shift
    :param length: the length of bits
    :return: the shifted data from flake id
    """
    bitmask = ((1 << length) - 1) << shift
    return (data & bitmask) >> shift


def get_ip_octets() -> tuple[int, int, int, int]:
    """
    Helper to extract and return all 4 IP octets as integers.

    Returns:
        tuple[int, int, int, int]: (first, second, third, fourth) IP octets
    """
    ip_string = socket.gethostbyname(socket.gethostname())

    if "/" in ip_string:
        ip_string, _ = ip_string.split("/")

    octets = ip_string.split(".")
    if len(octets) == 4:
        return tuple(int(octet) for octet in octets)

    raise ValueError(f"Invalid IPv4 address: {ip_string}")


def get_worker_id_by_ip(worker_bits: int) -> int:
    """
    Generates worker id based on the ip address by joining the last two octets as an integer.
    Examples:
        ip: 255.255.123.101 -> 123101 % 2**getattr(SixtyFourFlake, "_WORKER_ID_BITS")

    Args:
        worker_bits: number of worker bit supported by core algorith

    Returns:
        int: worker_id
    """
    _, _, third_octet, fourth_octet = get_ip_octets()

    third_octet = third_octet & 0xFF  # 8 bits
    fourth_octet = fourth_octet & 0xFF  # 8 bits

    # Layout: [8 bits 3rd octet][8 bits 4th octet]
    worker_id = (third_octet << 8) | fourth_octet

    # Ensure the worker ID is within the valid range
    return worker_id % (2**worker_bits)


def generate_worker_by_ip_process_id(worker_bits: int) -> int:
    """Generates a unique worker ID based on the IP address and process ID.

    Args:
        worker_bits (int): The number of bits allocated for the worker ID.

    Returns:
        int: A unique worker ID within the allowed range.
    """
    _, _, third_octet, fourth_octet = get_ip_octets()

    third_octet = third_octet & 0b11  # Use only 2 bits
    fourth_octet = fourth_octet & 0xFF  # Use only 8 bits
    process_id = os.getpid() & 0b111111  # Use only 6 bits

    # Layout: [2 bits 3rd octet][8 bits 4th octet][6 bits process id]
    worker_id = (third_octet << 14) | (fourth_octet << 6) | process_id

    # Ensure the worker ID is within the valid range
    return worker_id % (2**worker_bits)
