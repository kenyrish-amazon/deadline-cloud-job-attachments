# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""Module that defines the hashing algorithms supported by this library."""

import io

from enum import Enum

from ..exceptions import UnsupportedHashingAlgorithmError


class HashAlgorithm(str, Enum):
    """
    Enumerant of all hashing algorithms supported by this library.

    Algorithms:
      XXH128 - The xxhash 128-bit hashing algorithm.

    """

    XXH128 = "xxh128"


_HASH_READ_SIZE = 1024 * 1024


def hash_file(file_path: str, hash_alg: HashAlgorithm) -> str:
    """Hashes the given file using the given hashing algorithm."""
    if hash_alg == HashAlgorithm.XXH128:
        from xxhash import xxh3_128

        hasher = xxh3_128()
    else:
        raise UnsupportedHashingAlgorithmError(
            f"Unsupported hashing algorithm provided: {hash_alg}"
        )

    # Read in large chunks into a reused buffer. With small reads, the per-call overhead and
    # GIL contention between hashing threads limited hashing to well below disk speed.
    buffer = bytearray(_HASH_READ_SIZE)
    view = memoryview(buffer)
    with io.FileIO(file_path, "rb") as file:
        while True:
            count = file.readinto(buffer)
            if not count:
                break
            hasher.update(view[:count])
        return hasher.hexdigest()


def hash_data(data: bytes, hash_alg: HashAlgorithm) -> str:
    """Hashes the given data bytes using the given hashing algorithm."""
    if hash_alg == HashAlgorithm.XXH128:
        from xxhash import xxh3_128

        hasher = xxh3_128()
    else:
        raise UnsupportedHashingAlgorithmError(
            f"Unsupported hashing algorithm provided: {hash_alg}"
        )

    hasher.update(data)
    return hasher.hexdigest()
