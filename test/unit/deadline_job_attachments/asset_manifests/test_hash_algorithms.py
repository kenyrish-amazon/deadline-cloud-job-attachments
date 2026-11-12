# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

import os

import pytest
from xxhash import xxh3_128_hexdigest

from deadline.job_attachments.asset_manifests import hash_algorithms
from deadline.job_attachments.asset_manifests.hash_algorithms import (
    HashAlgorithm,
    hash_data,
    hash_file,
)

READ_SIZE = hash_algorithms._HASH_READ_SIZE


@pytest.mark.parametrize(
    "size",
    [0, 1, READ_SIZE - 1, READ_SIZE, READ_SIZE + 1, 3 * READ_SIZE + 7],
)
def test_hash_file_matches_hash_of_contents(tmp_path, size):
    # GIVEN
    data = os.urandom(size)
    path = tmp_path / "file.bin"
    path.write_bytes(data)

    # WHEN
    result = hash_file(str(path), HashAlgorithm.XXH128)

    # THEN
    assert result == xxh3_128_hexdigest(data)
    assert result == hash_data(data, HashAlgorithm.XXH128)
