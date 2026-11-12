# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

import os

from deadline.job_attachments._diff import _fast_file_list_to_manifest_diff
from deadline.job_attachments.asset_manifests.decode import decode_manifest
from deadline.job_attachments.models import FileStatus


def _manifest(entries):
    paths = ",".join(
        f'{{"hash":"{"0" * 32}","mtime":{mtime},"path":"{path}","size":{size}}}'
        for path, size, mtime in sorted(entries)
    )
    total = sum(size for _, size, _ in entries)
    return decode_manifest(
        f'{{"hashAlg":"xxh128","manifestVersion":"2023-03-03","paths":[{paths}],"totalSize":{total}}}'
    )


def test_fast_diff_reports_new_modified_unchanged_and_deleted(tmp_path):
    # GIVEN
    root = tmp_path / "root"
    (root / "out").mkdir(parents=True)
    unchanged = root / "out" / "unchanged.exr"
    unchanged.write_bytes(b"1234")
    modified = root / "out" / "modified.exr"
    modified.write_bytes(b"123456")
    new = root / "out" / "new.exr"
    new.write_bytes(b"x")
    unchanged_mtime = unchanged.stat().st_mtime_ns // 1000
    modified_mtime = modified.stat().st_mtime_ns // 1000
    manifest = _manifest(
        [
            ("out/unchanged.exr", 4, unchanged_mtime),
            ("out/modified.exr", 2, modified_mtime),
            ("out/deleted.exr", 3, 1700000000000000),
        ]
    )

    # WHEN
    result = _fast_file_list_to_manifest_diff(
        root=str(root),
        current_files=[str(unchanged), str(modified), str(new)],
        diff_manifest=manifest,
    )

    # THEN
    assert sorted(result) == sorted(
        [
            ("out/modified.exr", FileStatus.MODIFIED),
            ("out/new.exr", FileStatus.NEW),
            ("out/deleted.exr", FileStatus.DELETED),
        ]
    )


def test_fast_diff_full_paths_for_deleted_files(tmp_path):
    # GIVEN
    root = tmp_path / "root"
    root.mkdir()
    manifest = _manifest([("gone.txt", 1, 1700000000000000)])

    # WHEN
    result = _fast_file_list_to_manifest_diff(
        root=str(root), current_files=[], diff_manifest=manifest, return_root_relative_path=False
    )

    # THEN
    assert result == [(os.path.join(str(root), "gone.txt"), FileStatus.DELETED)]
