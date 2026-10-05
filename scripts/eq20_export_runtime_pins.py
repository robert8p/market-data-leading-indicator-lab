"""Bind the export repair into the reviewed continuation supervisor image.

The continuation source includes its separately reviewed timeout diagnostics.
This build step changes only its two private-export metadata pins and binds the
full-population constructor to that exact guard image. Complete input and image
hashes are checked for both files, preserving their implementations. Private
source stays in the existing authenticated database bundle.
"""
from __future__ import annotations
import hashlib
from pathlib import Path

BASE_SHA256 = '6a67b00323cc74178aab3deb045ac767a05c4b4d919bb383650379988eca472b'
IMAGE_SHA256 = 'fd854c7827124165ea71a1d6018ae054ceb46c5f86551b9322897b82b28c4a0f'
CONSTRUCTOR_BASE_SHA256 = 'a488a7abf96c8913897804d5929adccb0539441a9b7b44aada4fca8c66fd1acc'
CONSTRUCTOR_IMAGE_SHA256 = 'ab8ed5fdfc8a41fa2f16ef234758458fd3c70616065426098b2f3e31e0a6a991'
OLD_BUNDLE = '5d10e1c4e733fb0e2a107fa88a8981cee8f792cbe65c0d8c3575bc817823d605'
NEW_BUNDLE = '539811dd4fa112cbd75d1e291948f89fc92b0e7589138d62df05a27e3071cbe6'
OLD_WORKER = "'w10_source_worker_v2.py': (84782, '5a250f117979f4d0c8310291302f97e8ffa8c44f74fc47b8cc0cd0db84004a90')"
NEW_WORKER = "'w10_source_worker_v2.py': (100279, 'c0e97c6988fb92f17870b285deefa1cdf43bcdde6ceca98e19a33a330dc3bb25')"


def patched_bytes(original: bytes) -> bytes:
    if hashlib.sha256(original).hexdigest() != BASE_SHA256:
        raise ValueError('EQ20_EXPORT_EXACT_PREDECESSOR_WRAPPER_REQUIRED')
    text = original.decode('utf-8')
    old_assignment = "PRIVATE_BUNDLE_SHA256 = '" + OLD_BUNDLE + "'"
    new_assignment = "PRIVATE_BUNDLE_SHA256 = '" + NEW_BUNDLE + "'"
    if text.count(old_assignment) != 1 or text.count(OLD_WORKER) != 1:
        raise ValueError('EQ20_EXPORT_EXACT_TWO_PIN_LOCATIONS_REQUIRED')
    updated = text.replace(old_assignment, new_assignment, 1).replace(OLD_WORKER, NEW_WORKER, 1)
    if updated.replace(new_assignment, old_assignment, 1).replace(NEW_WORKER, OLD_WORKER, 1) != text:
        raise ValueError('EQ20_EXPORT_NON_PIN_CHANGE_REJECTED')
    compile(updated, 'eq20_source_supervisor.py', 'exec')
    raw = updated.encode('utf-8')
    if hashlib.sha256(raw).hexdigest() != IMAGE_SHA256:
        raise ValueError('EQ20_EXPORT_EXACT_IMAGE_WRAPPER_REQUIRED')
    return raw


def patched_constructor_bytes(original: bytes) -> bytes:
    if hashlib.sha256(original).hexdigest() != CONSTRUCTOR_BASE_SHA256:
        raise ValueError('EQ20_EXPORT_EXACT_PREDECESSOR_CONSTRUCTOR_REQUIRED')
    text = original.decode('utf-8')
    old_assignment = "PROCESS_GUARDS_SHA256 = '" + BASE_SHA256 + "'"
    new_assignment = "PROCESS_GUARDS_SHA256 = '" + IMAGE_SHA256 + "'"
    if text.count(old_assignment) != 1:
        raise ValueError('EQ20_EXPORT_EXACT_CONSTRUCTOR_GUARD_PIN_REQUIRED')
    updated = text.replace(old_assignment, new_assignment, 1)
    if updated.replace(new_assignment, old_assignment, 1) != text:
        raise ValueError('EQ20_EXPORT_CONSTRUCTOR_NON_PIN_CHANGE_REJECTED')
    compile(updated, 'eq20_full_population_source.py', 'exec')
    raw = updated.encode('utf-8')
    if hashlib.sha256(raw).hexdigest() != CONSTRUCTOR_IMAGE_SHA256:
        raise ValueError('EQ20_EXPORT_EXACT_CONSTRUCTOR_IMAGE_REQUIRED')
    return raw


def main() -> None:
    app = Path(__file__).resolve().parents[1] / 'app'
    source_path = app / 'eq20_source_supervisor.py'
    constructor_path = app / 'eq20_full_population_source.py'
    # Validate both original files before either build output is written.
    source_image = patched_bytes(source_path.read_bytes())
    constructor_image = patched_constructor_bytes(constructor_path.read_bytes())
    for path, updated in ((source_path, source_image), (constructor_path, constructor_image)):
        path.write_bytes(updated)
        if path.read_bytes() != updated:
            raise ValueError('EQ20_EXPORT_IMAGE_BUILD_READBACK_REJECTED')
    print('EQ20_EXPORT_PUBLIC_WRAPPER_SHA256=' + hashlib.sha256(source_image).hexdigest())
    print('EQ20_FULL_POPULATION_SOURCE_IMAGE_SHA256=' + hashlib.sha256(constructor_image).hexdigest())


if __name__ == '__main__':
    main()
