# remote.py - Entry point for syncabletree custom remote (supports multiple backends)

import sys
import os
import importlib
from syncabletree import SyncableTreeRemote

# Read git-annex startup parameters from stdin

def parse_annex_options():
    options = {}
    for line in sys.stdin:
        line = line.strip()
        if line == '':
            break
        if '=' in line:
            k, v = line.split('=', 1)
            options[k.strip()] = v.strip()
    return options


def load_backend(options):
    backend_name = options.get("backend")
    mock = options.get("mock", "false").lower() == "true"

    if not backend_name:
        raise ValueError("Missing required 'backend' option (e.g. backend=s3 or backend=gdrive)")

    if backend_name.lower() == "s3":
        if mock:
            from backends.s3_backend_mock import S3BackendMock
            return S3BackendMock(bucket_name=options.get("bucket", "mock-annex-bucket"))
        else:
            from backends.s3_backend import S3Backend
            return S3Backend(bucket_name=options.get("bucket", "annex"))

    elif backend_name.lower() == "gdrive":
        if mock:
            from backends.gdrive_backend_mock import GDriveBackendMock
            return GDriveBackendMock(drive_name=options.get("drive", "mock-drive"))
        else:
            from backends.gdrive_backend import GDriveBackend
            return GDriveBackend(drive_name=options.get("drive", "gdrive"))

    else:
        raise ValueError(f"Unsupported backend: {backend_name}")


def main():
    options = parse_annex_options()
    backend = load_backend(options)
    remote = SyncableTreeRemote(backend)
    remote.run()


if __name__ == "__main__":
    main()





## Examples

# git annex initremote syncabletree \
#   type=external \
#   externaltype=syncabletree \
#   backend=gdrive mock=true drive=mock-drive \
#   encryption=none


# For real S3
# git annex initremote syncabletree \
#   type=external \
#   externaltype=syncabletree \
#   backend=s3 mock=false bucket=my-annex-bucket \
#   encryption=none

