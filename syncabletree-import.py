#! /usr/bin/env python3

import os
import sys
import shutil
import subprocess
import json
from pathlib import Path

# Configurable path to remote root and local repo
REMOTE_ROOT = Path("/mnt/gdrive")
LOCAL_REPO = Path(".")  # Run script from the root of your repo
ANNEXMAP_FILE = REMOTE_ROOT / ".annexmap.json"


def load_annexmap():
    if ANNEXMAP_FILE.exists():
        with open(ANNEXMAP_FILE) as f:
            return json.load(f)
    else:
        return {}


def save_annexmap(mapping):
    with open(ANNEXMAP_FILE, "w") as f:
        json.dump(mapping, f, indent=2)


def find_non_annexed_files():
    annexmap = load_annexmap()
    known_paths = set(annexmap.values())

    non_annexed = []
    for root, dirs, files in os.walk(REMOTE_ROOT):
        for file in files:
            full_path = Path(root) / file
            rel_path = full_path.relative_to(REMOTE_ROOT)
            if not str(rel_path).startswith(".annexmap") and str(rel_path) not in known_paths:
                non_annexed.append(rel_path)

    return non_annexed


def auto_import_files(non_annexed):
    for rel_path in non_annexed:
        src = REMOTE_ROOT / rel_path
        dst = LOCAL_REPO / rel_path
        dst.parent.mkdir(parents=True, exist_ok=True)

        print(f"Importing {src} -> {dst}")
        shutil.copy2(src, dst)

        subprocess.run(["git", "annex", "add", str(dst)], check=True)

    subprocess.run(["git", "commit", "-m", f"Auto-imported {len(non_annexed)} file(s) from readabletree remote"], check=True)


def main():
    if len(sys.argv) > 1 and sys.argv[1] == "import-nonannexed":
        dry_run = "--dry-run" in sys.argv
        non_annexed = find_non_annexed_files()

        if not non_annexed:
            print("No new files to import.")
            return

        if dry_run:
            print("Files to import:")
            for path in non_annexed:
                print(f"  {path}")
        else:
            auto_import_files(non_annexed)

    else:
        print("Usage: syncabletree import-nonannexed [--dry-run]")


if __name__ == "__main__":
    main()
