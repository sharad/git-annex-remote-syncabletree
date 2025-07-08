#!/usr/bin/env python3

import sys
import json
from pathlib import Path
from backends.gdrive_backend import GDriveBackend

# Initialize backend
backend = GDriveBackend(folder_id=None, rclone_remote_name="gdrive")

def read_command():
    """Read the next command line from stdin."""
    return sys.stdin.readline().strip()

def send(msg):
    """Send message to git-annex."""
    sys.stdout.write(msg + "\n")
    sys.stdout.flush()

def handle_initremote():
    send("OK")

def handle_prepare():
    send("OK")

def handle_getuuid():
    send("UUID 12345678-90ab-cdef-1234-567890abcdef")  # Replace with stable UUID if needed
    send("OK")

def handle_cost():
    send("COST 100")  # Medium cost
    send("OK")

def handle_checkpresent():
    key = read_command().split()[1]
    if backend.has_key(key):
        send("PRESENT")
    else:
        send("MISSING")
    send("OK")

def handle_remove():
    key = read_command().split()[1]
    backend.remove_key(key)
    send("OK")

def get_path_from_key(key):
    """Fetch readable path from annex for the key, fallback to key."""
    from subprocess import check_output, CalledProcessError
    try:
        out = check_output(["git", "annex", "find", "--key", key, "--format=${file}\n"], text=True).strip()
        if out:
            return out.splitlines()[0]
    except CalledProcessError:
        pass
    return key

def handle_transfer():
    parts = read_command().split()
    direction = parts[1]
    key = parts[2]
    size = int(parts[3])
    temp_path = parts[4]

    if direction == "STORE":
        readable_path = get_path_from_key(key)
        backend.upload_file(temp_path, readable_path, key)
    elif direction == "RETRIEVE":
        backend.download_file(key, temp_path)
    send("OK")

def main():
    while True:
        cmd = read_command()
        if not cmd:
            break
        if cmd == "INITREMOTE":
            handle_initremote()
        elif cmd == "PREPARE":
            handle_prepare()
        elif cmd == "GETUUID":
            handle_getuuid()
        elif cmd == "COST":
            handle_cost()
        elif cmd.startswith("CHECKPRESENT"):
            handle_checkpresent()
        elif cmd.startswith("REMOVE"):
            handle_remove()
        elif cmd.startswith("TRANSFER"):
            handle_transfer()
        elif cmd == "EXIT":
            break
        else:
            # Ignore unsupported commands gracefully
            send("UNSUPPORTED")
            send("OK")

if __name__ == "__main__":
    main()


