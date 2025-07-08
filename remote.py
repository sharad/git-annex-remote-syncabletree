#!/usr/bin/env python3

import sys
import os
import subprocess
import json
from pathlib import Path
from backends.gdrive_backend import GDriveBackend

# Initialize backend
backend = GDriveBackend(folder_id=None, rclone_remote_name="gdrive")

def read_command():
    """Read the next command line from stdin."""
    return sys.stdin.readline().strip()


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

def log(msg):
    print(f"# {msg}", file=sys.stderr)

def send(msg):
    """Send message to git-annex."""
    sys.stdout.write(msg + "\n")
    sys.stdout.flush()


def send_ok():
    print("OK")
    sys.stdout.flush()

def send_error(msg):
    print(f"ERROR {msg}")
    sys.stdout.flush()

def send_value(val):
    print(f"VALUE {val}")
    sys.stdout.flush()

def handle_initremote(params):
    log(f"INITREMOTE {params}")
    send_ok()

def handle_prepare(params):
    log(f"PREPARE {params}")
    send_ok()

def handle_transfer(direction, key, size):
    log(f"TRANSFER {direction} {key} {size}")

    if direction == "STORE":
        # Read file content from stdin, save to temp, upload
        temp_path = f"/tmp/gitannex_{key}"
        with open(temp_path, "wb") as f:
            remaining = int(size)
            while remaining > 0:
                chunk = sys.stdin.buffer.read(min(65536, remaining))
                if not chunk:
                    break
                f.write(chunk)
                remaining -= len(chunk)
        log(f"Received file {temp_path}")

        # Optionally:
        # path = get_path_from_key(key)
        # backend.upload_file(temp_path, path or key)

        os.remove(temp_path)
        send_ok()

    elif direction == "RETRIEVE":
        # Download from backend, write to stdout
        # backend.download_file(key, temp_path)
        # with open(temp_path, "rb") as f:
        #     shutil.copyfileobj(f, sys.stdout.buffer)
        # os.remove(temp_path)
        send_ok()

    else:
        send_error("Unknown TRANSFER direction")



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



def handle_checkpresent(key):
    log(f"CHECKPRESENT {key}")
    # backend.has_key(key)
    # send_value("1") or send_value("0")
    send_value("1")
    send_ok()

def handle_checkpresent():
    key = read_command().split()[1]
    if backend.has_key(key):
        send("PRESENT")
    else:
        send("MISSING")
    send("OK")


def handle_remove(key):
    log(f"REMOVE {key}")
    # backend.remove_key(key)
    send_ok()

def handle_remove():
    key = read_command().split()[1]
    backend.remove_key(key)
    send("OK")

def handle_getcost():
    log("GETCOST")
    send_value("100")
    send_ok()

def handle_cost():
    send("COST 100")  # Medium cost
    send("OK")

def handle_getavailability():
    log("GETAVAILABILITY")
    send_value("globally-available")
    send_ok()

def handle_getinfo():
    log("GETINFO")
    # Send zero or more VALUE lines with info
    send_ok()

def handle_whereis(key):
    log(f"WHEREIS {key}")
    # backend-specific location reporting if desired
    send_ok()

def handle_listconfig():
    log("LISTCONFIG")
    # send_value("name=value")
    send_ok()

def handle_setconfig(params):
    log(f"SETCONFIG {params}")
    send_ok()

def handle_version():
    log("VERSION")
    send_value("1")
    send_ok()

def handle_getuuid():
    send("UUID 12345678-90ab-cdef-1234-567890abcdef")  # Replace with stable UUID if needed
    send("OK")

def main():
    while True:
        line = sys.stdin.readline()
        if not line:
            break
        line = line.strip()
        if not line:
            continue

        parts = line.split()
        if not parts:
            continue

        cmd = parts[0]

        if cmd == "INITREMOTE":
            handle_initremote(parts[1:])
        elif cmd == "PREPARE":
            handle_prepare(parts[1:])
        elif cmd == "TRANSFER":
            if len(parts) < 4:
                send_error("TRANSFER requires direction, key, size")
            else:
                handle_transfer(parts[1], parts[2], parts[3])
        elif cmd == "CHECKPRESENT":
            if len(parts) < 2:
                send_error("CHECKPRESENT requires key")
            else:
                handle_checkpresent(parts[1])
        elif cmd == "REMOVE":
            if len(parts) < 2:
                send_error("REMOVE requires key")
            else:
                handle_remove(parts[1])
        elif cmd == "GETCOST":
            handle_getcost()
        elif cmd == "GETAVAILABILITY":
            handle_getavailability()
        elif cmd == "GETINFO":
            handle_getinfo()
        elif cmd == "WHEREIS":
            if len(parts) < 2:
                send_error("WHEREIS requires key")
            else:
                handle_whereis(parts[1])
        elif cmd == "LISTCONFIG":
            handle_listconfig()
        elif cmd == "SETCONFIG":
            handle_setconfig(parts[1:])
        elif cmd == "VERSION":
            handle_version()
        elif cmd == "QUIT":
            log("QUIT received, exiting.")
            send_ok()
            break
        else:
            log(f"Unsupported command: {cmd}")
            print("UNSUPPORTED")
            sys.stdout.flush()

if __name__ == "__main__":
    main()



