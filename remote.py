#!/usr/bin/env python3

import sys
import os
import subprocess

def log(msg):
    print(f"# {msg}", file=sys.stderr)

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

def handle_checkpresent(key):
    log(f"CHECKPRESENT {key}")
    # backend.has_key(key)
    # send_value("1") or send_value("0")
    send_value("1")
    send_ok()

def handle_remove(key):
    log(f"REMOVE {key}")
    # backend.remove_key(key)
    send_ok()

def handle_getcost():
    log("GETCOST")
    send_value("100")
    send_ok()

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



