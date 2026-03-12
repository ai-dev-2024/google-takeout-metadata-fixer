"""Resumable ADB copy helper.

Copies files from a local directory to an Android device over ADB, skipping
files that already exist on the device with matching size. Re-run the same
command after any disconnect to resume safely.
"""

import argparse
import os
import subprocess
import time
from pathlib import Path


def get_phone_files(dest_dir):
    """Get dict of {filename: size} for files already on phone."""
    print("Scanning phone for existing files...", flush=True)
    r = subprocess.run(
        ["adb", "shell", f'find "{dest_dir}" -maxdepth 1 -type f -exec stat -c "%s %n" {{}} +'],
        capture_output=True, text=True, timeout=120
    )
    phone_files = {}
    for line in r.stdout.strip().split("\n"):
        line = line.strip()
        if not line:
            continue
        parts = line.split(" ", 1)
        if len(parts) == 2:
            try:
                size = int(parts[0])
                name = parts[1].rsplit("/", 1)[-1]
                phone_files[name] = size
            except ValueError:
                continue
    print(f"  Found {len(phone_files)} files already on phone", flush=True)
    return phone_files


def get_pc_files(source_dir):
    """Get list of (filename, full_path, size) sorted largest first."""
    print("Scanning PC files...", flush=True)
    pc_files = []
    total = 0
    for f in os.scandir(source_dir):
        if f.is_file():
            sz = f.stat().st_size
            pc_files.append((f.name, f.path, sz))
            total += sz
    pc_files.sort(key=lambda x: -x[2])
    print(f"  Found {len(pc_files)} files ({format_size(total)})", flush=True)
    return pc_files, total


def push_file(src_path, dest_path):
    """Push a single file via adb. Returns True on success."""
    r = subprocess.run(
        ["adb", "push", src_path, dest_path],
        capture_output=True, text=True, timeout=1200
    )
    return r.returncode == 0


def format_size(b):
    if b >= 1 << 30:
        return f"{b / (1 << 30):.2f} GB"
    if b >= 1 << 20:
        return f"{b / (1 << 20):.1f} MB"
    return f"{b / (1 << 10):.0f} KB"


def format_time(seconds):
    if seconds < 60:
        return f"{seconds:.0f}s"
    if seconds < 3600:
        return f"{int(seconds // 60)}m {int(seconds % 60)}s"
    h = int(seconds // 3600)
    m = int((seconds % 3600) // 60)
    return f"{h}h {m}m"


def parse_args():
    parser = argparse.ArgumentParser(
        description="Resumable ADB copy from a local folder to an Android folder."
    )
    parser.add_argument(
        "--source-dir",
        default="./Output",
        help="Local directory containing files to copy. Default: ./Output",
    )
    parser.add_argument(
        "--dest-dir",
        default="/sdcard/Download/Output",
        help="Destination directory on the Android device. Default: /sdcard/Download/Output",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    source_dir = str(Path(args.source_dir).resolve())
    dest_dir = args.dest_dir

    print("=" * 60, flush=True)
    print("  RESUMABLE ADB COPY", flush=True)
    print("=" * 60, flush=True)

    subprocess.run(["adb", "shell", f'mkdir -p "{dest_dir}"'], capture_output=True, timeout=10)

    pc_files, pc_total = get_pc_files(source_dir)
    phone_files = get_phone_files(dest_dir)

    to_copy = []
    skipped_count = 0
    skipped_bytes = 0
    for name, path, size in pc_files:
        phone_size = phone_files.get(name)
        if phone_size is not None and phone_size == size:
            skipped_count += 1
            skipped_bytes += size
        else:
            to_copy.append((name, path, size))

    copy_bytes = sum(s for _, _, s in to_copy)

    print(f"\n  Total on PC:  {len(pc_files)} files ({format_size(pc_total)})", flush=True)
    print(f"  Already done: {skipped_count} files ({format_size(skipped_bytes)})", flush=True)
    print(f"  To copy:      {len(to_copy)} files ({format_size(copy_bytes)})", flush=True)
    print("=" * 60, flush=True)

    if not to_copy:
        print("\nAll files already on phone!", flush=True)
        return

    copied = 0
    copied_bytes = 0
    failed = []
    start_time = time.time()

    for i, (name, path, size) in enumerate(to_copy):
        overall_pct = ((skipped_bytes + copied_bytes) / pc_total) * 100
        elapsed = time.time() - start_time
        if copied_bytes > 0 and elapsed > 0:
            speed = copied_bytes / elapsed
            remaining = (copy_bytes - copied_bytes) / speed
            eta_str = f" | ETA: {format_time(remaining)} | {format_size(speed)}/s"
        else:
            eta_str = ""

        print(
            f"[{overall_pct:5.1f}%] ({i+1}/{len(to_copy)}) {name} ({format_size(size)}){eta_str}",
            flush=True
        )

        try:
            ok = push_file(path, f"{dest_dir}/{name}")
            if ok:
                copied += 1
                copied_bytes += size
            else:
                print(f"  FAILED: {name}", flush=True)
                failed.append(name)
        except subprocess.TimeoutExpired:
            print(f"  TIMEOUT: {name}", flush=True)
            failed.append(name)
        except Exception as e:
            print(f"  ERROR: {name}: {e}", flush=True)
            failed.append(name)

    elapsed = time.time() - start_time
    print(f"\n{'=' * 60}", flush=True)
    print(f"  Done in {format_time(elapsed)}", flush=True)
    print(f"  Copied:  {copied} files ({format_size(copied_bytes)})", flush=True)
    print(f"  Skipped: {skipped_count} (already on phone)", flush=True)
    print(f"  Failed:  {len(failed)}", flush=True)
    if failed:
        print(f"  Run this script again to retry failed files.", flush=True)
    print(f"{'=' * 60}", flush=True)


if __name__ == "__main__":
    main()
