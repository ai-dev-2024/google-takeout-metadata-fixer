#!/usr/bin/env python3
"""
Fix metadata for zip 3 by reading JSONs directly via adb exec-out + dd.

The phone's unzip has issues with this particular zip (some entries give
"Inconsistent information" errors). This script bypasses unzip entirely
by reading raw bytes from the zip using dd + adb exec-out, then parsing
the zip's central directory and local file headers to extract JSONs.

Speed optimization: instead of individual ADB calls per JSON (~770ms each),
this pushes a shell script to the phone that reads ALL JSONs in one shot,
reducing 10,000+ ADB calls to just 1.
"""

import json
import argparse
import os
import struct
import subprocess
import sys
import tempfile
import time
import zlib
from pathlib import Path

REMOTE_ZIP = "/sdcard/Download/takeout-20260310T064252Z-3-003.zip"
OUTPUT_DIR = Path("./Output")
PROGRESS_FILE = "fix_metadata_progress.json"
ZIP_NAME = "takeout-20260310T064252Z-3-003.zip"

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from google_takeout_metadata_fixer import (
    MEDIA_EXTENSIONS, EXIFTOOL_BATCH_SIZE,
    build_exiftool_args, find_json_match, parse_takeout_json,
    run_exiftool_batch, is_media, format_time, format_size,
    load_progress, save_progress,
)


def adb_read_bytes(offset, length):
    """Read bytes from REMOTE_ZIP via adb exec-out + dd."""
    if length <= 0:
        return b""
    bs = 4096
    skip_blocks = offset // bs
    skip_remainder = offset % bs
    count_blocks = (length + skip_remainder + bs - 1) // bs
    cmd = f'dd if="{REMOTE_ZIP}" bs={bs} skip={skip_blocks} count={count_blocks} 2>/dev/null'
    r = subprocess.run(["adb", "exec-out", cmd], capture_output=True, timeout=120)
    return r.stdout[skip_remainder:skip_remainder + length]


def find_eocd(tail_data, tail_offset):
    """Find End of Central Directory record."""
    sig = b"PK\x05\x06"
    idx = tail_data.rfind(sig)
    if idx < 0:
        return None, None

    eocd = tail_data[idx:idx + 22]
    if len(eocd) < 22:
        return None, None

    cd_size = struct.unpack_from("<I", eocd, 12)[0]
    cd_offset = struct.unpack_from("<I", eocd, 16)[0]

    if cd_offset == 0xFFFFFFFF or cd_size == 0xFFFFFFFF:
        # Zip64 — find locator
        sig64 = b"PK\x06\x07"
        idx64 = tail_data.rfind(sig64, 0, idx)
        if idx64 >= 0:
            locator = tail_data[idx64:idx64 + 20]
            if len(locator) >= 20:
                eocd64_offset = struct.unpack_from("<Q", locator, 8)[0]
                return None, eocd64_offset
        return None, None

    return cd_offset, cd_size


def parse_zip64_eocd(data):
    """Parse Zip64 EOCD record, return (cd_offset, cd_size)."""
    if len(data) < 56 or data[:4] != b"PK\x06\x06":
        return None, None
    cd_size = struct.unpack_from("<Q", data, 40)[0]
    cd_offset = struct.unpack_from("<Q", data, 48)[0]
    return cd_offset, cd_size


def parse_central_directory(cd_data):
    """Parse central directory. Returns list of dicts with name, local_offset, comp_size, uncomp_size, method, name_len."""
    entries = []
    pos = 0
    while pos + 46 <= len(cd_data):
        sig = struct.unpack_from("<I", cd_data, pos)[0]
        if sig != 0x02014B50:
            break

        method = struct.unpack_from("<H", cd_data, pos + 10)[0]
        comp_size = struct.unpack_from("<I", cd_data, pos + 20)[0]
        uncomp_size = struct.unpack_from("<I", cd_data, pos + 24)[0]
        name_len = struct.unpack_from("<H", cd_data, pos + 28)[0]
        extra_len = struct.unpack_from("<H", cd_data, pos + 30)[0]
        comment_len = struct.unpack_from("<H", cd_data, pos + 32)[0]
        local_offset = struct.unpack_from("<I", cd_data, pos + 42)[0]

        name_bytes = cd_data[pos + 46:pos + 46 + name_len]
        name = name_bytes.decode("utf-8", errors="replace")

        # Handle zip64 extra field
        if comp_size == 0xFFFFFFFF or uncomp_size == 0xFFFFFFFF or local_offset == 0xFFFFFFFF:
            extra_data = cd_data[pos + 46 + name_len:pos + 46 + name_len + extra_len]
            epos = 0
            while epos + 4 <= len(extra_data):
                eid = struct.unpack_from("<H", extra_data, epos)[0]
                esize = struct.unpack_from("<H", extra_data, epos + 2)[0]
                if eid == 0x0001:
                    field_offset = epos + 4
                    if uncomp_size == 0xFFFFFFFF and field_offset + 8 <= epos + 4 + esize:
                        uncomp_size = struct.unpack_from("<Q", extra_data, field_offset)[0]
                        field_offset += 8
                    if comp_size == 0xFFFFFFFF and field_offset + 8 <= epos + 4 + esize:
                        comp_size = struct.unpack_from("<Q", extra_data, field_offset)[0]
                        field_offset += 8
                    if local_offset == 0xFFFFFFFF and field_offset + 8 <= epos + 4 + esize:
                        local_offset = struct.unpack_from("<Q", extra_data, field_offset)[0]
                    break
                epos += 4 + esize

        entries.append({
            'name': name,
            'local_offset': local_offset,
            'comp_size': comp_size,
            'uncomp_size': uncomp_size,
            'method': method,
            'name_len': name_len,
        })

        pos += 46 + name_len + extra_len + comment_len

    return entries


def batch_extract_jsons(json_entries):
    """Extract ALL JSON files from the zip using a single ADB call.

    Pushes a shell script to the phone that reads all JSON data in sequence,
    then parses the concatenated output. This reduces 10,000+ ADB calls to 1.

    Returns dict of {name: parsed_json_data}.
    """
    print("  Generating batch read script...")

    # For each JSON, we need to read: local_file_header + compressed_data
    # Local header: 30 bytes fixed + name_len + extra_len (unknown)
    # We know name_len from CD. Extra_len in local header is usually <=256.
    # Read generously: 30 + name_len + 256 + comp_size bytes per entry.
    EXTRA_BUFFER = 256
    read_specs = []  # (name, local_offset, read_size, method, comp_size)
    for e in json_entries:
        read_size = 30 + e['name_len'] + EXTRA_BUFFER + e['comp_size']
        read_specs.append((e['name'], e['local_offset'], read_size, e['method'], e['comp_size']))

    # Generate shell script with all dd commands
    # Use bs=1 for exact positioning (shell script runs on phone, no ADB overhead per command)
    script_lines = [f'Z="{REMOTE_ZIP}"']
    for _, offset, read_size, _, _ in read_specs:
        script_lines.append(f'dd if="$Z" bs=1 skip={offset} count={read_size} 2>/dev/null')

    script_content = "\n".join(script_lines) + "\n"
    print(f"  Script: {len(json_entries)} dd commands ({len(script_content):,} bytes)")

    # Write script to temp file and push to phone
    script_path = os.path.join(tempfile.gettempdir(), "read_jsons.sh")
    with open(script_path, "w", newline="\n") as f:
        f.write(script_content)

    remote_script = "/data/local/tmp/read_jsons.sh"
    print("  Pushing script to phone...")
    r = subprocess.run(
        ["adb", "push", script_path, remote_script],
        capture_output=True, text=True, timeout=30,
    )
    if r.returncode != 0:
        # Try /sdcard/Download instead
        remote_script = "/sdcard/Download/read_jsons.sh"
        subprocess.run(
            ["adb", "push", script_path, remote_script],
            capture_output=True, text=True, timeout=30,
        )
    os.unlink(script_path)

    # Run the script — this is the ONE big ADB call
    print("  Running batch extraction (single ADB call for all JSONs)...")
    t0 = time.time()
    r = subprocess.run(
        ["adb", "exec-out", f"sh {remote_script}"],
        capture_output=True, timeout=3600,  # 1 hour max
    )
    raw_output = r.stdout
    elapsed = time.time() - t0
    print(f"  Got {format_size(len(raw_output))} in {format_time(elapsed)}")

    # Cleanup remote script
    subprocess.run(
        ["adb", "shell", f"rm -f {remote_script}"],
        capture_output=True, timeout=10,
    )

    # Parse the concatenated output
    print("  Parsing JSON payloads...")
    json_data_map = {}
    pos = 0
    ok_count = 0
    fail_count = 0

    for name, local_offset, read_size, method, comp_size in read_specs:
        if pos + 30 > len(raw_output):
            fail_count += 1
            pos += read_size  # Skip expected bytes
            continue

        chunk = raw_output[pos:pos + read_size]
        pos += read_size

        if len(chunk) < 30:
            fail_count += 1
            continue

        # Verify local file header signature
        if chunk[:4] != b"PK\x03\x04":
            fail_count += 1
            continue

        # Parse local file header to find data start
        local_name_len = struct.unpack_from("<H", chunk, 26)[0]
        local_extra_len = struct.unpack_from("<H", chunk, 28)[0]
        data_start = 30 + local_name_len + local_extra_len

        if data_start + comp_size > len(chunk):
            fail_count += 1
            continue

        comp_data = chunk[data_start:data_start + comp_size]

        # Decompress
        if method == 0:  # stored
            raw_json = comp_data
        elif method == 8:  # deflated
            try:
                raw_json = zlib.decompress(comp_data, -15)
            except zlib.error:
                fail_count += 1
                continue
        else:
            fail_count += 1
            continue

        try:
            data = json.loads(raw_json.decode("utf-8", errors="replace"))
            json_data_map[name] = data
            ok_count += 1
        except (json.JSONDecodeError, UnicodeDecodeError):
            fail_count += 1

    print(f"  Extracted {ok_count:,} JSONs, {fail_count:,} failed")
    return json_data_map


def parse_args():
    parser = argparse.ArgumentParser(
        description=(
            "Fallback metadata fixer for a problematic Google Takeout zip when "
            "the device unzip listing is unreliable."
        )
    )
    parser.add_argument(
        "--remote-zip",
        default=REMOTE_ZIP,
        help="Path to the zip on the Android device.",
    )
    parser.add_argument(
        "--output",
        default="./Output",
        help="Directory containing the already extracted media files.",
    )
    return parser.parse_args()


def main():
    global REMOTE_ZIP, OUTPUT_DIR, ZIP_NAME
    args = parse_args()
    REMOTE_ZIP = args.remote_zip
    OUTPUT_DIR = Path(args.output).resolve()
    ZIP_NAME = Path(REMOTE_ZIP).name

    print("=" * 65)
    print("  Takeout Zip Metadata Recovery (batch binary read via ADB)")
    print("=" * 65)

    # Verify ADB
    print("\nChecking ADB...")
    r = subprocess.run(["adb", "devices"], capture_output=True, text=True, timeout=10)
    devices = [l for l in r.stdout.strip().split("\n")[1:] if "device" in l and "offline" not in l]
    if not devices:
        print("ERROR: No ADB device found!")
        sys.exit(1)
    print(f"  Device: {devices[0].split()[0]}")

    # Get zip size
    r = subprocess.run(
        ["adb", "shell", f'stat -c %s "{REMOTE_ZIP}"'],
        capture_output=True, text=True, timeout=10,
    )
    zip_size = int(r.stdout.strip())
    print(f"  Zip size: {format_size(zip_size)} ({zip_size:,} bytes)")

    # Step 1: Find EOCD
    print("\nStep 1: Finding central directory...")
    tail_size = 1024 * 1024
    tail_offset = zip_size - tail_size
    tail_data = adb_read_bytes(tail_offset, tail_size)

    cd_offset, cd_size_or_eocd64 = find_eocd(tail_data, tail_offset)

    if cd_offset is None and cd_size_or_eocd64 is not None:
        print(f"  Zip64 EOCD at offset {cd_size_or_eocd64:,}")
        eocd64_data = adb_read_bytes(cd_size_or_eocd64, 56)
        cd_offset, cd_size = parse_zip64_eocd(eocd64_data)
        if cd_offset is None:
            print("ERROR: Could not parse Zip64 EOCD")
            sys.exit(1)
    elif cd_offset is not None:
        cd_size = cd_size_or_eocd64
    else:
        print("ERROR: Could not find EOCD")
        sys.exit(1)

    print(f"  Central directory: {format_size(cd_size)} at offset {cd_offset:,}")

    # Step 2: Read and parse central directory
    print("\nStep 2: Reading central directory...")
    cd_data = b""
    chunk_size = 4 * 1024 * 1024
    bytes_read = 0
    while bytes_read < cd_size:
        read_size = min(chunk_size, cd_size - bytes_read)
        chunk = adb_read_bytes(cd_offset + bytes_read, read_size)
        if not chunk:
            break
        cd_data += chunk
        bytes_read += len(chunk)
    print(f"  Got {format_size(len(cd_data))}")

    print("  Parsing entries...")
    all_entries = parse_central_directory(cd_data)
    print(f"  {len(all_entries):,} total entries")

    json_entries = [e for e in all_entries if e['name'].endswith('.json')]
    media_entries = [e for e in all_entries if is_media(e['name']) and not e['name'].endswith('/')]
    print(f"  {len(json_entries):,} JSON sidecars, {len(media_entries):,} media files")

    nameset = set(e['name'] for e in all_entries)

    # Step 3: Index output files
    print(f"\nStep 3: Indexing output files...")
    output_index = {}
    for f in OUTPUT_DIR.iterdir():
        if f.is_file() and is_media(f.name):
            output_index.setdefault(f.name, []).append(f)
    print(f"  {sum(len(v) for v in output_index.values()):,} media files indexed")

    # Step 4: Batch extract all JSONs (single ADB call!)
    print(f"\nStep 4: Batch extracting {len(json_entries):,} JSON sidecars...")
    json_data_map = batch_extract_jsons(json_entries)

    # Build title index
    json_title_index = {}
    for json_path, data in json_data_map.items():
        title = data.get("title")
        if title and isinstance(title, str) and title not in json_title_index:
            json_title_index[title] = json_path

    # Step 5: Load progress
    progress = load_progress()
    processed_set = set(progress.get("processed_entries", []))

    # Step 6: Process media files
    print(f"\nStep 5: Processing {len(media_entries):,} media files...")
    start_time = time.time()

    stats = {'fixed': 0, 'no_json': 0, 'no_match': 0, 'no_meta': 0, 'err': 0, 'skip': 0}
    exiftool_batch = []

    for i, media_e in enumerate(media_entries, 1):
        entry_key = f"{ZIP_NAME}::{media_e['name']}"

        if entry_key in processed_set:
            stats['skip'] += 1
            continue

        filename = Path(media_e['name']).name

        # Find output file
        candidates = output_index.get(filename, [])
        if not candidates:
            stem = Path(filename).stem
            suffix = Path(filename).suffix
            for n in range(1, 20):
                alt = f"{stem}_{n}{suffix}"
                if alt in output_index:
                    candidates = output_index[alt]
                    break
        if not candidates:
            stats['no_match'] += 1
            processed_set.add(entry_key)
            continue

        # Find JSON match
        json_name = find_json_match(nameset, media_e['name'], json_title_index)
        if not json_name:
            stats['no_json'] += 1
            processed_set.add(entry_key)
            continue

        json_data = json_data_map.get(json_name)
        if json_data is None:
            stats['no_json'] += 1
            processed_set.add(entry_key)
            continue

        # Parse metadata
        metadata = parse_takeout_json(json_data)
        if not metadata:
            stats['no_meta'] += 1
            processed_set.add(entry_key)
            continue

        target = candidates[0]
        et_args = build_exiftool_args(metadata, target)
        if not et_args:
            stats['no_meta'] += 1
            processed_set.add(entry_key)
            continue

        exiftool_batch.append((target, et_args, entry_key))

        if len(exiftool_batch) >= EXIFTOOL_BATCH_SIZE:
            ok, err = run_exiftool_batch([(fp, a) for fp, a, _ in exiftool_batch])
            stats['fixed'] += ok
            stats['err'] += err
            for _, _, ek in exiftool_batch:
                processed_set.add(ek)
            exiftool_batch = []

            progress['processed_entries'] = list(processed_set)
            try:
                save_progress(progress)
            except (PermissionError, OSError):
                pass  # Will retry on next batch
            elapsed = time.time() - start_time
            rate = i / elapsed if elapsed > 0 else 0
            eta = (len(media_entries) - i) / rate if rate > 0 else 0
            print(
                f"    [{i}/{len(media_entries)}] "
                f"fixed={stats['fixed']} no_json={stats['no_json']} "
                f"skip={stats['skip']} err={stats['err']} "
                f"| {rate:.0f}/s | ETA: {format_time(eta)}",
                flush=True,
            )

    # Flush remaining batch
    if exiftool_batch:
        ok, err = run_exiftool_batch([(fp, a) for fp, a, _ in exiftool_batch])
        stats['fixed'] += ok
        stats['err'] += err
        for _, _, ek in exiftool_batch:
            processed_set.add(ek)

    # Save final progress
    progress['processed_entries'] = list(processed_set)
    if ZIP_NAME not in progress.get('completed_zips', []):
        progress.setdefault('completed_zips', []).append(ZIP_NAME)
    save_progress(progress)

    elapsed = time.time() - start_time
    print(f"\n{'=' * 65}")
    print(f"  ZIP 3 METADATA FIX COMPLETE ({format_time(elapsed)})")
    print(f"{'=' * 65}")
    print(f"  Fixed:      {stats['fixed']}")
    print(f"  Skipped:    {stats['skip']}")
    print(f"  No JSON:    {stats['no_json']}")
    print(f"  No match:   {stats['no_match']}")
    print(f"  No meta:    {stats['no_meta']}")
    print(f"  Errors:     {stats['err']}")
    print(f"  JSON cache: {len(json_data_map)} unique JSONs extracted")
    print(f"{'=' * 65}")


if __name__ == "__main__":
    main()
