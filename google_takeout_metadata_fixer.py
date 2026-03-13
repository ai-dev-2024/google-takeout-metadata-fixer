#!/usr/bin/env python3
"""
Google Takeout Metadata Fixer
=============================
GitHub-ready Google Takeout / Google Photos Takeout metadata fixer.

The most comprehensive tool for recovering metadata from Google Takeout exports.
Embeds dates, GPS coordinates, descriptions, and people tags directly into your
photos and videos using ExifTool — so they sort correctly everywhere.

Two modes:
  ADB mode:   Streams JSON sidecars from zips on your Android phone via USB.
              Uses virtually ZERO extra disk space.
  Local mode: Reads JSON sidecars from local zip files on your computer.

Features:
  - Handles Google's new 'supplemental-metadata' JSON naming format
  - Handles truncated suffixes (e.g. .supple.json, .supplemental-metad.json)
  - Handles duplicate files: photo(2).jpg -> photo.jpg.supplemental-metadata(2).json
  - Handles -edited files in 12+ languages (English, German, French, etc.)
  - Handles live photos: video.mp4 -> photo.jpg's JSON
  - Falls back to JSON 'title' field matching when filename matching fails
  - Falls back to 'creationTime' when 'photoTakenTime' is missing
  - Writes: DateTimeOriginal, CreateDate, ModifyDate, GPS coordinates,
    descriptions, people tags, and video-specific tags
  - Batched ExifTool calls for speed (~50 files per invocation)
  - Resume support — safely restarts from where it left off

Requirements:
  - Python 3.9+
  - ExifTool (https://exiftool.org) on PATH
  - ADB on PATH (only for ADB mode — phone connected via USB)

Usage:
  # ADB mode (zips on phone):
    python google_takeout_metadata_fixer.py --output ./Output --phone-dir /sdcard/Download

  # Local mode (zips on computer):
    python google_takeout_metadata_fixer.py --output ./Output --local-zips ./takeout-zips/

  # Dry run (preview without modifying files):
    python google_takeout_metadata_fixer.py --output ./Output --local-zips ./zips/ --dry-run
"""

import argparse
import json
import os
import re
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

# ---------------------------------------------------------------------------
# CONFIGURATION
# ---------------------------------------------------------------------------

MEDIA_EXTENSIONS = {
    ".jpg", ".jpeg", ".png", ".gif", ".bmp", ".tiff", ".tif",
    ".webp", ".heic", ".heif", ".raw", ".cr2", ".nef", ".arw",
    ".mp4", ".mov", ".avi", ".mkv", ".3gp", ".m4v", ".webm",
}

VIDEO_EXTENSIONS = {".mp4", ".mov", ".avi", ".mkv", ".3gp", ".m4v", ".webm"}

EXIFTOOL_BATCH_SIZE = 50   # files per exiftool invocation
PROGRESS_FILE = "fix_metadata_progress.json"

# ---------------------------------------------------------------------------
# UTILITIES
# ---------------------------------------------------------------------------

def is_media(name):
    return Path(name).suffix.lower() in MEDIA_EXTENSIONS


def format_size(nbytes):
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if abs(nbytes) < 1024:
            return f"{nbytes:.1f} {unit}"
        nbytes /= 1024
    return f"{nbytes:.1f} PB"


def format_time(seconds):
    if seconds < 60:
        return f"{int(seconds)}s"
    if seconds < 3600:
        return f"{int(seconds // 60)}m {int(seconds % 60)}s"
    h = int(seconds // 3600)
    m = int((seconds % 3600) // 60)
    return f"{h}h {m}m"


def load_progress():
    if os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE, "r") as f:
            return json.load(f)
    return {"completed_zips": [], "processed_entries": []}


def save_progress(progress):
    for attempt in range(5):
        try:
            tmp = PROGRESS_FILE + ".tmp"
            with open(tmp, "w") as f:
                json.dump(progress, f)
            os.replace(tmp, PROGRESS_FILE)
            return
        except PermissionError:
            time.sleep(1)
    with open(PROGRESS_FILE, "w") as f:
        json.dump(progress, f)


# ---------------------------------------------------------------------------
# JSON SIDECAR MATCHING (the core challenge)
#
# Google Takeout generates JSON sidecars with inconsistent naming:
#   - photo.jpg.supplemental-metadata.json        (new format, standard)
#   - photo.jpg.supplemental-metad.json            (new format, truncated)
#   - photo.jpg.supplemental-metadata(1).json      (duplicate index)
#   - photo.jpg.json                                (old format)
#   - photo.json                                    (old format, stem only)
#   - photo.jpg(1).json                             (old bracket format)
#   - photo(2).jpg -> photo.jpg.supplemental-metadata(2).json  (Google dups)
#   - photo-edited.jpg -> photo.jpg's JSON          (edited variants)
#   - video.mp4 -> photo.jpg's JSON                 (live photos)
# ---------------------------------------------------------------------------

def find_json_match(nameset, media_entry, json_title_index=None):
    """Find the JSON sidecar for a media file within a zip's namelist.

    Args:
        nameset: set of all filenames in the zip
        media_entry: full path of the media file in the zip
        json_title_index: dict mapping JSON 'title' field -> JSON entry path
                         (optional, for title-based fallback matching)

    Returns:
        The matching JSON entry path, or None.
    """
    p_media = media_entry.rsplit("/", 1)
    parent = p_media[0] + "/" if len(p_media) > 1 else ""
    basename = p_media[-1]
    stem = Path(basename).stem
    ext = Path(basename).suffix

    # --- Rule 1: supplemental-metadata (new format) ---
    c = f"{media_entry}.supplemental-metadata.json"
    if c in nameset:
        return c

    # --- Rule 2: supplemental-metadata with duplicate index ---
    for i in range(1, 20):
        c = f"{media_entry}.supplemental-metadata({i}).json"
        if c in nameset:
            return c

    # --- Rule 3: Truncated supplemental-metadata ---
    # Google truncates the JSON filename to a max length, cutting into
    # ".supplemental-metadata" at any point. We match by prefix.
    prefix = f"{media_entry}.suppl"
    for name in nameset:
        if name.startswith(prefix) and name.endswith(".json"):
            return name

    # Also try with just "." prefix for extreme truncation (.s.json, ..json)
    prefix_dot = f"{media_entry}."
    for name in nameset:
        if (name.startswith(prefix_dot) and name.endswith(".json")
                and name != f"{media_entry}.json"):  # don't match old format yet
            inner = name[len(prefix_dot):-5]  # between "photo.jpg." and ".json"
            if inner and ".supplemental-metadata"[:len(inner)] == inner.lower():
                return name

    # --- Rule 4: Google duplicate files ---
    # file(N).ext -> file.ext.supplemental-metadata(N).json
    dup_match = re.match(r'^(.+?)\((\d+)\)(\.[^.]+)$', basename)
    if dup_match:
        orig_name = dup_match.group(1) + dup_match.group(3)
        dup_idx = dup_match.group(2)
        orig_entry = parent + orig_name
        # Try: original.ext.supplemental-metadata(N).json
        c = f"{orig_entry}.supplemental-metadata({dup_idx}).json"
        if c in nameset:
            return c
        # Try: original.ext.supplemental-metadata.json (shared metadata)
        c = f"{orig_entry}.supplemental-metadata.json"
        if c in nameset:
            return c
        # Try other indices
        for i in range(1, 20):
            c = f"{orig_entry}.supplemental-metadata({i}).json"
            if c in nameset:
                return c
        # Truncated version for the original name
        prefix = f"{orig_entry}.suppl"
        for name in nameset:
            if name.startswith(prefix) and name.endswith(".json"):
                return name
        # Old format for the original
        c = f"{orig_entry}.json"
        if c in nameset:
            return c
        orig_stem = Path(orig_name).stem
        c = parent + orig_stem + ".json"
        if c in nameset:
            return c

    # --- Rule 5: Old format (pre-2024 exports) ---
    c = media_entry + ".json"
    if c in nameset:
        return c

    # Old format: stem only
    c = parent + stem + ".json"
    if c in nameset:
        return c

    # Old bracket variations: photo.jpg(1).json
    for i in range(1, 10):
        c = f"{media_entry}({i}).json"
        if c in nameset:
            return c

    # --- Rule 6: Edited files (12+ languages) ---
    # photo-edited.jpg -> photo.jpg.supplemental-metadata.json
    # Covers: -edited (EN), -bearbeitet (DE), -modifié (FR), -editado (ES/PT),
    #         -modificato (IT), -bewerkt (NL), -edytowany (PL), -redigerad (SV),
    #         -muokattu (FI), -düzenlendi (TR), -отредактировано (RU), -編集済み (JA)
    edited_match = re.match(
        r'^(.+)-(?:edited|bearbeitet|modifi[eé]|editado|modificato|bewerkt|'
        r'edytowany|redigerad|muokattu|d[uü]zenlendi|отредактировано|編集済み)'
        r'(\.[^.]+)$', basename, re.IGNORECASE
    )
    if edited_match:
        orig_name = edited_match.group(1) + edited_match.group(2)
        orig_entry = parent + orig_name
        c = f"{orig_entry}.supplemental-metadata.json"
        if c in nameset:
            return c
        c = orig_entry + ".json"
        if c in nameset:
            return c
        prefix = f"{orig_entry}.suppl"
        for name in nameset:
            if name.startswith(prefix) and name.endswith(".json"):
                return name

    # --- Rule 7: Live photos ---
    # video.mp4 -> matching still image's JSON
    ext_lower = ext.lower()
    if ext_lower in ('.mp4', '.mov'):
        for photo_ext in ('.jpg', '.jpeg', '.heic', '.png',
                          '.JPG', '.JPEG', '.HEIC', '.PNG'):
            photo_entry = parent + stem + photo_ext
            c = f"{photo_entry}.supplemental-metadata.json"
            if c in nameset:
                return c
            c = photo_entry + ".json"
            if c in nameset:
                return c
            prefix = f"{photo_entry}.suppl"
            for name in nameset:
                if name.startswith(prefix) and name.endswith(".json"):
                    return name

    # --- Rule 8: JSON title field matching ---
    # Some JSONs have a 'title' field that matches the media filename.
    if json_title_index and basename in json_title_index:
        return json_title_index[basename]

    return None


# ---------------------------------------------------------------------------
# METADATA PARSING
# ---------------------------------------------------------------------------

def parse_takeout_json(data):
    """Extract all usable metadata from a Google Takeout JSON sidecar.

    Returns dict with keys: datetime, latitude, longitude, altitude,
    description, people, title, favorited
    """
    metadata = {}

    # Timestamp: prefer photoTakenTime, fall back to creationTime
    for time_key in ("photoTakenTime", "creationTime"):
        if time_key in data and "datetime" not in metadata:
            ts_str = data[time_key].get("timestamp", "0")
            try:
                ts = int(ts_str)
            except (ValueError, TypeError):
                continue
            if ts > 0:
                dt = datetime.fromtimestamp(ts, tz=timezone.utc)
                metadata["datetime"] = dt.strftime("%Y:%m:%d %H:%M:%S")

    # GPS coordinates
    for geo_key in ("geoData", "geoDataExif"):
        if geo_key in data and "latitude" not in metadata:
            geo = data[geo_key]
            lat = geo.get("latitude", 0)
            lng = geo.get("longitude", 0)
            if lat != 0 or lng != 0:
                metadata["latitude"] = lat
                metadata["longitude"] = lng
                metadata["altitude"] = geo.get("altitude", 0)

    # Description
    desc = data.get("description", "")
    if desc and desc.strip():
        metadata["description"] = desc.strip()

    # People tags
    people = data.get("people", [])
    if people:
        names = [p.get("name", "") for p in people if p.get("name")]
        if names:
            metadata["people"] = names

    # Favorite status
    if data.get("favorited"):
        metadata["favorited"] = True

    # Title (for reference)
    if data.get("title"):
        metadata["title"] = data["title"]

    return metadata


# ---------------------------------------------------------------------------
# EXIFTOOL COMMAND BUILDING
# ---------------------------------------------------------------------------

def build_exiftool_args(metadata, filepath):
    """Build exiftool arguments to embed all metadata into a file."""
    args = []
    is_video = Path(filepath).suffix.lower() in VIDEO_EXTENSIONS

    if "datetime" in metadata:
        dt = metadata["datetime"]
        args.extend([
            f"-DateTimeOriginal={dt}",
            f"-CreateDate={dt}",
            f"-ModifyDate={dt}",
        ])
        if is_video:
            args.extend([
                f"-TrackCreateDate={dt}",
                f"-TrackModifyDate={dt}",
                f"-MediaCreateDate={dt}",
                f"-MediaModifyDate={dt}",
            ])

    if "latitude" in metadata:
        lat, lng = metadata["latitude"], metadata["longitude"]
        args.extend([
            f"-GPSLatitude={abs(lat)}",
            f"-GPSLatitudeRef={'N' if lat >= 0 else 'S'}",
            f"-GPSLongitude={abs(lng)}",
            f"-GPSLongitudeRef={'E' if lng >= 0 else 'W'}",
        ])
        alt = metadata.get("altitude", 0)
        if alt != 0:
            args.extend([
                f"-GPSAltitude={abs(alt)}",
                f"-GPSAltitudeRef={0 if alt >= 0 else 1}",
            ])

    if "description" in metadata:
        args.append(f"-ImageDescription={metadata['description']}")

    if "people" in metadata:
        for name in metadata["people"]:
            args.append(f"-XMP:PersonInImage={name}")

    return args


def run_exiftool_batch(file_args_list):
    """Run exiftool on a batch of files using -@ argfile for speed.

    Writes all arguments to a temporary argfile and runs exiftool once.
    Each file gets -overwrite_original to prevent backup file creation.

    file_args_list: list of (filepath, [arg1, arg2, ...])
    Returns: (success_count, error_count)
    """
    if not file_args_list:
        return 0, 0

    import tempfile
    # Build argfile: one argument per line, groups separated by -execute
    argfile = tempfile.NamedTemporaryFile(
        mode="w", suffix=".args", delete=False, encoding="utf-8"
    )
    try:
        for i, (filepath, args) in enumerate(file_args_list):
            argfile.write("-m\n")
            argfile.write("-overwrite_original\n")
            for a in args:
                argfile.write(a + "\n")
            argfile.write(str(filepath) + "\n")
            if i < len(file_args_list) - 1:
                argfile.write("-execute\n")
        argfile.close()

        result = subprocess.run(
            ["exiftool", "-@", argfile.name],
            capture_output=True, text=True,
            encoding="utf-8", errors="replace", timeout=600
        )
        output = result.stdout + result.stderr
        updated = output.count("1 image files updated")
        errors = output.count("0 image files updated")
        return updated, errors
    except subprocess.TimeoutExpired:
        return 0, len(file_args_list)
    except Exception:
        return 0, len(file_args_list)
    finally:
        try:
            os.unlink(argfile.name)
        except OSError:
            pass


# ---------------------------------------------------------------------------
# ADB HELPERS
# ---------------------------------------------------------------------------

def check_adb():
    """Verify ADB connection to an Android device."""
    try:
        r = subprocess.run(
            ["adb", "devices"], capture_output=True, text=True,
            encoding="utf-8", errors="replace", timeout=10
        )
        lines = [l.strip() for l in r.stdout.strip().split("\n")[1:] if l.strip()]
        devices = [l for l in lines if "device" in l and "offline" not in l]
        if not devices:
            print("ERROR: No Android device found.")
            print("  Connect via USB and enable USB Debugging.")
            sys.exit(1)
        print(f"  ADB device: {devices[0].split()[0]}")
        return True
    except FileNotFoundError:
        print("ERROR: ADB not found! Install Android SDK Platform Tools.")
        sys.exit(1)


def adb_list_zips(remote_dir):
    """List zip files on the Android device."""
    r = subprocess.run(
        ["adb", "shell", f"ls -1 {remote_dir}"],
        capture_output=True, text=True, encoding="utf-8", errors="replace", timeout=30,
    )
    files = [f.strip() for f in r.stdout.strip().split("\n") if f.strip()]
    return sorted([f for f in files if f.lower().endswith(".zip")])


def adb_get_zip_listing(remote_zip):
    """Get the list of all entries in a remote zip via ADB.

    Returns list of (size, name) tuples.
    """
    r = subprocess.run(
        ["adb", "shell", f'unzip -l "{remote_zip}"'],
        capture_output=True, text=True, encoding="utf-8", errors="replace", timeout=120,
    )
    entries = []
    for line in r.stdout.split("\n"):
        parts = line.strip().split(None, 3)
        if len(parts) == 4 and "/" in parts[3]:
            try:
                size = int(parts[0])
            except ValueError:
                continue
            entries.append((size, parts[3].strip()))
    return entries


def adb_stream_all_jsons(remote_zip):
    """Stream ALL JSON file contents from a remote zip via ADB.

    Returns the raw bytes of all concatenated JSON files.
    Uses 'unzip -p *.json' which is very fast (~0.3s for thousands of JSONs).
    """
    cmd = f'unzip -p "{remote_zip}" "*.json" 2>/dev/null'
    r = subprocess.run(
        ["adb", "shell", cmd],
        capture_output=True, timeout=300,
    )
    return r.stdout


def adb_stream_single_json(remote_zip, json_entry):
    """Stream a single JSON file from a remote zip.

    Fallback for when batch parsing has issues.
    """
    cmd = f'unzip -p "{remote_zip}" "{json_entry}" 2>/dev/null'
    r = subprocess.run(
        ["adb", "shell", cmd],
        capture_output=True, timeout=30,
    )
    return r.stdout


def parse_concatenated_jsons(raw_bytes, json_names_ordered):
    """Parse a stream of concatenated JSON objects and match to filenames.

    The key insight: 'unzip -p *.json' outputs JSONs in the same order
    as they appear in the zip directory, which matches 'unzip -l' order.

    Returns dict of {json_entry_path: parsed_dict}.
    """
    text = raw_bytes.decode("utf-8", errors="replace")

    # Split by finding balanced top-level braces
    objects = []
    depth = 0
    start = None
    for i, ch in enumerate(text):
        if ch == "{":
            if depth == 0:
                start = i
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0 and start is not None:
                objects.append(text[start : i + 1])
                start = None

    result = {}
    # If counts match, pair by order (fastest case)
    if len(objects) == len(json_names_ordered):
        for name, raw_json in zip(json_names_ordered, objects):
            try:
                result[name] = json.loads(raw_json)
            except json.JSONDecodeError:
                pass
    else:
        # Counts don't match — parse what we can and match by title field
        parsed_list = []
        for raw_json in objects:
            try:
                parsed_list.append(json.loads(raw_json))
            except json.JSONDecodeError:
                parsed_list.append(None)

        # If close in count, still pair sequentially (some might be metadata.json etc.)
        if abs(len(objects) - len(json_names_ordered)) <= len(json_names_ordered) * 0.05:
            for i, name in enumerate(json_names_ordered):
                if i < len(parsed_list) and parsed_list[i]:
                    result[name] = parsed_list[i]
        else:
            # Large mismatch — fall back to individual reads (handled by caller)
            pass

    return result


# ---------------------------------------------------------------------------
# MAIN PROCESSING
# ---------------------------------------------------------------------------

def local_list_zips(local_dir):
    """List zip files in a local directory."""
    p = Path(local_dir)
    return sorted([f.name for f in p.glob("*.zip")])


def local_get_zip_listing(zip_path):
    """Get the list of all entries in a local zip.

    Returns list of (size, name) tuples.
    """
    import zipfile
    entries = []
    with zipfile.ZipFile(zip_path, "r") as zf:
        for info in zf.infolist():
            if not info.is_dir():
                entries.append((info.file_size, info.filename))
    return entries


def local_stream_all_jsons(zip_path):
    """Read ALL JSON file contents from a local zip.

    Returns the raw bytes of all concatenated JSON files.
    """
    import zipfile
    chunks = []
    with zipfile.ZipFile(zip_path, "r") as zf:
        for info in zf.infolist():
            if info.filename.endswith(".json") and not info.is_dir():
                chunks.append(zf.read(info.filename))
    return b"".join(chunks)


def local_stream_single_json(zip_path, json_entry):
    """Read a single JSON file from a local zip."""
    import zipfile
    with zipfile.ZipFile(zip_path, "r") as zf:
        return zf.read(json_entry)


def main():
    parser = argparse.ArgumentParser(
        description="Fix metadata on Google Takeout photos using JSON sidecars.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""Examples:
  # ADB mode (zips on Android phone):
    python google_takeout_metadata_fixer.py --output ./Output --phone-dir /sdcard/Download

  # Local mode (zips on your computer):
    python google_takeout_metadata_fixer.py --output ./Output --local-zips ./takeout-zips/

  # Dry run:
    python google_takeout_metadata_fixer.py --output ./Output --local-zips ./zips/ --dry-run
"""
    )
    parser.add_argument(
        "--output", required=True,
        help="Directory containing extracted photos/videos to fix"
    )
    parser.add_argument(
        "--phone-dir", default="/sdcard/Download",
        help="Directory on phone containing takeout zips (ADB mode)"
    )
    parser.add_argument(
        "--local-zips",
        help="Local directory containing takeout zip files (skips ADB)"
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Show what would be done without modifying any files"
    )
    args = parser.parse_args()

    use_local = args.local_zips is not None
    output_dir = Path(args.output)
    if not output_dir.exists():
        print(f"ERROR: Output directory not found: {args.output}")
        sys.exit(1)

    mode_label = "Local zip mode" if use_local else "ADB mode (streams JSONs from phone)"
    print("=" * 65)
    print("  Google Takeout Metadata Fixer")
    print(f"  {mode_label}")
    print("=" * 65)

    # Check prerequisites
    print("\nChecking prerequisites...")
    if not use_local:
        check_adb()
    try:
        r = subprocess.run(
            ["exiftool", "-ver"], capture_output=True, text=True,
            encoding="utf-8", errors="replace", timeout=10
        )
        print(f"  ExifTool: v{r.stdout.strip()}")
    except FileNotFoundError:
        print("ERROR: ExifTool not found on PATH!")
        sys.exit(1)

    # Index output files by filename
    print(f"\nIndexing output files in {output_dir}...")
    output_index = {}  # filename -> list of Paths
    for f in output_dir.iterdir():
        if f.is_file() and is_media(f.name):
            output_index.setdefault(f.name, []).append(f)
    total_output = sum(len(v) for v in output_index.values())
    print(f"  {total_output} media files indexed")

    # List zips
    if use_local:
        local_dir = Path(args.local_zips)
        if not local_dir.exists():
            print(f"ERROR: Local zip directory not found: {args.local_zips}")
            sys.exit(1)
        print(f"\nListing zips in {local_dir}...")
        zip_names = local_list_zips(local_dir)
    else:
        print(f"\nListing zips on phone ({args.phone_dir})...")
        zip_names = adb_list_zips(args.phone_dir)
    if not zip_names:
        print("  No zip files found!")
        sys.exit(1)
    for z in zip_names:
        print(f"  {z}")
    print(f"  {len(zip_names)} zip(s) total")

    # Load progress
    progress = load_progress()
    processed_set = set(progress.get("processed_entries", []))

    # Stats
    overall_start = time.time()
    stats = {
        "fixed": 0,
        "already_done": 0,
        "no_json": 0,
        "no_output_match": 0,
        "no_metadata": 0,
        "errors": 0,
        "has_datetime": 0,
        "has_gps": 0,
        "has_description": 0,
        "has_people": 0,
    }

    for zi, zip_name in enumerate(zip_names, 1):
        if zip_name in progress.get("completed_zips", []):
            print(f"\n[{zi}/{len(zip_names)}] SKIP (already done): {zip_name}")
            continue

        print(f"\n{'=' * 65}")
        print(f"[{zi}/{len(zip_names)}] {zip_name}")
        print(f"{'=' * 65}")

        remote_zip = f"{args.phone_dir.rstrip('/')}/{zip_name}"
        zip_source = str(Path(args.local_zips) / zip_name) if use_local else remote_zip

        # Step 1: Get zip listing
        print("  Listing zip contents...", end="", flush=True)
        t = time.time()
        if use_local:
            entries = local_get_zip_listing(zip_source)
        else:
            entries = adb_get_zip_listing(remote_zip)
        all_names = [name for _, name in entries]
        nameset = set(all_names)
        media_entries = [name for name in all_names if is_media(name) and not name.endswith("/")]
        json_entries = [name for name in all_names if name.endswith(".json")]
        print(f" {len(media_entries)} media, {len(json_entries)} JSON ({time.time()-t:.1f}s)")

        # Step 2: Stream all JSONs from this zip
        print("  Streaming JSON metadata...", end="", flush=True)
        t = time.time()
        if use_local:
            raw_json_bytes = local_stream_all_jsons(zip_source)
        else:
            raw_json_bytes = adb_stream_all_jsons(remote_zip)
        json_size = len(raw_json_bytes)
        print(f" {format_size(json_size)} ({time.time()-t:.1f}s)")

        # Step 3: Parse concatenated JSON stream
        print("  Parsing JSON sidecars...", end="", flush=True)
        t = time.time()
        json_data_map = parse_concatenated_jsons(raw_json_bytes, json_entries)
        print(f" {len(json_data_map)}/{len(json_entries)} parsed ({time.time()-t:.1f}s)")

        # Build JSON title index for fallback matching
        json_title_index = {}
        for json_path, data in json_data_map.items():
            title = data.get("title")
            if title and isinstance(title, str):
                json_title_index[title] = json_path

        # Step 4: Process each media file
        print(f"  Processing {len(media_entries)} media files...")
        zip_start = time.time()
        exiftool_batch = []
        zip_stats = {"fixed": 0, "no_json": 0, "no_match": 0, "no_meta": 0, "err": 0, "skip": 0}
        fallback_singles = []

        for i, media_entry in enumerate(media_entries, 1):
            entry_key = f"{zip_name}::{media_entry}"

            # Skip already processed
            if entry_key in processed_set:
                zip_stats["skip"] += 1
                continue

            filename = Path(media_entry).name

            # Find matching output file
            candidates = output_index.get(filename, [])
            if not candidates:
                # Try with _N suffix (from duplicate handling during extraction)
                stem = Path(filename).stem
                suffix = Path(filename).suffix
                for n in range(1, 20):
                    alt = f"{stem}_{n}{suffix}"
                    if alt in output_index:
                        candidates = output_index[alt]
                        break
            if not candidates:
                zip_stats["no_match"] += 1
                continue

            # Find JSON sidecar
            json_entry = find_json_match(nameset, media_entry, json_title_index)
            if not json_entry:
                zip_stats["no_json"] += 1
                processed_set.add(entry_key)
                continue

            # Get parsed JSON data
            json_data = json_data_map.get(json_entry)
            if json_data is None:
                # Batch parsing failed for this one — queue for individual read
                fallback_singles.append((media_entry, json_entry, candidates[0], entry_key))
                continue

            # Parse metadata
            metadata = parse_takeout_json(json_data)
            if not metadata:
                zip_stats["no_meta"] += 1
                processed_set.add(entry_key)
                continue

            # Build exiftool args
            target = candidates[0]
            et_args = build_exiftool_args(metadata, target)
            if not et_args:
                zip_stats["no_meta"] += 1
                processed_set.add(entry_key)
                continue

            # Track stats
            if "datetime" in metadata:
                stats["has_datetime"] += 1
            if "latitude" in metadata:
                stats["has_gps"] += 1
            if "description" in metadata:
                stats["has_description"] += 1
            if "people" in metadata:
                stats["has_people"] += 1

            if args.dry_run:
                zip_stats["fixed"] += 1
                processed_set.add(entry_key)
                continue

            exiftool_batch.append((target, et_args, entry_key))

            # Run batch when full
            if len(exiftool_batch) >= EXIFTOOL_BATCH_SIZE:
                ok, err = run_exiftool_batch(
                    [(fp, a) for fp, a, _ in exiftool_batch]
                )
                zip_stats["fixed"] += ok
                zip_stats["err"] += err
                for _, _, ek in exiftool_batch:
                    processed_set.add(ek)
                exiftool_batch = []

                # Save progress periodically
                if i % 500 == 0:
                    progress["processed_entries"] = list(processed_set)
                    save_progress(progress)

            # Progress display
            if i % 200 == 0 or i == len(media_entries):
                elapsed = time.time() - zip_start
                rate = i / elapsed if elapsed > 0 else 0
                eta = (len(media_entries) - i) / rate if rate > 0 else 0
                print(
                    f"    [{i}/{len(media_entries)}] "
                    f"fixed={zip_stats['fixed']} no_json={zip_stats['no_json']} "
                    f"no_match={zip_stats['no_match']} skip={zip_stats['skip']} "
                    f"| {rate:.0f}/s | ETA: {format_time(eta)}",
                    flush=True,
                )

        # Flush remaining batch
        if exiftool_batch:
            ok, err = run_exiftool_batch(
                [(fp, a) for fp, a, _ in exiftool_batch]
            )
            zip_stats["fixed"] += ok
            zip_stats["err"] += err
            for _, _, ek in exiftool_batch:
                processed_set.add(ek)

        # Handle fallback singles (JSONs that failed batch parsing)
        if fallback_singles:
            print(f"  Fetching {len(fallback_singles)} individual JSONs (fallback)...")
            fb_batch = []
            for media_entry, json_entry, target, entry_key in fallback_singles:
                if use_local:
                    raw = local_stream_single_json(zip_source, json_entry)
                else:
                    raw = adb_stream_single_json(remote_zip, json_entry)
                if not raw:
                    zip_stats["no_json"] += 1
                    processed_set.add(entry_key)
                    continue
                try:
                    data = json.loads(raw.decode("utf-8", errors="replace"))
                except json.JSONDecodeError:
                    zip_stats["err"] += 1
                    processed_set.add(entry_key)
                    continue

                metadata = parse_takeout_json(data)
                if not metadata:
                    zip_stats["no_meta"] += 1
                    processed_set.add(entry_key)
                    continue

                et_args = build_exiftool_args(metadata, target)
                if et_args and not args.dry_run:
                    fb_batch.append((target, et_args, entry_key))
                else:
                    processed_set.add(entry_key)

                if len(fb_batch) >= EXIFTOOL_BATCH_SIZE:
                    ok, err = run_exiftool_batch(
                        [(fp, a) for fp, a, _ in fb_batch]
                    )
                    zip_stats["fixed"] += ok
                    zip_stats["err"] += err
                    for _, _, ek in fb_batch:
                        processed_set.add(ek)
                    fb_batch = []

            if fb_batch:
                ok, err = run_exiftool_batch(
                    [(fp, a) for fp, a, _ in fb_batch]
                )
                zip_stats["fixed"] += ok
                zip_stats["err"] += err
                for _, _, ek in fb_batch:
                    processed_set.add(ek)

        # Save progress
        progress["processed_entries"] = list(processed_set)
        progress.setdefault("completed_zips", []).append(zip_name)
        save_progress(progress)

        elapsed = time.time() - zip_start
        stats["fixed"] += zip_stats["fixed"]
        stats["no_json"] += zip_stats["no_json"]
        stats["no_output_match"] += zip_stats["no_match"]
        stats["no_metadata"] += zip_stats["no_meta"]
        stats["errors"] += zip_stats["err"]
        stats["already_done"] += zip_stats["skip"]

        print(
            f"  DONE: fixed={zip_stats['fixed']} no_json={zip_stats['no_json']} "
            f"no_match={zip_stats['no_match']} skip={zip_stats['skip']} "
            f"err={zip_stats['err']} ({format_time(elapsed)})"
        )
        sys.stdout.flush()

    # Final summary
    elapsed = time.time() - overall_start
    print(f"\n{'=' * 65}")
    print(f"  METADATA FIX COMPLETE ({format_time(elapsed)})")
    print(f"{'=' * 65}")
    print(f"  Files with metadata fixed:  {stats['fixed']}")
    print(f"  Already processed (skip):   {stats['already_done']}")
    print(f"  No JSON sidecar found:      {stats['no_json']}")
    print(f"  No matching output file:    {stats['no_output_match']}")
    print(f"  No usable metadata in JSON: {stats['no_metadata']}")
    print(f"  Errors:                     {stats['errors']}")
    print(f"  ---")
    print(f"  With date/time:             {stats['has_datetime']}")
    print(f"  With GPS coordinates:       {stats['has_gps']}")
    print(f"  With descriptions:          {stats['has_description']}")
    print(f"  With people tags:           {stats['has_people']}")
    print(f"{'=' * 65}")

    if args.dry_run:
        print("\n  [DRY RUN] No files were modified.")
    else:
        print("\n  All done! Verify with:")
        print('  exiftool -DateTimeOriginal -CreateDate -GPSLatitude -GPSLongitude -XMP:PersonInImage "<file>"')


if __name__ == "__main__":
    main()
