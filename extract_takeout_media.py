"""
Google Takeout Processor - Process takeout zips directly from phone

Reads zip files from your phone (via drive path or ADB), extracts and fixes
metadata one file at a time to minimize HDD usage. Keeps the zips on the phone.

Usage:
  # Phone mounted as drive letter (or zips on USB stick, etc.)
    python extract_takeout_media.py E:\\Takeout

  # Android phone via ADB (no drive letter needed)
    python extract_takeout_media.py --adb /sdcard/Takeout

  # Custom output directory
    python extract_takeout_media.py --adb /sdcard/Takeout --output fixed_photos

Disk usage:
  - Direct mode: only 1 media file at a time + output folder
  - ADB mode: 1 zip at a time + 1 media file + output folder
"""

import argparse
import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
import zipfile
from datetime import datetime, timezone
from pathlib import Path

OUTPUT_DIR = "output"
PROGRESS_FILE = "processing_progress.json"
TEMP_ZIP_DIR = "_temp_zip"

MEDIA_EXTENSIONS = {
    ".jpg", ".jpeg", ".png", ".gif", ".bmp", ".tiff", ".tif",
    ".webp", ".heic", ".heif", ".raw", ".cr2", ".nef", ".arw",
    ".mp4", ".mov", ".avi", ".mkv", ".3gp", ".m4v", ".webm",
}


def is_media(name):
    return Path(name).suffix.lower() in MEDIA_EXTENSIONS


def load_progress():
    if os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE, "r") as f:
            return json.load(f)
    return {"completed_zips": [], "processed_files": {}}


def save_progress(progress):
    for attempt in range(5):
        try:
            tmp = PROGRESS_FILE + ".tmp"
            with open(tmp, "w") as f:
                json.dump(progress, f, indent=2)
            # Atomic-ish replace to avoid corruption
            if os.path.exists(PROGRESS_FILE):
                os.replace(tmp, PROGRESS_FILE)
            else:
                os.rename(tmp, PROGRESS_FILE)
            return
        except PermissionError:
            time.sleep(1)
    # Last resort: just write directly
    with open(PROGRESS_FILE, "w") as f:
        json.dump(progress, f, indent=2)


# --- JSON sidecar matching (within a zip) ---

def find_json_in_zip(nameset, media_entry):
    """Find the JSON sidecar for a media file within a zip's namelist.
    
    Google Takeout uses 'supplemental-metadata' naming which gets truncated
    when filenames are long. Also handles Google's duplicate naming where
    file(2).jpg maps to file.jpg.supplemental-metadata(2).json
    """
    import re

    p_media = media_entry.rsplit("/", 1)
    parent = p_media[0] + "/" if len(p_media) > 1 else ""
    basename = p_media[-1]
    stem = Path(basename).stem

    # New format: photo.jpg.supplemental-metadata.json
    c = f"{media_entry}.supplemental-metadata.json"
    if c in nameset:
        return c

    # New format with duplicate index: photo.jpg.supplemental-metadata(1).json
    for i in range(1, 20):
        c = f"{media_entry}.supplemental-metadata({i}).json"
        if c in nameset:
            return c

    # Handle Google duplicate files: file(N).ext -> file.ext.supplemental-metadata(N).json
    # e.g. MAC_ (29)(2).JPG -> MAC_ (29).JPG.supplemental-metadata(2).json
    dup_match = re.match(r'^(.+?)\((\d+)\)(\.[^.]+)$', basename)
    if dup_match:
        orig_name = dup_match.group(1) + dup_match.group(3)  # e.g. MAC_ (29).JPG
        dup_idx = dup_match.group(2)  # e.g. 2
        orig_entry = parent + orig_name
        # Try: original.jpg.supplemental-metadata(N).json
        c = f"{orig_entry}.supplemental-metadata({dup_idx}).json"
        if c in nameset:
            return c
        # Fallback: try base supplemental-metadata.json (same metadata for all dupes)
        c = f"{orig_entry}.supplemental-metadata.json"
        if c in nameset:
            return c
        # Fallback: try any other numbered supplemental-metadata
        for i in range(1, 20):
            c = f"{orig_entry}.supplemental-metadata({i}).json"
            if c in nameset:
                return c
        # Try truncated version
        prefix = f"{orig_entry}.suppl"
        for name in nameset:
            if name.startswith(prefix) and name.endswith(".json"):
                return name

    # Old format: photo.jpg.json
    c = media_entry + ".json"
    if c in nameset:
        return c

    # Old format: photo.json (stem only)
    c = parent + stem + ".json"
    if c in nameset:
        return c

    # Old bracket variations: photo.jpg(1).json
    for i in range(1, 10):
        c = f"{media_entry}({i}).json"
        if c in nameset:
            return c

    # Truncated supplemental-metadata: match any JSON in same folder that
    # starts with the media filename + ".suppl" (covers all truncation lengths)
    prefix = f"{media_entry}.suppl"
    for name in nameset:
        if name.startswith(prefix) and name.endswith(".json"):
            return name

    # Strip -edited suffix: photo-edited.jpg -> try photo.jpg's JSON
    edited_match = re.match(r'^(.+)-edited(\.[^.]+)$', basename, re.IGNORECASE)
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

    # Live photo MP4: match video.mp4 -> video.jpg or video.heic JSON
    ext_lower = Path(basename).suffix.lower()
    if ext_lower in ('.mp4', '.mov'):
        stem_part = Path(basename).stem
        for photo_ext in ('.jpg', '.jpeg', '.heic', '.png', '.JPG', '.HEIC'):
            photo_entry = parent + stem_part + photo_ext
            c = f"{photo_entry}.supplemental-metadata.json"
            if c in nameset:
                return c
            c = photo_entry + ".json"
            if c in nameset:
                return c

    return None


def parse_takeout_json(raw_bytes):
    """Extract metadata from a Google Takeout JSON sidecar."""
    data = json.loads(raw_bytes)
    metadata = {}

    # Try photoTakenTime first, fall back to creationTime
    for time_key in ("photoTakenTime", "creationTime"):
        if time_key in data and "datetime" not in metadata:
            ts_str = data[time_key].get("timestamp", "0")
            ts = int(ts_str)
            if ts > 0:
                dt = datetime.fromtimestamp(ts, tz=timezone.utc)
                metadata["datetime"] = dt.strftime("%Y:%m:%d %H:%M:%S")

    for geo_key in ("geoData", "geoDataExif"):
        if geo_key in data and "latitude" not in metadata:
            geo = data[geo_key]
            lat, lng = geo.get("latitude", 0), geo.get("longitude", 0)
            if lat != 0 or lng != 0:
                metadata["latitude"] = lat
                metadata["longitude"] = lng
                metadata["altitude"] = geo.get("altitude", 0)

    if data.get("description"):
        metadata["description"] = data["description"]

    return metadata


VIDEO_EXTENSIONS = {".mp4", ".mov", ".avi", ".mkv", ".3gp", ".m4v", ".webm"}


def build_exiftool_args(metadata, filepath):
    """Build exiftool command to write metadata."""
    args = ["exiftool", "-overwrite_original"]
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

    args.append(str(filepath))
    return args


# --- Output path handling ---

def get_unique_output_path(output_dir, filename):
    """Get a unique output path, adding _1, _2 etc. if file exists."""
    output_file = output_dir / filename
    if not output_file.exists():
        return output_file

    stem = Path(filename).stem
    suffix = Path(filename).suffix
    counter = 1
    while output_file.exists():
        output_file = output_dir / f"{stem}_{counter}{suffix}"
        counter += 1
    return output_file


# --- ADB helpers ---

def check_adb():
    try:
        r = subprocess.run(["adb", "devices"], capture_output=True, text=True, timeout=10)
        lines = [l.strip() for l in r.stdout.strip().split("\n")[1:] if l.strip()]
        devices = [l for l in lines if "device" in l]
        if not devices:
            print("ERROR: No Android device found. Connect your phone and enable USB debugging.")
            sys.exit(1)
        return True
    except FileNotFoundError:
        print("ERROR: ADB not found! Install Android SDK Platform Tools and add to PATH.")
        print("  https://developer.android.com/tools/releases/platform-tools")
        sys.exit(1)


def adb_list_zips(remote_dir):
    """List zip files on the Android device."""
    result = subprocess.run(
        ["adb", "shell", "ls", "-1", remote_dir],
        capture_output=True, text=True, timeout=30,
    )
    if result.returncode != 0:
        print(f"ADB error listing {remote_dir}: {result.stderr.strip()}")
        sys.exit(1)
    files = [f.strip() for f in result.stdout.strip().split("\n") if f.strip()]
    return sorted([f for f in files if f.lower().endswith(".zip")])


def adb_get_file_size(remote_path):
    """Get file size on Android device in bytes."""
    r = subprocess.run(
        ["adb", "shell", "stat", "-c", "%s", remote_path],
        capture_output=True, text=True, timeout=10,
    )
    if r.returncode == 0:
        try:
            return int(r.stdout.strip())
        except ValueError:
            pass
    return 0


def format_size(nbytes):
    """Human-readable file size."""
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if abs(nbytes) < 1024:
            return f"{nbytes:.1f} {unit}"
        nbytes /= 1024
    return f"{nbytes:.1f} PB"


def format_time(seconds):
    """Human-readable duration."""
    if seconds < 60:
        return f"{int(seconds)}s"
    if seconds < 3600:
        return f"{int(seconds // 60)}m {int(seconds % 60)}s"
    h = int(seconds // 3600)
    m = int((seconds % 3600) // 60)
    return f"{h}h {m}m"


def adb_pull(remote_path, local_path):
    """Pull a file from Android device with live progress."""
    name = Path(remote_path).name
    total_size = adb_get_file_size(remote_path)
    size_str = format_size(total_size) if total_size else "unknown size"
    print(f"  Pulling: {name} ({size_str})")
    sys.stdout.flush()

    start = time.time()
    # Stream adb pull output live (progress goes to stderr)
    proc = subprocess.Popen(
        ["adb", "pull", remote_path, str(local_path)],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE,
    )

    # Monitor file size growth for progress
    local_file = Path(local_path)
    last_print = start
    while proc.poll() is None:
        time.sleep(2)
        if local_file.exists():
            current = local_file.stat().st_size
            elapsed = time.time() - start
            speed = current / elapsed if elapsed > 0 else 0
            if total_size > 0:
                pct = current / total_size * 100
                eta = (total_size - current) / speed if speed > 0 else 0
                print(f"    {format_size(current)}/{size_str} ({pct:.1f}%) | {format_size(speed)}/s | ETA: {format_time(eta)}   ", end="\r")
            else:
                print(f"    {format_size(current)} | {format_size(speed)}/s | {format_time(elapsed)}   ", end="\r")
            sys.stdout.flush()

    stdout, stderr = proc.communicate()
    elapsed = time.time() - start
    speed = total_size / elapsed if elapsed > 0 and total_size > 0 else 0

    if proc.returncode != 0:
        print(f"\n  ADB pull FAILED: {stderr.decode().strip()}")
        return False

    print(f"    Pulled {size_str} in {format_time(elapsed)} ({format_size(speed)}/s)                    ")
    sys.stdout.flush()
    return True


# --- Core processing ---

def process_zip(zip_path, output_dir, progress, zip_index=0, total_zips=0):
    """Process all media files in a single takeout zip, one file at a time."""
    zip_name = Path(zip_path).name
    zip_label = f"[Zip {zip_index}/{total_zips}]" if total_zips else ""

    try:
        zf = zipfile.ZipFile(zip_path, "r")
    except zipfile.BadZipFile:
        print(f"  ERROR: {zip_name} is not a valid zip file, skipping")
        return

    with zf:
        all_names = zf.namelist()
        nameset = set(all_names)
        media_entries = [e for e in all_names if is_media(e) and not e.endswith("/")]

        # Calculate total uncompressed media size
        media_size_total = sum(
            info.file_size for info in zf.infolist()
            if info.filename in set(media_entries)
        )
        print(f"  {len(media_entries)} media files ({format_size(media_size_total)}) in this zip")
        sys.stdout.flush()

        fixed = 0
        copied = 0
        errors = 0
        skipped = 0
        bytes_done = 0
        zip_start = time.time()

        for i, entry in enumerate(media_entries, 1):
            filename = Path(entry).name
            entry_key = f"{zip_name}::{entry}"

            # Get file size from zip info
            try:
                file_size = zf.getinfo(entry).file_size
            except KeyError:
                file_size = 0

            # Skip already processed
            if entry_key in progress.get("processed_files", {}):
                skipped += 1
                bytes_done += file_size
                continue

            temp_dir = None
            try:
                # Extract just this one media file to a temp dir
                temp_dir = Path(tempfile.mkdtemp(prefix="gphoto_"))
                temp_media = temp_dir / filename

                with zf.open(entry) as src, open(temp_media, "wb") as dst:
                    shutil.copyfileobj(src, dst)

                # Find and parse JSON sidecar
                json_entry = find_json_in_zip(nameset, entry)
                metadata = {}
                if json_entry:
                    with zf.open(json_entry) as jf:
                        metadata = parse_takeout_json(jf.read())

                # Determine output path
                output_file = get_unique_output_path(output_dir, filename)

                # Move to output
                shutil.move(str(temp_media), str(output_file))

                # Apply metadata with exiftool if we have any
                if metadata:
                    args = build_exiftool_args(metadata, output_file)
                    result = subprocess.run(args, capture_output=True, text=True, timeout=60)
                    if result.returncode != 0 and result.stderr.strip():
                        print(f"    exiftool warning: {filename}: {result.stderr.strip()[:100]}")
                    fixed += 1
                else:
                    copied += 1

                bytes_done += file_size

                # Track progress
                progress.setdefault("processed_files", {})[entry_key] = True

                # Show live progress every 10 files or at end
                if i % 10 == 0 or i == len(media_entries):
                    elapsed = time.time() - zip_start
                    rate = bytes_done / elapsed if elapsed > 0 else 0
                    pct = bytes_done / media_size_total * 100 if media_size_total > 0 else 0
                    eta = (media_size_total - bytes_done) / rate if rate > 0 else 0
                    print(
                        f"  {zip_label} [{i}/{len(media_entries)}] {pct:.0f}% "
                        f"| {format_size(bytes_done)}/{format_size(media_size_total)} "
                        f"| {format_size(rate)}/s | ETA: {format_time(eta)} "
                        f"| fix={fixed} copy={copied} err={errors}"
                    )
                    sys.stdout.flush()

                # Save progress every 50 files
                if i % 50 == 0:
                    save_progress(progress)

            except Exception as e:
                errors += 1
                bytes_done += file_size
                print(f"    ERROR: {filename}: {e}")
                sys.stdout.flush()
            finally:
                if temp_dir and temp_dir.exists():
                    shutil.rmtree(temp_dir, ignore_errors=True)

        save_progress(progress)
        elapsed = time.time() - zip_start
        print(f"  DONE {zip_name}: {fixed} fixed, {copied} no-metadata, {errors} errors, {skipped} skipped ({format_time(elapsed)})")
        sys.stdout.flush()


def main():
    parser = argparse.ArgumentParser(
        description="Process Google Takeout zips from phone, minimizing disk usage"
    )
    parser.add_argument(
        "source",
        help="Path to folder with takeout zips (drive letter or Android path with --adb)"
    )
    parser.add_argument(
        "--adb", action="store_true",
        help="Use ADB to pull zips from Android phone one at a time"
    )
    parser.add_argument(
        "--output", default=OUTPUT_DIR,
        help=f"Output directory for fixed photos (default: {OUTPUT_DIR})"
    )

    args = parser.parse_args()
    output_dir = Path(args.output)
    output_dir.mkdir(exist_ok=True)

    # Check exiftool
    try:
        subprocess.run(["exiftool", "-ver"], capture_output=True, check=True, timeout=10)
    except FileNotFoundError:
        print("ERROR: ExifTool not found! Install it and add to PATH.")
        print("  https://exiftool.org/install.html")
        sys.exit(1)

    progress = load_progress()

    if args.adb:
        # ==================== ADB MODE ====================
        check_adb()
        print(f"=== ADB Mode ===")
        print(f"Source:  phone:{args.source}")
        print(f"Output:  {output_dir.resolve()}\n")

        zip_names = adb_list_zips(args.source)
        if not zip_names:
            print(f"No zip files found at {args.source}")
            sys.exit(1)

        # Get sizes from phone
        zip_sizes = {}
        total_phone_size = 0
        for zn in zip_names:
            rp = f"{args.source.rstrip('/')}/{zn}"
            sz = adb_get_file_size(rp)
            zip_sizes[zn] = sz
            total_phone_size += sz

        print(f"Found {len(zip_names)} zip files on phone ({format_size(total_phone_size)} total):")
        for zn in zip_names:
            done = "DONE" if zn in progress.get("completed_zips", []) else "    "
            print(f"  {done} {zn}  ({format_size(zip_sizes[zn])})")

        # Check disk space
        import shutil as _shutil
        disk = _shutil.disk_usage(str(output_dir.resolve()))
        print(f"\nDisk free: {format_size(disk.free)}")
        print(f"Largest zip: {format_size(max(zip_sizes.values()))}")
        print(f"Needed per cycle: largest zip + output files\n")

        overall_start = time.time()
        temp_zip_dir = Path(TEMP_ZIP_DIR)
        temp_zip_dir.mkdir(exist_ok=True)

        for zi, zip_name in enumerate(zip_names, 1):
            if zip_name in progress.get("completed_zips", []):
                print(f"\n[{zi}/{len(zip_names)}] SKIP (already done): {zip_name}")
                continue

            print(f"\n{'='*60}")
            print(f"[{zi}/{len(zip_names)}] {zip_name} ({format_size(zip_sizes[zip_name])})")
            print(f"{'='*60}")

            remote_path = f"{args.source.rstrip('/')}/{zip_name}"
            local_zip = temp_zip_dir / zip_name

            if not adb_pull(remote_path, local_zip):
                continue

            try:
                process_zip(local_zip, output_dir, progress, zi, len(zip_names))
                progress.setdefault("completed_zips", []).append(zip_name)
                save_progress(progress)
            finally:
                # Free HDD space immediately
                if local_zip.exists():
                    local_zip.unlink()
                    print(f"  Cleaned up local copy of {zip_name}")
                # Show overall progress
                done_zips = len(progress.get("completed_zips", []))
                done_files = len(progress.get("processed_files", {}))
                elapsed = time.time() - overall_start
                print(f"  Overall: {done_zips}/{len(zip_names)} zips, {done_files} files, elapsed: {format_time(elapsed)}")
                disk = _shutil.disk_usage(str(output_dir.resolve()))
                print(f"  Disk free: {format_size(disk.free)}")
                sys.stdout.flush()

        # Clean up temp dir
        if temp_zip_dir.exists() and not list(temp_zip_dir.iterdir()):
            temp_zip_dir.rmdir()

    else:
        # ==================== DIRECT PATH MODE ====================
        source_dir = Path(args.source)

        if not source_dir.exists():
            print(f"ERROR: '{args.source}' not found!")
            print()
            print("If your phone uses MTP (no drive letter), try one of these:")
            print("  1. Use ADB mode:  python extract_takeout_media.py --adb /sdcard/Takeout")
            print("  2. Map phone as drive letter (use an app like 'USB Mass Storage Enabler')")
            print("  3. Share phone folder over WiFi and use the network path")
            sys.exit(1)

        zip_files = sorted(source_dir.glob("*.zip"))
        if not zip_files:
            print(f"No zip files found in {args.source}")
            sys.exit(1)

        print(f"=== Direct Mode ===")
        print(f"Source:  {source_dir.resolve()}")
        print(f"Output:  {output_dir.resolve()}")
        print(f"Zips:    {len(zip_files)}\n")

        for zi, zip_path in enumerate(zip_files, 1):
            zip_name = zip_path.name

            if zip_name in progress.get("completed_zips", []):
                print(f"[{zi}/{len(zip_files)}] SKIP (done): {zip_name}")
                continue

            print(f"[{zi}/{len(zip_files)}] {zip_name}")
            process_zip(zip_path, output_dir, progress, zi, len(zip_files))
            progress.setdefault("completed_zips", []).append(zip_name)
            save_progress(progress)
            print()

    total_files = len(progress.get("processed_files", {}))
    total_zips = len(progress.get("completed_zips", []))
    print(f"\n=== All Done ===")
    print(f"Processed {total_files} media files from {total_zips} zips")
    print(f"Output: {output_dir.resolve()}")


if __name__ == "__main__":
    main()
