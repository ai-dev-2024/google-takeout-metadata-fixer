<p align="center">
  <img src="assets/icon.svg" alt="Google Takeout Metadata Fixer" width="128" height="128">
</p>

# Google Takeout Metadata Fixer

[![ZAI Community](https://img.shields.io/badge/Part%20of-ZAI%20Start--up%20Community-8b5cf6?style=for-the-badge)](https://startup.z.ai/)
[![Ko-fi](https://img.shields.io/badge/☕_Support_on_Ko--fi-FF5E5B?style=for-the-badge&logo=ko-fi)](https://ko-fi.com/ai_dev_2024)

Robust Google Takeout metadata fixer and Google Photos Takeout metadata fixer for restoring EXIF dates, GPS, descriptions, people tags, and video timestamps from Google Takeout JSON sidecars.

This project is built for one job: take messy Google Photos / Google Takeout exports and turn them back into a clean, portable media library with correct metadata embedded into the actual files.

Project description for GitHub homepage:

> Robust Google Takeout / Google Photos Takeout metadata fixer that restores EXIF dates, GPS, descriptions, people tags, and video timestamps from JSON sidecars.

[Support the project](https://ko-fi.com/ai_dev_2024)

## Why This Project Exists

Google Takeout often separates important metadata from the original media file and stores it in JSON sidecars. That breaks photo timelines, maps, descriptions, and portability across tools like Google Photos, Apple Photos, Windows Photos, Immich, Lightroom, PhotoPrism, and plain file explorers.

This project fixes that by reading the Takeout JSON sidecars and writing the metadata back into the media files themselves with ExifTool.

## Why This Tool Over Existing Options

| Feature | This tool | [gpth](https://github.com/TheLastGimbus/GooglePhotosTakeoutHelper) | [google-photos-migrate](https://github.com/garzj/google-photos-migrate) | [GoogleTakeoutFixer](https://github.com/feloex/GoogleTakeoutFixer) |
|---------|:---------:|:-------------------------------:|:---------------------------------------------:|:--------------------------------:|
| Writes real EXIF dates | ✅ | ❌ file timestamps only | ✅ | ✅ |
| Writes GPS coordinates | ✅ | ❌ | ✅ | ✅ |
| Writes descriptions | ✅ | ❌ | ✅ | ✅ |
| Writes people tags | ✅ | ❌ | ❌ | ❌ |
| Handles videos well | ✅ | ❌ | ✅ | ✅ |
| Supports new `supplemental-metadata` names | ✅ | ❌ | ❌ | ❌ |
| Handles truncated JSON names | ✅ | ✅ | ✅ | ✅ |
| Handles edited filenames in 12+ languages | ✅ | ✅ | Partial | ❌ |
| Handles live photo pairing | ✅ | ❌ | ✅ | ✅ |
| Falls back to JSON `title` matching | ✅ | ❌ | ✅ | ❌ |
| Reads zips directly from Android over ADB | ✅ | ❌ | ❌ | ❌ |
| Works without extracting zips locally | ✅ | ❌ | ❌ | ❌ |
| Resume support | ✅ | ❌ | ❌ | ❌ |
| Recovery path for broken zip listings | ✅ | ❌ | ❌ | ❌ |

## What It Fixes

After processing, your files can contain:

- Correct capture dates via `DateTimeOriginal`, `CreateDate`, and related video timestamps
- GPS latitude, longitude, and altitude
- Descriptions via `ImageDescription`
- People tags via `XMP:PersonInImage`
- Better portability across photo managers because metadata is embedded in the media file itself

## Core Features

- Robust Google Takeout metadata fixer for both local zip files and Android-hosted zip files
- Google Photos Takeout metadata fixer focused on correct embedded dates, not only filesystem timestamps
- Full JSON sidecar matching for Google’s newer `supplemental-metadata` naming scheme
- Truncation handling for long filenames and sidecar names
- Duplicate file handling such as `photo(2).jpg`
- Edited-file suffix support in 12+ languages
- Live photo still/video matching
- JSON `title` fallback matching when filenames are inconsistent
- Resume support for interrupted runs
- Batched ExifTool execution for speed and reduced overhead
- ADB mode for streaming sidecars from the phone without consuming huge extra local disk space
- Specialized recovery helper for problematic zips whose central directory can still be read even when standard unzip listing is unreliable

## Quick Start

### Requirements

1. Python 3.9+
2. ExifTool on your `PATH`
3. ADB on your `PATH` for Android / phone mode

Install optional downloader dependencies:

```bash
pip install -r requirements.txt
```

### Main Metadata Fixer

Local zip mode:

```bash
python google_takeout_metadata_fixer.py --output ./Output --local-zips ./takeout-zips
```

ADB / phone mode:

```bash
python google_takeout_metadata_fixer.py --output ./Output --phone-dir /sdcard/Download
```

Dry run:

```bash
python google_takeout_metadata_fixer.py --output ./Output --local-zips ./takeout-zips --dry-run
```

### Extract Takeout Media From a Phone

```bash
python extract_takeout_media.py --adb /sdcard/Download --output ./Output
```

### Resumable Copy Back to Phone

```bash
python adb_resumable_copy.py --source-dir ./Output --dest-dir /sdcard/Download/Output
```

### Recovery Mode for a Broken Zip Listing

```bash
python recover_takeout_zip_metadata.py --remote-zip /sdcard/Download/takeout-example.zip --output ./Output
```

## How It Works

1. Lists zip contents to discover media files and JSON sidecars
2. Extracts only metadata sidecars where possible instead of exploding entire archives
3. Applies layered matching rules to pair the right JSON with the right media file
4. Converts JSON metadata into ExifTool arguments
5. Writes metadata in batches and saves progress so interrupted runs can resume safely

## Matching Logic

Google Takeout sidecars are inconsistent. This project handles all of these patterns:

| Pattern | Example |
|---------|---------|
| Standard sidecar | `photo.jpg.supplemental-metadata.json` |
| Truncated sidecar | `photo.jpg.supplemental-metad.json` |
| Duplicate suffix | `photo.jpg.supplemental-metadata(2).json` |
| Older export format | `photo.jpg.json` or `photo.json` |
| Bracket variants | `photo.jpg(1).json` |
| Edited filename variants | `photo-edited.jpg` -> original photo JSON |
| Live photo mapping | video file matched to still image JSON |
| JSON title fallback | uses the `title` field from the JSON |

Supported edited suffixes include English, German, French, Spanish, Portuguese, Italian, Dutch, Polish, Swedish, Finnish, Turkish, Russian, and Japanese variants already seen in real exports.

## Metadata Written

### Photos

- `DateTimeOriginal`
- `CreateDate`
- `ModifyDate`
- `GPSLatitude`
- `GPSLongitude`
- `GPSAltitude`
- `ImageDescription`
- `XMP:PersonInImage`

### Videos

All of the above where applicable, plus:

- `TrackCreateDate`
- `TrackModifyDate`
- `MediaCreateDate`
- `MediaModifyDate`

## Privacy and GitHub Safety

- OAuth secrets and tokens are ignored via `.gitignore`
- Generated logs and progress files are ignored
- Local absolute paths have been removed from publishable helper scripts
- The repo is intended to be safe to push after you verify any local untracked files you want to keep out of version control

## Included Scripts

| File | Purpose |
|------|---------|
| `google_takeout_metadata_fixer.py` | Main Google Takeout metadata fixer |
| `extract_takeout_media.py` | Extract media from Takeout zips, including ADB mode |
| `recover_takeout_zip_metadata.py` | Recovery helper for problematic zip listings over ADB |
| `adb_resumable_copy.py` | Resumable ADB copy helper back to phone storage |
| `google_photos_downloader.py` | Optional Google Photos downloader using your own API credentials |

## Support

If this project saves you time, storage, or a broken migration, you can support continued maintenance here:

- [Ko-fi support page](https://ko-fi.com/ai_dev_2024)

## License

MIT

## Contributing

Pull requests and issue reports are welcome, especially for:

- New Google Takeout naming edge cases
- More edited filename language variants
- Better zip recovery and matching heuristics
- Test cases from unusual Takeout exports
