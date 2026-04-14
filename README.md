# PrismWave HITS

`prismwave-hits` is the static schedule repository for PrismWave HITS mode.

The repository is designed to work without a custom server:

- GitHub Actions builds the next UTC edition every day at `23:30 UTC`
- That build time equals `07:30` in Beijing (`Asia/Shanghai`), which is 30 minutes before the daily `08:00` HITS resume time
- The generated manifests are committed back to this repository
- PrismWave reads the public JSON files directly from GitHub Raw

## Published files

- `latest.json`
  - small entry manifest that points PrismWave to the active edition
- `schedules/YYYY-MM-DD.json`
  - the full daily schedule for one UTC edition
  - each track can optionally include `audio_url` and `cover_url`

## Current generator scope

The current generator now focuses on:

- daily manifest generation
- UTC schedule windows
- overnight off-air gaps
- optional Last.fm ingestion when `LASTFM_API_KEY` is configured
- a real playable fallback pool from Audius trending tracks
- best-effort Audius search matching for unresolved chart candidates
- stable `audio_url` playback endpoints for tracks that can be streamed directly

Planned next steps:

- add more legal playback providers
- improve track scoring and diversity rules
- improve lyric-availability prioritisation
- enrich tracks with more metadata and better artwork hints
- publish richer station status fields for the PrismWave client

## Raw endpoints

PrismWave should read:

- [latest.json](https://raw.githubusercontent.com/shanbei2033/prismwave-hits/main/latest.json)
- `https://raw.githubusercontent.com/shanbei2033/prismwave-hits/main/schedules/<UTC-date>.json`

## Secrets

Optional:

- `LASTFM_API_KEY`

If no API key is configured, the generator still tries to build a playable schedule from Audius.
`data/demo_tracks.json` remains only as the final local fallback when remote catalog fetches fail.

## Local build

```bash
python scripts/build_hits.py
```

Optional environment variables:

- `TARGET_DATE=2026-04-13`
- `LASTFM_API_KEY=...`

## Schedule model

The current default station model is:

- edition timezone: `UTC`
- manifest build time: `23:30 UTC` (`07:30` Beijing time)
- overnight off-air window: `20:00-00:00 UTC` (`04:00-08:00` Beijing time)

That means the generated daily schedule intentionally contains a gap during the overnight maintenance window.

## Playback notes

- When a track is streamable, the schedule carries:
  - `audio_url`
  - `audio_provider`
  - `provider_track_id`
- For Audius, `audio_url` is stored as a stable API endpoint like `/v1/tracks/<id>/stream`
  instead of a short-lived signed CDN URL.
- This lets the PrismWave client fetch a fresh redirect at playback time, which is more reliable
  for all-day HITS scheduling.
