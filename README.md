# PrismWave HITS

`prismwave-hits` is the static schedule repository for PrismWave HITS mode.

The repository is designed to work without a custom server:

- GitHub Actions builds the next UTC edition every day at `23:30 UTC`
- The generated manifests are committed back to this repository
- PrismWave reads the public JSON files directly from GitHub Raw

## Published files

- `latest.json`
  - small entry manifest that points PrismWave to the active edition
- `schedules/YYYY-MM-DD.json`
  - the full daily schedule for one UTC edition

## Current generator scope

This first bootstrap version focuses on:

- daily manifest generation
- UTC schedule windows
- overnight off-air gaps
- a fallback bootstrap track pool so the pipeline always produces valid JSON
- optional Last.fm ingestion when `LASTFM_API_KEY` is configured

Planned next steps:

- add more real-world chart providers
- improve track scoring and diversity rules
- enrich tracks with more metadata and better artwork hints
- publish richer station status fields for the PrismWave client

## Raw endpoints

PrismWave should read:

- [latest.json](https://raw.githubusercontent.com/shanbei2033/prismwave-hits/main/latest.json)
- `https://raw.githubusercontent.com/shanbei2033/prismwave-hits/main/schedules/<UTC-date>.json`

## Secrets

Optional:

- `LASTFM_API_KEY`

If no API key is configured, the generator falls back to `data/demo_tracks.json`.

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
- manifest build time: `23:30 UTC`
- overnight off-air window: `02:00-05:00 UTC`

That means the generated daily schedule intentionally contains a gap during the overnight maintenance window.
