"""Generate the daily home recommendations JSON for PrismWave's online mode.

Independent of the HITS schedule. Reuses fetchers from build_hits.py.

Output:
- home/home_recommendations-YYYY-MM-DD.json (per-day archive)
- home/latest_home.json                     (always points at today's edition)
"""

from __future__ import annotations

import json
import os
import random
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from build_hits import (  # type: ignore
    CandidateTrack,
    audius_stream_endpoint,
    fetch_audius_genre_trending,
    fetch_audius_trending,
    fetch_audius_trending_monthly,
    fetch_deezer_chart,
    fetch_itunes_rss,
    fetch_json,
    fetch_lastfm_global,
    fetch_lastfm_tag,
    iso_z,
    load_json,
    now_utc,
    write_json,
)

UTC = timezone.utc
GENERATOR_VERSION = "prismwave-home/0.1.0"
LASTFM_ENDPOINT = "https://ws.audioscrobbler.com/2.0/"
ROOT = Path(__file__).resolve().parents[1]
CONFIG_PATH = ROOT / "config" / "station.json"
HOME_DIR = ROOT / "home"
LATEST_HOME_PATH = HOME_DIR / "latest_home.json"

SECTION_TRACK_LIMIT = 20
TAG_LIMIT = 40
TOP_PLAYLIST_LIMIT = 10

# Last.fm uses this exact image as the placeholder when a track has no
# real artwork. The URL passes our magic-byte check (it's a real PNG)
# but renders as a generic star/note glyph that looks broken. Drop it.
LASTFM_PLACEHOLDER_HASHES = (
    "2a96cbd8b46e442fc41c2b86b821562f",
)

SECTION_DEFINITIONS: list[dict[str, Any]] = [
    {
        "id": "global-hot",
        "title": {
            "zh-Hans": "今日热门",
            "zh-Hant": "今日熱門",
            "en-US": "Global Hits",
        },
        "subtitle": "Powered by Last.fm",
        "fetcher": "lastfm_global",
        "fetcher_args": {"limit": 60},
    },
    {
        "id": "audius-trending",
        "title": {
            "zh-Hans": "Audius 流行",
            "zh-Hant": "Audius 流行",
            "en-US": "Trending on Audius",
        },
        "subtitle": "Streamable now",
        "fetcher": "audius_trending",
        "fetcher_args": {"limit": 60},
    },
    {
        "id": "tag-pop",
        "title": {"zh-Hans": "流行乐", "zh-Hant": "流行樂", "en-US": "Pop"},
        "subtitle": "Top tracks · pop",
        "fetcher": "lastfm_tag",
        "fetcher_args": {"tag": "pop", "limit": 60},
    },
    {
        "id": "tag-rock",
        "title": {"zh-Hans": "摇滚", "zh-Hant": "搖滾", "en-US": "Rock"},
        "subtitle": "Top tracks · rock",
        "fetcher": "lastfm_tag",
        "fetcher_args": {"tag": "rock", "limit": 60},
    },
    {
        "id": "tag-electronic",
        "title": {"zh-Hans": "电子", "zh-Hant": "電子", "en-US": "Electronic"},
        "subtitle": "Top tracks · electronic",
        "fetcher": "lastfm_tag",
        "fetcher_args": {"tag": "electronic", "limit": 60},
    },
    {
        "id": "tag-indie",
        "title": {"zh-Hans": "独立", "zh-Hant": "獨立", "en-US": "Indie"},
        "subtitle": "Top tracks · indie",
        "fetcher": "lastfm_tag",
        "fetcher_args": {"tag": "indie", "limit": 60},
    },
    {
        "id": "tag-hiphop",
        "title": {"zh-Hans": "嘻哈", "zh-Hant": "嘻哈", "en-US": "Hip-Hop"},
        "subtitle": "Top tracks · hip-hop",
        "fetcher": "lastfm_tag",
        "fetcher_args": {"tag": "hip-hop", "limit": 60},
    },
    {
        "id": "tag-rnb",
        "title": {"zh-Hans": "R&B / 灵魂乐", "zh-Hant": "R&B / 靈魂樂", "en-US": "R&B / Soul"},
        "subtitle": "Top tracks · rnb",
        "fetcher": "lastfm_tag",
        "fetcher_args": {"tag": "rnb", "limit": 60},
    },
    {
        "id": "audius-monthly",
        "title": {"zh-Hans": "本月新声", "zh-Hant": "本月新聲", "en-US": "This Month on Audius"},
        "subtitle": "Audius monthly trending",
        "fetcher": "audius_trending_monthly",
        "fetcher_args": {"limit": 60},
    },
    {
        "id": "deezer-chart",
        "title": {"zh-Hans": "环球榜单", "zh-Hant": "環球榜單", "en-US": "Worldwide Charts"},
        "subtitle": "Powered by Deezer",
        "fetcher": "deezer_chart",
        "fetcher_args": {"limit": 60},
    },
]


def main() -> None:
    api_key = os.environ.get("LASTFM_API_KEY", "").strip()
    if not api_key:
        raise SystemExit("LASTFM_API_KEY environment variable is required.")

    station = load_json(CONFIG_PATH)
    edition_date = datetime.now(UTC).date()
    generated_at = now_utc()

    random.seed(f"home-{edition_date.isoformat()}")
    HOME_DIR.mkdir(parents=True, exist_ok=True)

    print(f"[home] building edition {edition_date.isoformat()}")

    tags = build_tag_cloud(api_key=api_key, limit=TAG_LIMIT)
    print(f"[home] resolved {len(tags)} tags")

    sections: list[dict[str, Any]] = []
    for definition in SECTION_DEFINITIONS:
        try:
            section = build_section(
                definition=definition,
                api_key=api_key,
                station=station,
            )
        except Exception as exc:  # noqa: BLE001 - log per-section failure, continue
            print(
                f"[home] section {definition['id']} failed: {exc!r} -- skipping",
                flush=True,
            )
            continue
        if section is None:
            print(f"[home] section {definition['id']} produced no tracks -- skipping")
            continue
        sections.append(section)
        print(f"[home] section {definition['id']} -> {len(section['tracks'])} tracks")

    if not sections:
        raise SystemExit("Home build produced zero sections.")

    top_playlist = build_top_playlist(api_key=api_key)
    if top_playlist is not None:
        print(f"[home] top playlist -> {len(top_playlist['tracks'])} tracks")
    else:
        print("[home] top playlist unavailable -- skipping")

    payload = {
        "schemaVersion": 1,
        "generatorVersion": GENERATOR_VERSION,
        "generatedAt": iso_z(generated_at),
        "editionDate": edition_date.isoformat(),
        "tags": tags,
        "sections": sections,
    }
    if top_playlist is not None:
        payload["topPlaylist"] = top_playlist

    archive_path = HOME_DIR / f"home_recommendations-{edition_date.isoformat()}.json"
    write_json(archive_path, payload)
    write_json(LATEST_HOME_PATH, payload)
    print(f"[home] wrote {archive_path.relative_to(ROOT)} and {LATEST_HOME_PATH.relative_to(ROOT)}")


def build_tag_cloud(api_key: str, limit: int) -> list[dict[str, Any]]:
    payload = fetch_json(
        LASTFM_ENDPOINT,
        {
            "method": "chart.gettoptags",
            "api_key": api_key,
            "format": "json",
            "limit": str(limit),
        },
    )
    rows = payload.get("tags", {}).get("tag", []) or []
    if not isinstance(rows, list):
        return []

    tags: list[dict[str, Any]] = []
    max_count = 0
    for row in rows:
        if not isinstance(row, dict):
            continue
        name = str(row.get("name", "")).strip()
        if not name:
            continue
        try:
            count = int(row.get("taggings") or row.get("count") or 0)
        except (TypeError, ValueError):
            count = 0
        max_count = max(max_count, count)
        tags.append({"name": name, "count": count})

    if max_count <= 0:
        for tag in tags:
            tag["weight"] = 1.0
    else:
        for tag in tags:
            tag["weight"] = round(tag["count"] / max_count, 4)
    return tags[:limit]


def build_section(
    definition: dict[str, Any],
    api_key: str,
    station: dict[str, Any],
) -> dict[str, Any] | None:
    fetcher_kind = definition["fetcher"]
    args = dict(definition.get("fetcher_args", {}))
    raw_candidates: list[CandidateTrack]

    if fetcher_kind == "lastfm_global":
        raw_candidates = fetch_lastfm_global(
            api_key=api_key, limit=int(args["limit"]), weight=1.0
        )
    elif fetcher_kind == "lastfm_tag":
        raw_candidates = fetch_lastfm_tag(
            api_key=api_key,
            tag=str(args["tag"]),
            limit=int(args["limit"]),
            weight=1.0,
        )
    elif fetcher_kind == "audius_trending":
        raw_candidates = fetch_audius_trending(limit=int(args["limit"]), weight=1.0)
    elif fetcher_kind == "audius_trending_monthly":
        raw_candidates = fetch_audius_trending_monthly(
            limit=int(args["limit"]), weight=1.0
        )
    elif fetcher_kind == "audius_genre":
        raw_candidates = fetch_audius_genre_trending(
            genre=str(args["genre"]),
            limit=int(args["limit"]),
            weight=1.0,
        )
    elif fetcher_kind == "deezer_chart":
        raw_candidates = fetch_deezer_chart(
            limit=int(args["limit"]),
            weight=1.0,
            genre_id=int(args.get("genre_id", 0)),
        )
    elif fetcher_kind == "itunes_rss":
        raw_candidates = fetch_itunes_rss(limit=int(args["limit"]), weight=1.0)
    else:
        raise ValueError(f"Unknown fetcher kind: {fetcher_kind}")

    pool = [c for c in raw_candidates if c.title and c.artist]
    if not pool:
        return None

    if len(pool) <= SECTION_TRACK_LIMIT:
        picks = pool
    else:
        picks = random.sample(pool, SECTION_TRACK_LIMIT)

    tracks_payload = [serialize_candidate(c) for c in picks]
    return {
        "id": definition["id"],
        "title": definition["title"],
        "subtitle": definition.get("subtitle"),
        "tracks": tracks_payload,
    }


def serialize_candidate(candidate: CandidateTrack) -> dict[str, Any]:
    audio_url = candidate.audio_url
    audio_provider = candidate.audio_provider
    provider_track_id = candidate.provider_track_id

    if audio_provider == "audius" and provider_track_id and not audio_url:
        audio_url = audius_stream_endpoint(provider_track_id)

    cover_url = candidate.cover_url
    if cover_url and any(h in cover_url for h in LASTFM_PLACEHOLDER_HASHES):
        cover_url = None

    return {
        "title": candidate.title,
        "artist": candidate.artist,
        "album": candidate.album or "",
        "durationMs": candidate.duration_ms,
        "coverUrl": cover_url,
        "audioUrl": audio_url,
        "audioProvider": audio_provider,
        "providerTrackId": provider_track_id,
        "sourceTags": sorted(candidate.source_tags),
    }


def build_top_playlist(api_key: str) -> dict[str, Any] | None:
    """Today's Top 10 — Last.fm global chart's first 10 tracks, in rank order.

    Independent of `sections` (which is randomized). Stable rank ordering is
    important so the banner's "Top 10" label is honest.
    """
    try:
        candidates = fetch_lastfm_global(
            api_key=api_key, limit=TOP_PLAYLIST_LIMIT, weight=1.0
        )
    except Exception as exc:  # noqa: BLE001
        print(f"[home] top playlist fetch failed: {exc!r}", flush=True)
        return None

    pool = [c for c in candidates if c.title and c.artist][:TOP_PLAYLIST_LIMIT]
    if not pool:
        return None

    return {
        "id": "daily-top-10",
        "title": {
            "zh-Hans": "今日 Top 10",
            "zh-Hant": "今日 Top 10",
            "en-US": "Today's Top 10",
        },
        "subtitle": "Most played worldwide today",
        "tracks": [serialize_candidate(c) for c in pool],
    }


if __name__ == "__main__":
    main()
