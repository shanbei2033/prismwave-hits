"""Generate PrismWave's daily online home recommendations.

The home payload is intentionally separate from the HITS radio schedule. It
builds a single Top 100 chart from several public trend sources, keeps cover
metadata in the JSON, and lets the app cache one file per Beijing date.

Output:
- home/home_recommendations-YYYY-MM-DD.json
- home/latest_home.json
"""

from __future__ import annotations

import os
from datetime import timedelta, timezone
from http.client import RemoteDisconnected
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError

from build_hits import (  # type: ignore
    CandidateTrack,
    audius_stream_endpoint,
    fetch_json,
    load_candidate_pool,
    load_json,
    normalize_text,
    now_utc,
    rank_candidates,
    resolve_playable_sources,
    safe_int,
    write_json,
    iso_z,
)

GENERATOR_VERSION = "prismwave-home/0.2.0"
SCHEMA_VERSION = 7
ROOT = Path(__file__).resolve().parents[1]
CONFIG_PATH = ROOT / "config" / "station.json"
HOME_DIR = ROOT / "home"
LATEST_HOME_PATH = HOME_DIR / "latest_home.json"
BEIJING = timezone(timedelta(hours=8), name="Asia/Shanghai")

TOP_PLAYLIST_LIMIT = 100
SECTION_TRACK_LIMIT = 20
METADATA_ENRICH_LIMIT = 140

ITUNES_SEARCH_URL = "https://itunes.apple.com/search"
DEEZER_SEARCH_URL = "https://api.deezer.com/search"

# Last.fm's placeholder artwork is a real image, but it renders as a generic
# glyph. Treat it as missing so downstream cover fallbacks can replace it.
LASTFM_PLACEHOLDER_HASHES = (
    "2a96cbd8b46e442fc41c2b86b821562f",
)


def main() -> None:
    station = load_json(CONFIG_PATH)
    generated_at = now_utc()
    edition_date = generated_at.astimezone(BEIJING).date()

    HOME_DIR.mkdir(parents=True, exist_ok=True)
    print(f"[home] building Beijing edition {edition_date.isoformat()}")

    merged_candidates, source_snapshot = load_candidate_pool(station)
    if should_resolve_audio():
        try:
            source_snapshot.extend(resolve_playable_sources(station, merged_candidates))
        except (HTTPError, URLError, TimeoutError, ValueError, RemoteDisconnected) as error:
            source_snapshot.append(
                {
                    "source": "audius_match",
                    "status": f"error:{type(error).__name__}",
                    "scanned_count": 0,
                    "resolved_count": 0,
                    "playable_count": count_existing_audio(merged_candidates.values()),
                }
            )
    else:
        source_snapshot.append(
            {
                "source": "audius_match",
                "status": "skipped:home_fast_path",
                "scanned_count": 0,
                "resolved_count": 0,
                "playable_count": count_existing_audio(merged_candidates.values()),
            }
        )
    ranked_candidates = rank_candidates(merged_candidates.values())
    if not ranked_candidates:
        raise SystemExit("Home build produced zero candidates.")

    enrich_missing_metadata(ranked_candidates[:METADATA_ENRICH_LIMIT])
    ranked_candidates = rank_candidates(ranked_candidates)
    top_candidates = ranked_candidates[:TOP_PLAYLIST_LIMIT]
    if len(top_candidates) < 20:
        raise SystemExit("Home build produced too few Top 100 candidates.")

    top_playlist = build_top_playlist(top_candidates)
    sections = build_sections(ranked_candidates)
    payload = {
        "schemaVersion": SCHEMA_VERSION,
        "generatorVersion": GENERATOR_VERSION,
        "generatedAt": iso_z(generated_at),
        "generatedAtBeijing": generated_at.astimezone(BEIJING)
        .replace(microsecond=0)
        .isoformat(),
        "editionDate": edition_date.isoformat(),
        "timezone": "Asia/Shanghai",
        "sourceSnapshot": source_snapshot,
        "tags": build_source_tags(ranked_candidates),
        "topPlaylist": top_playlist,
        "sections": sections,
        "albumRecommendations": [],
    }

    archive_path = HOME_DIR / f"home_recommendations-{edition_date.isoformat()}.json"
    write_json(archive_path, payload)
    write_json(LATEST_HOME_PATH, payload)
    print(
        "[home] wrote "
        f"{archive_path.relative_to(ROOT)} and {LATEST_HOME_PATH.relative_to(ROOT)}"
    )
    print(
        "[home] top100 "
        f"tracks={len(top_playlist['tracks'])} "
        f"with_cover={count_cover_urls(top_playlist['tracks'])} "
        f"with_audio={count_audio_urls(top_playlist['tracks'])}"
    )


def enrich_missing_metadata(candidates: list[CandidateTrack]) -> None:
    for candidate in candidates:
        if needs_cover_cleanup(candidate.cover_url):
            candidate.cover_url = None
        if candidate.cover_url and candidate.album and candidate.duration_ms != 210000:
            continue

        for lookup in (lookup_itunes_metadata, lookup_deezer_metadata):
            try:
                metadata = lookup(candidate)
            except (HTTPError, URLError, TimeoutError, ValueError, RemoteDisconnected):
                continue
            if metadata is None:
                continue
            apply_metadata(candidate, metadata)
            if candidate.cover_url and candidate.album and candidate.duration_ms != 210000:
                break


def lookup_itunes_metadata(candidate: CandidateTrack) -> dict[str, Any] | None:
    payload = fetch_json(
        ITUNES_SEARCH_URL,
        {
            "term": build_metadata_query(candidate),
            "media": "music",
            "entity": "song",
            "limit": "5",
        },
    )
    rows = payload.get("results", [])
    if not isinstance(rows, list):
        return None

    best: dict[str, Any] | None = None
    best_score = 0
    for row in rows:
        if not isinstance(row, dict):
            continue
        title = str(row.get("trackName", "")).strip()
        artist = str(row.get("artistName", "")).strip()
        score = metadata_match_score(
            candidate=candidate,
            title=title,
            artist=artist,
            duration_ms=safe_int(row.get("trackTimeMillis")),
        )
        if score > best_score:
            best_score = score
            best = row
    if best is None or best_score < 58:
        return None

    cover = str(best.get("artworkUrl100", "")).strip() or None
    if cover:
        cover = upgrade_itunes_cover_url(cover)
    return {
        "album": str(best.get("collectionName", "")).strip(),
        "duration_ms": safe_int(best.get("trackTimeMillis")),
        "cover_url": cover,
        "source_tag": "itunes_search",
        "rank_signal": safe_int(best.get("trackId")),
    }


def lookup_deezer_metadata(candidate: CandidateTrack) -> dict[str, Any] | None:
    payload = fetch_json(
        DEEZER_SEARCH_URL,
        {
            "q": build_metadata_query(candidate),
            "limit": "5",
        },
    )
    rows = payload.get("data", [])
    if not isinstance(rows, list):
        return None

    best: dict[str, Any] | None = None
    best_score = 0
    for row in rows:
        if not isinstance(row, dict):
            continue
        title = str(row.get("title", "")).strip()
        artist_data = row.get("artist", {})
        artist = (
            str(artist_data.get("name", "")).strip()
            if isinstance(artist_data, dict)
            else ""
        )
        score = metadata_match_score(
            candidate=candidate,
            title=title,
            artist=artist,
            duration_ms=safe_int(row.get("duration")) * 1000,
        )
        if score > best_score:
            best_score = score
            best = row
    if best is None or best_score < 54:
        return None

    album = best.get("album", {})
    album_name = ""
    cover = None
    if isinstance(album, dict):
        album_name = str(album.get("title", "")).strip()
        cover = (
            str(album.get("cover_xl", "")).strip()
            or str(album.get("cover_big", "")).strip()
            or str(album.get("cover_medium", "")).strip()
            or None
        )
    return {
        "album": album_name,
        "duration_ms": safe_int(best.get("duration")) * 1000,
        "cover_url": cover,
        "source_tag": "deezer_search",
        "rank_signal": safe_int(best.get("id")),
    }


def metadata_match_score(
    *,
    candidate: CandidateTrack,
    title: str,
    artist: str,
    duration_ms: int,
) -> int:
    candidate_title = normalize_text(candidate.title)
    candidate_artist = normalize_text(candidate.artist)
    matched_title = normalize_text(title)
    matched_artist = normalize_text(artist)
    if not matched_title or not matched_artist:
        return 0

    score = 0
    if matched_title == candidate_title:
        score += 58
    elif matched_title in candidate_title or candidate_title in matched_title:
        score += 30
    else:
        score += token_overlap_score(matched_title, candidate_title, maximum=24)

    if matched_artist == candidate_artist:
        score += 34
    elif matched_artist in candidate_artist or candidate_artist in matched_artist:
        score += 16
    else:
        score += token_overlap_score(matched_artist, candidate_artist, maximum=12)

    if duration_ms > 0 and candidate.duration_ms > 0:
        delta = abs(duration_ms - candidate.duration_ms)
        if delta <= 2500:
            score += 10
        elif delta <= 10000:
            score += 5
    return score


def token_overlap_score(left: str, right: str, *, maximum: int) -> int:
    left_tokens = set(left.split())
    right_tokens = set(right.split())
    if not left_tokens or not right_tokens:
        return 0
    overlap = len(left_tokens & right_tokens) / max(len(left_tokens), len(right_tokens))
    return round(maximum * overlap)


def apply_metadata(candidate: CandidateTrack, metadata: dict[str, Any]) -> None:
    album = str(metadata.get("album", "")).strip()
    cover_url = str(metadata.get("cover_url", "")).strip()
    duration_ms = safe_int(metadata.get("duration_ms"))
    source_tag = str(metadata.get("source_tag", "")).strip()

    if not candidate.album and album:
        candidate.album = album
    if not candidate.cover_url and cover_url:
        candidate.cover_url = cover_url
    if candidate.duration_ms == 210000 and duration_ms > 0:
        candidate.duration_ms = duration_ms
    if source_tag:
        candidate.source_tags.add(source_tag)
        signal = safe_int(metadata.get("rank_signal"))
        if signal > 0:
            candidate.rank_signals[source_tag] = signal


def build_top_playlist(candidates: list[CandidateTrack]) -> dict[str, Any]:
    return {
        "id": "daily-top-100",
        "title": {
            "zh-Hans": "今日趋势",
            "zh-Hant": "今日趨勢",
            "en-US": "Today's Trending",
        },
        "subtitle": "Global multi-platform Top 100",
        "tracks": [serialize_candidate(c) for c in candidates[:TOP_PLAYLIST_LIMIT]],
    }


def build_sections(ranked_candidates: list[CandidateTrack]) -> list[dict[str, Any]]:
    definitions = [
        (
            "global-hot",
            {"zh-Hans": "全球热门", "zh-Hant": "全球熱門", "en-US": "Global Hot"},
            "Top signals from Last.fm, Deezer, iTunes and Audius",
            lambda c: True,
        ),
        (
            "streamable-now",
            {"zh-Hans": "可直接播放", "zh-Hant": "可直接播放", "en-US": "Streamable Now"},
            "Resolved Audius streams",
            lambda c: bool(c.audio_url),
        ),
        (
            "world-charts",
            {"zh-Hans": "环球榜单", "zh-Hant": "環球榜單", "en-US": "World Charts"},
            "Deezer and iTunes chart signals",
            lambda c: has_any_source(c, ("deezer", "itunes")),
        ),
        (
            "listener-trends",
            {"zh-Hans": "听众趋势", "zh-Hant": "聽眾趨勢", "en-US": "Listener Trends"},
            "Last.fm global and regional charts",
            lambda c: has_any_source(c, ("lastfm_global", "lastfm_geo")),
        ),
        (
            "audius-trending",
            {"zh-Hans": "Audius 流行", "zh-Hant": "Audius 流行", "en-US": "Audius Trending"},
            "Independent streaming trends",
            lambda c: has_any_source(c, ("audius",)),
        ),
    ]

    sections: list[dict[str, Any]] = []
    for section_id, title, subtitle, predicate in definitions:
        tracks = [c for c in ranked_candidates if predicate(c)][:SECTION_TRACK_LIMIT]
        if len(tracks) < 4:
            continue
        sections.append(
            {
                "id": section_id,
                "title": title,
                "subtitle": subtitle,
                "tracks": [serialize_candidate(c) for c in tracks],
            }
        )
    return sections


def serialize_candidate(candidate: CandidateTrack) -> dict[str, Any]:
    audio_url = candidate.audio_url
    audio_provider = candidate.audio_provider
    provider_track_id = candidate.provider_track_id

    if audio_provider == "audius" and provider_track_id and not audio_url:
        audio_url = audius_stream_endpoint(provider_track_id)

    cover_url = candidate.cover_url
    if needs_cover_cleanup(cover_url):
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
        "rankSignals": dict(sorted(candidate.rank_signals.items())),
        "score": round(candidate.score, 6),
    }


def build_source_tags(candidates: list[CandidateTrack]) -> list[dict[str, Any]]:
    counts: dict[str, int] = {}
    for candidate in candidates:
        for tag in candidate.source_tags:
            root = tag.split(":", maxsplit=1)[0]
            counts[root] = counts.get(root, 0) + 1

    max_count = max(counts.values(), default=1)
    return [
        {
            "name": name,
            "count": count,
            "weight": round(count / max_count, 4),
        }
        for name, count in sorted(counts.items(), key=lambda item: (-item[1], item[0]))
    ][:40]


def build_metadata_query(candidate: CandidateTrack) -> str:
    return " ".join(
        item for item in [candidate.title.strip(), candidate.artist.strip()] if item
    )


def upgrade_itunes_cover_url(url: str) -> str:
    return (
        url.replace("100x100bb", "600x600bb")
        .replace("100x100-75", "600x600-75")
        .replace("60x60bb", "600x600bb")
    )


def has_any_source(candidate: CandidateTrack, needles: tuple[str, ...]) -> bool:
    return any(
        any(tag.startswith(needle) or needle in tag for tag in candidate.source_tags)
        for needle in needles
    )


def needs_cover_cleanup(cover_url: str | None) -> bool:
    if not cover_url:
        return False
    return any(marker in cover_url for marker in LASTFM_PLACEHOLDER_HASHES)


def count_cover_urls(tracks: list[dict[str, Any]]) -> int:
    return sum(1 for track in tracks if str(track.get("coverUrl") or "").strip())


def count_audio_urls(tracks: list[dict[str, Any]]) -> int:
    return sum(1 for track in tracks if str(track.get("audioUrl") or "").strip())


def count_existing_audio(candidates: Any) -> int:
    return sum(1 for candidate in candidates if getattr(candidate, "audio_url", None))


def should_resolve_audio() -> bool:
    value = os.environ.get("PRISMWAVE_HOME_RESOLVE_AUDIO", "").strip().lower()
    return value in {"1", "true", "yes", "on"}


if __name__ == "__main__":
    main()
