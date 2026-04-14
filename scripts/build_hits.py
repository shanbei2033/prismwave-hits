from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlencode
from urllib.request import Request, urlopen

UTC = timezone.utc
GENERATOR_VERSION = "prismwave-hits/0.2.0"
LASTFM_ENDPOINT = "https://ws.audioscrobbler.com/2.0/"
AUDIUS_API_BASE = "https://api.audius.co/v1"
ROOT = Path(__file__).resolve().parents[1]
CONFIG_PATH = ROOT / "config" / "station.json"
REGIONS_PATH = ROOT / "config" / "lastfm_regions.json"
DEMO_TRACKS_PATH = ROOT / "data" / "demo_tracks.json"
LATEST_PATH = ROOT / "latest.json"
SCHEDULES_DIR = ROOT / "schedules"

AUDIO_VARIANT_KEYWORDS = {
    "remix",
    "cover",
    "edit",
    "live",
    "mashup",
    "bootleg",
    "version",
    "vip",
    "flip",
    "instrumental",
    "karaoke",
    "nightcore",
    "rework",
}


@dataclass
class CandidateTrack:
    title: str
    artist: str
    album: str = ""
    duration_ms: int = 210000
    score: float = 0.0
    audio_url: str | None = None
    audio_provider: str | None = None
    provider_track_id: str | None = None
    cover_url: str | None = None
    isrc: str | None = None
    rank_signals: dict[str, int] = field(default_factory=dict)
    source_tags: set[str] = field(default_factory=set)
    title_variants: set[str] = field(default_factory=set)
    artist_variants: set[str] = field(default_factory=set)


def main() -> None:
    station = load_json(CONFIG_PATH)
    edition_date = resolve_target_date(station)
    generated_at = now_utc()

    merged_candidates, source_snapshot = load_candidate_pool(station)
    source_snapshot.extend(resolve_playable_sources(station, merged_candidates))
    ranked_candidates = rank_candidates(merged_candidates.values())
    if not ranked_candidates:
        raise SystemExit("No candidates available for schedule generation.")

    schedule_candidates, schedule_pool_snapshot = select_schedule_candidates(
        station=station,
        ranked_candidates=ranked_candidates,
    )
    source_snapshot.append(schedule_pool_snapshot)
    if not schedule_candidates:
        raise SystemExit("No schedule candidates available after playback filtering.")

    service_windows, off_air_windows = build_daily_windows(
        edition_date=edition_date,
        off_air_specs=station["off_air_windows"],
    )

    schedule = build_schedule(
        station=station,
        edition_date=edition_date,
        generated_at=generated_at,
        service_windows=service_windows,
        off_air_windows=off_air_windows,
        ranked_candidates=schedule_candidates,
        source_snapshot=source_snapshot,
    )

    schedule_path = SCHEDULES_DIR / f"{edition_date.isoformat()}.json"
    write_json(schedule_path, schedule)

    repo = station["repository"]
    schedule_rel_path = f"schedules/{edition_date.isoformat()}.json"
    latest = {
        "schema_version": station["schema_version"],
        "station_id": station["station_id"],
        "timezone": station["timezone"],
        "generated_at": iso_z(generated_at),
        "generator_version": GENERATOR_VERSION,
        "active_edition_date": edition_date.isoformat(),
        "schedule_path": schedule_rel_path,
        "schedule_url": raw_url(
            owner=repo["owner"],
            repo=repo["name"],
            branch=repo["branch"],
            path=schedule_rel_path,
        ),
        "service_windows": format_windows(service_windows),
        "off_air_windows": format_windows(off_air_windows),
        "source_snapshot": source_snapshot,
        "track_count": len(schedule["tracks"]),
        "playable_track_count": count_playable_tracks(schedule["tracks"]),
        "reserve_track_count": len(schedule["reserve_tracks"]),
    }
    write_json(LATEST_PATH, latest)


def load_candidate_pool(
    station: dict[str, Any],
) -> tuple[dict[str, CandidateTrack], list[dict[str, Any]]]:
    merged: dict[str, CandidateTrack] = {}
    source_snapshot: list[dict[str, Any]] = []

    lastfm_api_key = os.environ.get("LASTFM_API_KEY", "").strip()
    lastfm_cfg = station["lastfm"]

    if lastfm_api_key:
        try:
            global_tracks = fetch_lastfm_global(
                api_key=lastfm_api_key,
                limit=int(lastfm_cfg["global_limit"]),
                weight=float(station["source_weights"]["lastfm_global"]),
            )
            merge_candidates(merged, global_tracks)
            source_snapshot.append(
                {
                    "source": "lastfm_global",
                    "status": "ok",
                    "candidate_count": len(global_tracks),
                }
            )
        except (HTTPError, URLError, TimeoutError, ValueError) as error:
            source_snapshot.append(
                {
                    "source": "lastfm_global",
                    "status": f"error:{type(error).__name__}",
                    "candidate_count": 0,
                }
            )

        for region in load_json(REGIONS_PATH):
            country = region["country"]
            try:
                region_tracks = fetch_lastfm_region(
                    api_key=lastfm_api_key,
                    country=country,
                    limit=int(lastfm_cfg["region_limit"]),
                    weight=float(region["weight"]),
                )
                merge_candidates(merged, region_tracks)
                source_snapshot.append(
                    {
                        "source": "lastfm_geo",
                        "scope": country,
                        "status": "ok",
                        "candidate_count": len(region_tracks),
                    }
                )
            except (HTTPError, URLError, TimeoutError, ValueError) as error:
                source_snapshot.append(
                    {
                        "source": "lastfm_geo",
                        "scope": country,
                        "status": f"error:{type(error).__name__}",
                        "candidate_count": 0,
                    }
                )
    else:
        source_snapshot.append(
            {
                "source": "lastfm_global",
                "status": "missing_api_key",
                "candidate_count": 0,
            }
        )

    audius_cfg = station.get("audius", {})
    try:
        audius_tracks = fetch_audius_trending(
            limit=int(audius_cfg.get("trending_limit", 180)),
            weight=float(station["source_weights"].get("audius_trending", 0.32)),
        )
        merge_candidates(merged, audius_tracks)
        source_snapshot.append(
            {
                "source": "audius_trending",
                "status": "ok",
                "candidate_count": len(audius_tracks),
                "playable_count": count_candidate_audio(audius_tracks),
            }
        )
    except (HTTPError, URLError, TimeoutError, ValueError) as error:
        source_snapshot.append(
            {
                "source": "audius_trending",
                "status": f"error:{type(error).__name__}",
                "candidate_count": 0,
                "playable_count": 0,
            }
        )

    if not merged:
        bootstrap_tracks = load_bootstrap_tracks(
            weight=float(station["source_weights"]["bootstrap_seed"])
        )
        merge_candidates(merged, bootstrap_tracks)
        source_snapshot.append(
            {
                "source": "bootstrap_seed",
                "status": "ok",
                "candidate_count": len(bootstrap_tracks),
                "playable_count": count_candidate_audio(bootstrap_tracks),
            }
        )

    return merged, source_snapshot


def load_bootstrap_tracks(weight: float) -> list[CandidateTrack]:
    demo_rows = load_json(DEMO_TRACKS_PATH)
    total = max(len(demo_rows), 1)
    candidates: list[CandidateTrack] = []
    for index, row in enumerate(demo_rows, start=1):
        score = rank_score(index, total) * weight
        candidates.append(
            CandidateTrack(
                title=row["title"].strip(),
                artist=row["artist"].strip(),
                album=row.get("album", "").strip(),
                duration_ms=clamp_duration_ms(int(row.get("duration_ms", 210000))),
                score=score,
                audio_url=(row.get("audio_url") or "").strip() or None,
                audio_provider=(row.get("audio_provider") or "").strip() or None,
                provider_track_id=(row.get("provider_track_id") or "").strip() or None,
                rank_signals={"bootstrap_seed": index},
                source_tags={"bootstrap_seed"},
                title_variants={row["title"].strip()},
                artist_variants={row["artist"].strip()},
            )
        )
    return candidates


def fetch_lastfm_global(api_key: str, limit: int, weight: float) -> list[CandidateTrack]:
    payload = fetch_json(
        LASTFM_ENDPOINT,
        {
            "method": "chart.gettoptracks",
            "api_key": api_key,
            "format": "json",
            "limit": str(limit),
        },
    )
    return parse_lastfm_tracks(
        rows=payload.get("tracks", {}).get("track", []),
        source_label="lastfm_global",
        weight=weight,
        limit=limit,
    )


def fetch_lastfm_region(
    api_key: str,
    country: str,
    limit: int,
    weight: float,
) -> list[CandidateTrack]:
    payload = fetch_json(
        LASTFM_ENDPOINT,
        {
            "method": "geo.gettoptracks",
            "api_key": api_key,
            "country": country,
            "format": "json",
            "limit": str(limit),
        },
    )
    source_label = f"lastfm_geo:{country}"
    return parse_lastfm_tracks(
        rows=payload.get("tracks", {}).get("track", []),
        source_label=source_label,
        weight=weight,
        limit=limit,
    )


def fetch_audius_trending(limit: int, weight: float) -> list[CandidateTrack]:
    payload = fetch_json(
        f"{AUDIUS_API_BASE}/tracks/trending",
        {
            "time": "week",
            "limit": str(limit),
        },
    )
    rows = payload.get("data", [])
    if not isinstance(rows, list):
        raise ValueError("Unexpected Audius trending payload.")
    return parse_audius_tracks(
        rows=rows,
        source_label="audius_trending",
        weight=weight,
        limit=limit,
    )


def parse_lastfm_tracks(
    rows: list[dict[str, Any]],
    source_label: str,
    weight: float,
    limit: int,
) -> list[CandidateTrack]:
    parsed: list[CandidateTrack] = []
    for index, row in enumerate(rows, start=1):
        title = str(row.get("name", "")).strip()
        artist_data = row.get("artist", {})
        if isinstance(artist_data, dict):
            artist = str(artist_data.get("name", "")).strip()
        else:
            artist = str(artist_data).strip()
        if not title or not artist:
            continue

        duration_seconds = safe_int(row.get("duration"))
        cover_url = extract_lastfm_image(row.get("image", []))
        parsed.append(
            CandidateTrack(
                title=title,
                artist=artist,
                duration_ms=clamp_duration_ms(
                    duration_seconds * 1000 if duration_seconds else 210000
                ),
                score=rank_score(index, limit) * weight,
                cover_url=cover_url,
                rank_signals={source_label: index},
                source_tags={source_label},
                title_variants={title},
                artist_variants={artist},
            )
        )
    return parsed


def parse_audius_tracks(
    rows: list[dict[str, Any]],
    source_label: str,
    weight: float,
    limit: int,
) -> list[CandidateTrack]:
    parsed: list[CandidateTrack] = []
    total = max(limit, len(rows), 1)
    for index, row in enumerate(rows, start=1):
        candidate = candidate_from_audius_row(
            row=row,
            score=rank_score(index, total) * weight,
            source_label=source_label,
        )
        if candidate is not None:
            parsed.append(candidate)
    return parsed


def candidate_from_audius_row(
    row: dict[str, Any],
    score: float,
    source_label: str,
) -> CandidateTrack | None:
    track_id = str(row.get("id", "")).strip()
    raw_title = str(row.get("title", "")).strip()
    uploader = extract_audius_uploader_name(row)
    if not track_id or not raw_title:
        return None

    title, artist = infer_audius_title_artist(raw_title, uploader)
    if not title or not artist:
        return None

    duration_seconds = safe_int(row.get("duration"))
    cover_url = extract_audius_artwork_url(row.get("artwork"))
    title_variants = {raw_title, title}
    artist_variants = {artist}
    if uploader:
        artist_variants.add(uploader)
    is_streamable = bool(row.get("is_streamable", True)) and bool(
        row.get("is_available", True)
    )

    return CandidateTrack(
        title=title,
        artist=artist,
        duration_ms=clamp_duration_ms(duration_seconds * 1000 if duration_seconds else 210000),
        score=score,
        audio_url=audius_stream_endpoint(track_id) if is_streamable else None,
        audio_provider="audius" if is_streamable else None,
        provider_track_id=track_id if is_streamable else None,
        cover_url=cover_url,
        rank_signals={source_label: safe_int(row.get("play_count")) or 0},
        source_tags={source_label, "audius"},
        title_variants={item for item in title_variants if item},
        artist_variants={item for item in artist_variants if item},
    )


def resolve_playable_sources(
    station: dict[str, Any],
    merged_candidates: dict[str, CandidateTrack],
) -> list[dict[str, Any]]:
    audius_cfg = station.get("audius", {})
    candidate_resolution_limit = int(audius_cfg.get("candidate_resolution_limit", 96))
    search_limit = int(audius_cfg.get("search_limit", 8))
    min_score = int(audius_cfg.get("resolution_min_score", 82))

    if candidate_resolution_limit <= 0:
        return [
            {
                "source": "audius_match",
                "status": "disabled",
                "scanned_count": 0,
                "resolved_count": 0,
                "playable_count": count_candidate_audio(merged_candidates.values()),
            }
        ]

    unresolved = [
        item
        for item in rank_candidates(merged_candidates.values())
        if not item.audio_url
    ]
    scanned = 0
    resolved = 0
    errors = 0

    for candidate in unresolved:
        if scanned >= candidate_resolution_limit:
            break
        scanned += 1
        try:
            match = resolve_candidate_with_audius(
                candidate=candidate,
                search_limit=search_limit,
                min_score=min_score,
            )
        except (HTTPError, URLError, TimeoutError, ValueError):
            errors += 1
            continue

        if match is None:
            continue

        candidate.audio_url = match.audio_url
        candidate.audio_provider = match.audio_provider
        candidate.provider_track_id = match.provider_track_id
        candidate.cover_url = candidate.cover_url or match.cover_url
        candidate.source_tags.update(match.source_tags)
        candidate.title_variants.update(match.title_variants)
        candidate.artist_variants.update(match.artist_variants)
        resolved += 1

    status = "ok"
    if errors > 0 and resolved > 0:
        status = f"partial:{errors}_errors"
    elif errors > 0:
        status = f"error:{errors}_errors"

    return [
        {
            "source": "audius_match",
            "status": status,
            "scanned_count": scanned,
            "resolved_count": resolved,
            "playable_count": count_candidate_audio(merged_candidates.values()),
        }
    ]


def resolve_candidate_with_audius(
    candidate: CandidateTrack,
    search_limit: int,
    min_score: int,
) -> CandidateTrack | None:
    payload = fetch_json(
        f"{AUDIUS_API_BASE}/tracks/search",
        {
            "query": f"{candidate.title} {candidate.artist}".strip(),
            "limit": str(search_limit),
        },
    )
    rows = payload.get("data", [])
    if not isinstance(rows, list):
        raise ValueError("Unexpected Audius search payload.")

    best_match: CandidateTrack | None = None
    best_score = -9999
    for row in rows:
        if not isinstance(row, dict):
            continue
        matched = candidate_from_audius_row(
            row=row,
            score=0.0,
            source_label="audius_match",
        )
        if matched is None:
            continue
        score = score_audius_candidate_match(
            candidate=candidate,
            matched=matched,
            raw_row=row,
        )
        if score > best_score:
            best_score = score
            best_match = matched

    if best_match is None or best_score < min_score:
        return None
    return best_match


def score_audius_candidate_match(
    candidate: CandidateTrack,
    matched: CandidateTrack,
    raw_row: dict[str, Any],
) -> int:
    candidate_title_key = normalize_text(candidate.title)
    candidate_artist_key = normalize_text(candidate.artist)
    matched_title_key = normalize_text(matched.title)
    matched_artist_key = normalize_text(matched.artist)
    raw_title_key = normalize_text(str(raw_row.get("title", "")))
    uploader_key = normalize_text(extract_audius_uploader_name(raw_row))

    score = 0
    score += string_match_score(candidate_title_key, matched_title_key, exact=60, partial=28)
    if score < 24:
        score += string_match_score(candidate_title_key, raw_title_key, exact=40, partial=18)

    score += max(
        string_match_score(candidate_artist_key, matched_artist_key, exact=44, partial=18),
        string_match_score(candidate_artist_key, uploader_key, exact=18, partial=8),
    )

    if candidate.duration_ms > 0 and matched.duration_ms > 0:
        delta_seconds = abs(candidate.duration_ms - matched.duration_ms) / 1000
        if delta_seconds <= 2:
            score += 18
        elif delta_seconds <= 5:
            score += 12
        elif delta_seconds <= 10:
            score += 6

    if not bool(raw_row.get("is_streamable", True)):
        score -= 1000
    if not bool(raw_row.get("is_available", True)):
        score -= 1000

    score -= variant_penalty(
        source_title=str(raw_row.get("title", "")),
        requested_title=candidate.title,
    )
    return score


def rank_candidates(candidates: Any) -> list[CandidateTrack]:
    return sorted(
        list(candidates),
        key=lambda item: (-item.score, normalize_text(item.artist), normalize_text(item.title)),
    )


def select_schedule_candidates(
    station: dict[str, Any],
    ranked_candidates: list[CandidateTrack],
) -> tuple[list[CandidateTrack], dict[str, Any]]:
    schedule_cfg = station["schedule"]
    prefer_playable = bool(schedule_cfg.get("prefer_playable_candidates", True))
    min_playable_pool_size = int(schedule_cfg.get("min_playable_pool_size", 96))

    playable = [item for item in ranked_candidates if item.audio_url]
    if prefer_playable and len(playable) >= min_playable_pool_size:
        return playable, {
            "source": "schedule_pool",
            "status": "playable_only",
            "candidate_count": len(playable),
            "playable_count": len(playable),
        }

    if playable:
        mixed = playable + [item for item in ranked_candidates if not item.audio_url]
        return mixed, {
            "source": "schedule_pool",
            "status": "mixed_priority",
            "candidate_count": len(mixed),
            "playable_count": len(playable),
        }

    return ranked_candidates, {
        "source": "schedule_pool",
        "status": "no_playable_candidates",
        "candidate_count": len(ranked_candidates),
        "playable_count": 0,
    }


def fetch_json(base_url: str, params: dict[str, str]) -> dict[str, Any]:
    query = urlencode(params)
    request = Request(
        f"{base_url}?{query}" if query else base_url,
        headers={
            "User-Agent": GENERATOR_VERSION,
            "Accept": "application/json",
        },
    )
    with urlopen(request, timeout=15) as response:
        return json.loads(response.read().decode("utf-8"))


def build_schedule(
    station: dict[str, Any],
    edition_date: date,
    generated_at: datetime,
    service_windows: list[tuple[datetime, datetime, str]],
    off_air_windows: list[tuple[datetime, datetime, str]],
    ranked_candidates: list[CandidateTrack],
    source_snapshot: list[dict[str, Any]],
) -> dict[str, Any]:
    schedule_cfg = station["schedule"]
    repeat_track_gap_slots = int(schedule_cfg["repeat_track_gap_slots"])
    repeat_artist_gap_slots = int(schedule_cfg["repeat_artist_gap_slots"])
    min_tail_fill_ms = int(schedule_cfg["min_tail_fill_ms"])
    reserve_track_count = int(schedule_cfg["reserve_track_count"])
    lyrics_order = station["lyrics"]["preferred_order"]

    last_track_slot: dict[str, int] = {}
    last_artist_slot: dict[str, int] = {}
    tracks: list[dict[str, Any]] = []
    slot = 1

    for window_start, window_end, window_label in service_windows:
        cursor = window_start
        while cursor < window_end:
            remaining_ms = int((window_end - cursor).total_seconds() * 1000)
            if remaining_ms < min_tail_fill_ms:
                break

            candidate = pick_next_candidate(
                ranked_candidates=ranked_candidates,
                remaining_ms=remaining_ms,
                slot=slot,
                last_track_slot=last_track_slot,
                last_artist_slot=last_artist_slot,
                repeat_track_gap_slots=repeat_track_gap_slots,
                repeat_artist_gap_slots=repeat_artist_gap_slots,
            )
            if candidate is None:
                break

            start_at = cursor
            end_at = start_at + timedelta(milliseconds=candidate.duration_ms)
            if end_at > window_end:
                break

            track_key = track_identity(candidate)
            artist_key = normalize_text(candidate.artist)
            last_track_slot[track_key] = slot
            last_artist_slot[artist_key] = slot

            tracks.append(
                {
                    "slot": slot,
                    "station_track_id": f"{edition_date.isoformat()}-{slot:04d}",
                    "window": window_label,
                    "start_at": iso_z(start_at),
                    "end_at": iso_z(end_at),
                    "duration_ms": candidate.duration_ms,
                    "title": candidate.title,
                    "artist": candidate.artist,
                    "album": candidate.album,
                    "audio_url": candidate.audio_url,
                    "audio_provider": candidate.audio_provider,
                    "provider_track_id": candidate.provider_track_id,
                    "isrc": candidate.isrc,
                    "cover_url": candidate.cover_url,
                    "score": round(candidate.score, 6),
                    "source_tags": sorted(candidate.source_tags),
                    "rank_signals": candidate.rank_signals,
                    "search_hints": {
                        "query": build_search_query(candidate),
                        "title_variants": sorted(candidate.title_variants),
                        "artist_variants": sorted(candidate.artist_variants),
                    },
                    "lyrics_hints": {
                        "preferred_order": lyrics_order,
                    },
                }
            )
            slot += 1
            cursor = end_at

    reserve_tracks = [
        {
            "title": item.title,
            "artist": item.artist,
            "album": item.album,
            "duration_ms": item.duration_ms,
            "audio_url": item.audio_url,
            "audio_provider": item.audio_provider,
            "provider_track_id": item.provider_track_id,
            "cover_url": item.cover_url,
            "score": round(item.score, 6),
        }
        for item in ranked_candidates[:reserve_track_count]
    ]

    generation_mode = resolve_generation_mode(ranked_candidates)

    return {
        "schema_version": station["schema_version"],
        "station_id": station["station_id"],
        "edition_date": edition_date.isoformat(),
        "timezone": station["timezone"],
        "generated_at": iso_z(generated_at),
        "generator_version": GENERATOR_VERSION,
        "generation_mode": generation_mode,
        "play_policy": {
            "mode": "strict",
            "join_mid_track": True,
            "allowed_drift_ms": 1200,
        },
        "service_windows": format_windows(service_windows),
        "off_air_windows": format_windows(off_air_windows),
        "source_snapshot": source_snapshot,
        "tracks": tracks,
        "reserve_tracks": reserve_tracks,
        "playable_track_count": count_playable_tracks(tracks),
        "playable_candidate_count": count_candidate_audio(ranked_candidates),
    }


def resolve_generation_mode(ranked_candidates: list[CandidateTrack]) -> str:
    has_lastfm = any(
        tag.startswith("lastfm_")
        for track in ranked_candidates
        for tag in track.source_tags
    )
    has_audius = any("audius" in track.source_tags for track in ranked_candidates)
    playable_count = count_candidate_audio(ranked_candidates)

    if has_lastfm and has_audius and playable_count > 0:
        return "lastfm_plus_audius_playable"
    if has_audius and playable_count > 0:
        return "audius_playable_only"
    if has_lastfm:
        return "lastfm_metadata_only"
    return "bootstrap_seed_only"


def pick_next_candidate(
    ranked_candidates: list[CandidateTrack],
    remaining_ms: int,
    slot: int,
    last_track_slot: dict[str, int],
    last_artist_slot: dict[str, int],
    repeat_track_gap_slots: int,
    repeat_artist_gap_slots: int,
) -> CandidateTrack | None:
    candidate = scan_candidate_pool(
        ranked_candidates=ranked_candidates,
        remaining_ms=remaining_ms,
        slot=slot,
        last_track_slot=last_track_slot,
        last_artist_slot=last_artist_slot,
        repeat_track_gap_slots=repeat_track_gap_slots,
        repeat_artist_gap_slots=repeat_artist_gap_slots,
    )
    if candidate is not None:
        return candidate

    return scan_candidate_pool(
        ranked_candidates=ranked_candidates,
        remaining_ms=remaining_ms,
        slot=slot,
        last_track_slot=last_track_slot,
        last_artist_slot=last_artist_slot,
        repeat_track_gap_slots=max(12, repeat_track_gap_slots // 2),
        repeat_artist_gap_slots=max(6, repeat_artist_gap_slots // 2),
    )


def scan_candidate_pool(
    ranked_candidates: list[CandidateTrack],
    remaining_ms: int,
    slot: int,
    last_track_slot: dict[str, int],
    last_artist_slot: dict[str, int],
    repeat_track_gap_slots: int,
    repeat_artist_gap_slots: int,
) -> CandidateTrack | None:
    fallback: CandidateTrack | None = None

    for candidate in ranked_candidates:
        if candidate.duration_ms > remaining_ms:
            continue
        if fallback is None:
            fallback = candidate

        track_key = track_identity(candidate)
        artist_key = normalize_text(candidate.artist)
        track_gap = slot - last_track_slot.get(track_key, -999999)
        artist_gap = slot - last_artist_slot.get(artist_key, -999999)

        if track_gap < repeat_track_gap_slots:
            continue
        if artist_gap < repeat_artist_gap_slots:
            continue
        return candidate

    return fallback


def build_daily_windows(
    edition_date: date,
    off_air_specs: list[dict[str, str]],
) -> tuple[list[tuple[datetime, datetime, str]], list[tuple[datetime, datetime, str]]]:
    day_start = datetime.combine(edition_date, time(0, 0), tzinfo=UTC)
    day_end = day_start + timedelta(days=1)

    off_air_windows: list[tuple[datetime, datetime, str]] = []
    for spec in off_air_specs:
        start_time = parse_hhmm(spec["start"])
        end_time = parse_hhmm(spec["end"])
        start_dt = datetime.combine(edition_date, start_time, tzinfo=UTC)
        end_dt = datetime.combine(edition_date, end_time, tzinfo=UTC)
        if end_dt <= start_dt:
            end_dt += timedelta(days=1)
        start_dt = max(start_dt, day_start)
        end_dt = min(end_dt, day_end)
        if start_dt < end_dt:
            off_air_windows.append((start_dt, end_dt, spec.get("label", "off_air")))

    off_air_windows.sort(key=lambda item: item[0])

    service_windows: list[tuple[datetime, datetime, str]] = []
    cursor = day_start
    for start_dt, end_dt, _ in off_air_windows:
        if cursor < start_dt:
            service_windows.append((cursor, start_dt, "on_air"))
        cursor = max(cursor, end_dt)
    if cursor < day_end:
        service_windows.append((cursor, day_end, "on_air"))

    return service_windows, off_air_windows


def merge_candidates(
    merged: dict[str, CandidateTrack],
    incoming: list[CandidateTrack],
) -> None:
    for candidate in incoming:
        key = track_identity(candidate)
        existing = merged.get(key)
        if existing is None:
            merged[key] = candidate
            continue

        existing.score += candidate.score
        existing.source_tags.update(candidate.source_tags)
        existing.title_variants.update(candidate.title_variants)
        existing.artist_variants.update(candidate.artist_variants)
        existing.rank_signals.update(candidate.rank_signals)

        if not existing.album and candidate.album:
            existing.album = candidate.album
        if existing.duration_ms == 210000 and candidate.duration_ms != 210000:
            existing.duration_ms = candidate.duration_ms
        if not existing.audio_url and candidate.audio_url:
            existing.audio_url = candidate.audio_url
        if not existing.audio_provider and candidate.audio_provider:
            existing.audio_provider = candidate.audio_provider
        if not existing.provider_track_id and candidate.provider_track_id:
            existing.provider_track_id = candidate.provider_track_id
        if not existing.cover_url and candidate.cover_url:
            existing.cover_url = candidate.cover_url
        if not existing.isrc and candidate.isrc:
            existing.isrc = candidate.isrc


def resolve_target_date(station: dict[str, Any]) -> date:
    target_date = os.environ.get("TARGET_DATE", "").strip()
    if target_date:
        return date.fromisoformat(target_date)
    return (now_utc() + timedelta(days=int(station["default_target_offset_days"]))).date()


def load_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )


def now_utc() -> datetime:
    return datetime.now(UTC).replace(microsecond=0)


def iso_z(value: datetime) -> str:
    return value.astimezone(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def parse_hhmm(value: str) -> time:
    hour_text, minute_text = value.split(":", maxsplit=1)
    return time(int(hour_text), int(minute_text))


def raw_url(owner: str, repo: str, branch: str, path: str) -> str:
    normalized = path.replace("\\", "/").lstrip("/")
    return f"https://raw.githubusercontent.com/{owner}/{repo}/{branch}/{normalized}"


def format_windows(
    windows: list[tuple[datetime, datetime, str]],
) -> list[dict[str, str]]:
    return [
        {
            "label": label,
            "start_at": iso_z(start_at),
            "end_at": iso_z(end_at),
        }
        for start_at, end_at, label in windows
    ]


def rank_score(rank: int, limit: int) -> float:
    normalized = (limit - rank + 1) / max(limit, 1)
    return normalized ** 1.35


def track_identity(candidate: CandidateTrack) -> str:
    return f"{normalize_text(candidate.title)}::{normalize_text(candidate.artist)}"


def normalize_text(value: str) -> str:
    simplified = value.lower()
    simplified = re.sub(r"\([^)]*\)", " ", simplified)
    simplified = re.sub(r"\[[^]]*\]", " ", simplified)
    simplified = re.sub(r"\b(feat|ft|with)\b", " ", simplified)
    simplified = re.sub(r"_+", " ", simplified)
    simplified = re.sub(r"[^\w]+", " ", simplified, flags=re.UNICODE)
    return " ".join(simplified.split())


def string_match_score(
    left: str,
    right: str,
    *,
    exact: int,
    partial: int,
) -> int:
    if not left or not right:
        return 0
    if left == right:
        return exact
    if left in right or right in left:
        return partial
    return token_overlap_score(left, right)


def token_overlap_score(left: str, right: str) -> int:
    left_tokens = set(left.split())
    right_tokens = set(right.split())
    if not left_tokens or not right_tokens:
        return 0
    overlap = len(left_tokens & right_tokens) / max(len(left_tokens), len(right_tokens))
    if overlap >= 0.8:
        return 14
    if overlap >= 0.55:
        return 8
    return 0


def variant_penalty(source_title: str, requested_title: str) -> int:
    source_variants = detect_variant_keywords(source_title)
    requested_variants = detect_variant_keywords(requested_title)
    extra = source_variants - requested_variants
    if not extra:
        return 0
    if len(extra) == 1:
        return 28
    return 40


def detect_variant_keywords(value: str) -> set[str]:
    normalized = normalize_text(value)
    detected: set[str] = set()
    for keyword in AUDIO_VARIANT_KEYWORDS:
        keyword_key = normalize_text(keyword)
        if keyword_key and keyword_key in normalized:
            detected.add(keyword_key)
    return detected


def build_search_query(candidate: CandidateTrack) -> str:
    return " ".join(
        item
        for item in [candidate.title.strip(), candidate.artist.strip()]
        if item
    ).strip()


def safe_int(value: Any) -> int:
    try:
        return int(float(str(value)))
    except (TypeError, ValueError):
        return 0


def clamp_duration_ms(value: int) -> int:
    return min(max(value, 120000), 420000)


def extract_lastfm_image(rows: Any) -> str | None:
    if not isinstance(rows, list):
        return None
    for row in reversed(rows):
        if isinstance(row, dict):
            text = str(row.get("#text", "")).strip()
            if text:
                return text
    return None


def extract_audius_uploader_name(row: dict[str, Any]) -> str:
    user = row.get("user", {})
    if isinstance(user, dict):
        return str(user.get("name", "")).strip()
    return ""


def infer_audius_title_artist(raw_title: str, uploader: str) -> tuple[str, str]:
    title = raw_title.strip()
    artist = uploader.strip()
    if " - " not in title:
        return title, artist or "Unknown Artist"

    left, right = [part.strip() for part in title.split(" - ", maxsplit=1)]
    uploader_key = normalize_text(uploader)
    left_key = normalize_text(left)

    should_split = False
    if uploader_key and (uploader_key in left_key or left_key in uploader_key):
        should_split = True
    if "," in left or "&" in left:
        should_split = True

    if should_split and left and right:
        return right, left
    return title, artist or "Unknown Artist"


def extract_audius_artwork_url(value: Any) -> str | None:
    if not isinstance(value, dict):
        return None
    for key in ("1000x1000", "480x480", "150x150"):
        text = str(value.get(key, "")).strip()
        if text:
            return text
    return None


def audius_stream_endpoint(track_id: str) -> str:
    return f"{AUDIUS_API_BASE}/tracks/{quote(track_id, safe='')}/stream"


def count_candidate_audio(candidates: Any) -> int:
    return sum(1 for candidate in candidates if getattr(candidate, "audio_url", None))


def count_playable_tracks(tracks: list[dict[str, Any]]) -> int:
    return sum(1 for item in tracks if str(item.get("audio_url") or "").strip())


if __name__ == "__main__":
    main()
