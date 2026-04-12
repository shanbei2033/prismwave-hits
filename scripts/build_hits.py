from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

UTC = timezone.utc
GENERATOR_VERSION = "prismwave-hits/0.1.0"
LASTFM_ENDPOINT = "https://ws.audioscrobbler.com/2.0/"
ROOT = Path(__file__).resolve().parents[1]
CONFIG_PATH = ROOT / "config" / "station.json"
REGIONS_PATH = ROOT / "config" / "lastfm_regions.json"
DEMO_TRACKS_PATH = ROOT / "data" / "demo_tracks.json"
LATEST_PATH = ROOT / "latest.json"
SCHEDULES_DIR = ROOT / "schedules"


@dataclass
class CandidateTrack:
    title: str
    artist: str
    album: str = ""
    duration_ms: int = 210000
    score: float = 0.0
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
    ranked_candidates = sorted(
        merged_candidates.values(),
        key=lambda item: (-item.score, normalize_text(item.artist), normalize_text(item.title)),
    )
    if not ranked_candidates:
        raise SystemExit("No candidates available for schedule generation.")

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
        ranked_candidates=ranked_candidates,
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

    bootstrap_tracks = load_bootstrap_tracks(
        weight=float(station["source_weights"]["bootstrap_seed"])
    )
    merge_candidates(merged, bootstrap_tracks)
    source_snapshot.append(
        {
            "source": "bootstrap_seed",
            "status": "ok",
            "candidate_count": len(bootstrap_tracks),
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


def fetch_json(base_url: str, params: dict[str, str]) -> dict[str, Any]:
    query = urlencode(params)
    request = Request(
        f"{base_url}?{query}",
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
                    "isrc": candidate.isrc,
                    "cover_url": candidate.cover_url,
                    "score": round(candidate.score, 6),
                    "source_tags": sorted(candidate.source_tags),
                    "rank_signals": candidate.rank_signals,
                    "search_hints": {
                        "query": f"{candidate.title} {candidate.artist}",
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
            "score": round(item.score, 6),
        }
        for item in ranked_candidates[:reserve_track_count]
    ]

    generation_mode = (
        "lastfm_plus_bootstrap"
        if any(tag.startswith("lastfm_") for track in ranked_candidates for tag in track.source_tags)
        else "bootstrap_seed_only"
    )

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
    }


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


if __name__ == "__main__":
    main()
