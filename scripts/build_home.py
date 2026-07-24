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
import random
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
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
    track_identity,
    write_json,
    iso_z,
)

GENERATOR_VERSION = "prismwave-home/0.5.0"
SCHEMA_VERSION = 8
ROOT = Path(__file__).resolve().parents[1]
CONFIG_PATH = ROOT / "config" / "station.json"
HOME_DIR = ROOT / "home"
LATEST_HOME_PATH = HOME_DIR / "latest_home.json"
BEIJING = timezone(timedelta(hours=8), name="Asia/Shanghai")

TOP_PLAYLIST_LIMIT = 100
SECTION_TRACK_LIMIT = 20
METADATA_ENRICH_LIMIT = 140
ARTIST_PER_PLAYLIST_MIN = 5   # Minimum tracks per artist in Top 100
ARTIST_PER_PLAYLIST_MAX = 8   # Maximum tracks per artist in Top 100
ARTIST_PER_SECTION_MIN = 3    # Minimum tracks per artist in sections
ARTIST_PER_SECTION_MAX = 5    # Maximum tracks per artist in sections
TOP_PLAYLIST_LOOKAHEAD = 30
TOP_PLAYLIST_MIN_ARTIST_GAP = 12
SECTION_LOOKAHEAD = 12
SECTION_MIN_ARTIST_GAP = 4
MIN_SECTION_TRACKS = 4
ROTATION_HISTORY_DAYS = 14
ROTATION_AGE_PENALTIES = {
    2: 0.18,
    3: 0.14,
    4: 0.10,
    5: 0.07,
    6: 0.04,
    7: 0.02,
    10: 0.01,
    14: 0.005,
}

ITUNES_SEARCH_URL = "https://itunes.apple.com/search"
DEEZER_SEARCH_URL = "https://api.deezer.com/search"

# Last.fm's placeholder artwork is a real image, but it renders as a generic
# glyph. Treat it as missing so downstream cover fallbacks can replace it.
LASTFM_PLACEHOLDER_HASHES = (
    "2a96cbd8b46e442fc41c2b86b821562f",
)


@dataclass(frozen=True)
class RotationHistory:
    yesterday_keys: frozenset[str]
    recent_age_by_key: dict[str, int]
    history_days_loaded: int


def payload_track_identities(payload: Any) -> set[str]:
    if not isinstance(payload, dict):
        return set()

    tracks: list[Any] = []
    top_playlist = payload.get("topPlaylist")
    if isinstance(top_playlist, dict):
        top_tracks = top_playlist.get("tracks")
        if isinstance(top_tracks, list):
            tracks.extend(top_tracks)

    sections = payload.get("sections")
    if isinstance(sections, list):
        for section in sections:
            if not isinstance(section, dict):
                continue
            section_tracks = section.get("tracks")
            if isinstance(section_tracks, list):
                tracks.extend(section_tracks)

    identities: set[str] = set()
    for track in tracks:
        if not isinstance(track, dict):
            continue
        title = str(track.get("title") or "").strip()
        artist = str(track.get("artist") or "").strip()
        if not title or not artist:
            continue
        identities.add(f"{normalize_text(title)}::{normalize_text(artist)}")
    return identities


def load_rotation_history(
    home_dir: Path,
    edition_date: date,
    history_days: int = ROTATION_HISTORY_DAYS,
) -> RotationHistory:
    yesterday_keys: set[str] = set()
    recent_age_by_key: dict[str, int] = {}
    history_days_loaded = 0

    for age in range(1, history_days + 1):
        archive_date = edition_date - timedelta(days=age)
        path = home_dir / f"home_recommendations-{archive_date.isoformat()}.json"
        if not path.exists():
            continue
        try:
            payload = load_json(path)
        except (OSError, TypeError, ValueError):
            continue
        if not isinstance(payload, dict):
            continue
        if safe_int(payload.get("schemaVersion")) < SCHEMA_VERSION:
            continue
        if str(payload.get("editionDate") or "").strip() != archive_date.isoformat():
            continue

        history_days_loaded += 1
        identities = payload_track_identities(payload)
        if age == 1:
            yesterday_keys.update(identities)
            continue
        for identity in identities:
            previous_age = recent_age_by_key.get(identity)
            if previous_age is None or age < previous_age:
                recent_age_by_key[identity] = age

    for identity in yesterday_keys:
        recent_age_by_key.pop(identity, None)

    return RotationHistory(
        yesterday_keys=frozenset(yesterday_keys),
        recent_age_by_key=recent_age_by_key,
        history_days_loaded=history_days_loaded,
    )

SOURCE_SECTION_DEFINITIONS = [
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

STYLE_SECTION_DEFINITIONS = [
    (
        "style-pop",
        {"zh-Hans": "流行", "zh-Hant": "流行", "en-US": "Pop"},
        "Pop signals from Last.fm, Audius and Deezer",
        ("lastfm_tag:pop", "audius_genre:Pop", "deezer_chart:132"),
    ),
    (
        "style-rock",
        {"zh-Hans": "摇滚", "zh-Hant": "搖滾", "en-US": "Rock"},
        "Rock signals from Last.fm, Audius and Deezer",
        ("lastfm_tag:rock", "audius_genre:Rock", "deezer_chart:152"),
    ),
    (
        "style-electronic",
        {"zh-Hans": "电子", "zh-Hant": "電子", "en-US": "Electronic"},
        "Electronic signals from Last.fm, Audius and Deezer",
        ("lastfm_tag:electronic", "audius_genre:Electronic", "deezer_chart:113"),
    ),
    (
        "style-indie",
        {"zh-Hans": "独立", "zh-Hant": "獨立", "en-US": "Indie"},
        "Indie and alternative signals",
        ("lastfm_tag:indie", "audius_genre:Alternative", "deezer_chart:85"),
    ),
    (
        "style-hiphop",
        {"zh-Hans": "嘻哈", "zh-Hant": "嘻哈", "en-US": "Hip-Hop"},
        "Hip-Hop signals from Last.fm, Audius and Deezer",
        ("lastfm_tag:hip-hop", "audius_genre:Hip-Hop/Rap", "deezer_chart:116"),
    ),
    (
        "style-rnb",
        {"zh-Hans": "R&B / 灵魂乐", "zh-Hant": "R&B / 靈魂樂", "en-US": "R&B / Soul"},
        "R&B and soul signals",
        ("lastfm_tag:rnb", "audius_genre:R&B/Soul", "deezer_chart:165"),
    ),
    (
        "style-folk",
        {"zh-Hans": "民谣", "zh-Hant": "民謠", "en-US": "Folk"},
        "Folk signals from Last.fm",
        ("lastfm_tag:folk",),
    ),
    (
        "style-jazz",
        {"zh-Hans": "爵士", "zh-Hant": "爵士", "en-US": "Jazz"},
        "Jazz signals from Last.fm, Audius and Deezer",
        ("lastfm_tag:jazz", "audius_genre:Jazz", "deezer_chart:129"),
    ),
    (
        "style-ambient",
        {"zh-Hans": "氛围", "zh-Hant": "氛圍", "en-US": "Ambient"},
        "Ambient signals from Last.fm and Audius",
        ("lastfm_tag:ambient", "audius_genre:Ambient"),
    ),
]


def main() -> None:
    station = load_json(CONFIG_PATH)
    generated_at = now_utc()
    edition_date = generated_at.astimezone(BEIJING).date()

    HOME_DIR.mkdir(parents=True, exist_ok=True)
    print(f"[home] building Beijing edition {edition_date.isoformat()}")
    rotation_history = load_rotation_history(HOME_DIR, edition_date)
    print(
        "[home] rotation history "
        f"days={rotation_history.history_days_loaded} "
        f"yesterday_tracks={len(rotation_history.yesterday_keys)} "
        f"recent_tracks={len(rotation_history.recent_age_by_key)}"
    )

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

    random.seed(edition_date.isoformat())
    ranked_candidates = ranked_with_shuffle(ranked_candidates)

    enrich_missing_metadata(ranked_candidates[:METADATA_ENRICH_LIMIT])
    ranked_candidates = rank_candidates(ranked_candidates)
    ranked_candidates = ranked_with_shuffle(ranked_candidates)

    # Dynamic artist limit for Top 100
    dynamic_artist_limit_top = min(
        ARTIST_PER_PLAYLIST_MAX,
        max(
            ARTIST_PER_PLAYLIST_MIN,
            len(ranked_candidates) // 20,
            TOP_PLAYLIST_LIMIT // 15
        )
    )
    
    top_candidates = build_diverse_playlist(
        ranked_candidates,
        limit=TOP_PLAYLIST_LIMIT,
        artist_limit=dynamic_artist_limit_top,
        lookahead=TOP_PLAYLIST_LOOKAHEAD,
        min_artist_gap=TOP_PLAYLIST_MIN_ARTIST_GAP,
        rng=random,
        yesterday_track_keys=rotation_history.yesterday_keys,
        recent_age_by_key=rotation_history.recent_age_by_key,
    )
    if len(top_candidates) < TOP_PLAYLIST_LIMIT:
        raise SystemExit(
            "Home build produced "
            f"{len(top_candidates)} Top 100 candidates with dynamic_artist_limit="
            f"{dynamic_artist_limit_top}; need {TOP_PLAYLIST_LIMIT}."
        )

    top_playlist = build_top_playlist(top_candidates, generated_at)
    sections = build_sections(ranked_candidates, rotation_history)
    rotation_snapshot = build_rotation_snapshot(
        top_playlist,
        sections,
        rotation_history,
    )
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
        "rotationSnapshot": rotation_snapshot,
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
    print(
        "[home] rotation result "
        f"previous_day_overlap={rotation_snapshot['previousDayOverlapCount']} "
        f"recent_reuse={rotation_snapshot['recentReuseCount']} "
        f"fallback_reuse={rotation_snapshot['fallbackReuseCount']}"
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


def ranked_with_shuffle(candidates: list[CandidateTrack]) -> list[CandidateTrack]:
    result: list[CandidateTrack] = []
    for start in range(0, len(candidates), 20):
        group = list(candidates[start : start + 20])
        random.shuffle(group)
        result.extend(group)
    return result


def build_diverse_playlist(
    candidates: list[CandidateTrack],
    *,
    limit: int,
    artist_limit: int,
    lookahead: int,
    min_artist_gap: int,
    rng: random.Random | Any = random,
    excluded_track_keys: set[str] | None = None,
    yesterday_track_keys: set[str] | frozenset[str] | None = None,
    recent_age_by_key: dict[str, int] | None = None,
    allow_yesterday_fallback: bool = True,
) -> list[CandidateTrack]:
    unique_candidates: list[tuple[int, CandidateTrack]] = []
    seen_track_keys = set(excluded_track_keys or set())
    for index, candidate in enumerate(candidates):
        track_key = track_identity(candidate)
        if track_key in seen_track_keys:
            continue
        artist_key = normalize_text(candidate.artist)
        if not artist_key:
            continue
        seen_track_keys.add(track_key)
        unique_candidates.append((index, candidate))

    if not unique_candidates:
        return []

    scores = [candidate.score for _, candidate in unique_candidates]
    min_score = min(scores)
    max_score = max(scores)
    score_span = max(max_score - min_score, 0.000001)
    pool_size = len(unique_candidates)

    selected: list[CandidateTrack] = []
    artist_counts: dict[str, int] = {}
    last_artist_position: dict[str, int] = {}
    yesterday_keys = set(yesterday_track_keys or set())
    fresh_remaining = [
        item for item in unique_candidates if track_identity(item[1]) not in yesterday_keys
    ]
    fallback_remaining = [
        item for item in unique_candidates if track_identity(item[1]) in yesterday_keys
    ]

    def select_from(remaining: list[tuple[int, CandidateTrack]]) -> None:
        while len(selected) < limit:
            window: list[tuple[int, CandidateTrack]] = []
            for original_index, candidate in remaining:
                artist_key = normalize_text(candidate.artist)
                
                # Dynamic artist limit based on pool size and selection progress
                dynamic_artist_limit = min(
                    ARTIST_PER_PLAYLIST_MAX if limit == TOP_PLAYLIST_LIMIT else ARTIST_PER_SECTION_MAX,
                    max(
                        ARTIST_PER_PLAYLIST_MIN if limit == TOP_PLAYLIST_LIMIT else ARTIST_PER_SECTION_MIN,
                        pool_size // 20,  # Allow ~5% of pool per artist
                        limit // 15       # Don't exceed 1/15 of total limit
                    )
                )
                
                if artist_counts.get(artist_key, 0) >= dynamic_artist_limit:
                    continue
                window.append((original_index, candidate))
                if len(window) >= lookahead:
                    break
            if not window:
                break

            selected_position = len(selected)
            best_original_index, best_candidate = max(
                window,
                key=lambda item: diverse_candidate_score(
                    item[1],
                    original_index=item[0],
                    selected_position=selected_position,
                    artist_counts=artist_counts,
                    last_artist_position=last_artist_position,
                    pool_size=pool_size,
                    min_score=min_score,
                    score_span=score_span,
                    min_artist_gap=min_artist_gap,
                    recent_age_by_key=recent_age_by_key,
                    rng=rng,
                ),
            )

            selected.append(best_candidate)
            best_artist_key = normalize_text(best_candidate.artist)
            artist_counts[best_artist_key] = artist_counts.get(best_artist_key, 0) + 1
            last_artist_position[best_artist_key] = selected_position
            remaining.remove((best_original_index, best_candidate))

    select_from(fresh_remaining)
    if allow_yesterday_fallback and len(selected) < limit:
        select_from(fallback_remaining)

    return selected


def calculate_trend_score(
    candidate: CandidateTrack,
    rng: random.Random | Any = random,
) -> float:
    """
    Calculate trend boost based on track freshness and random jitter.
    New tracks get a bonus to increase diversity day-to-day.
    """
    trend_boost = 0.0
    
    # Factor 1: Freshness boost for newly added tracks
    if hasattr(candidate, 'created_at') and candidate.created_at:
        try:
            days_since_added = (now_utc() - candidate.created_at).days
            if days_since_added <= 7:
                trend_boost += 0.15  # Significant boost for very new tracks
            elif days_since_added <= 14:
                trend_boost += 0.08  # Moderate boost for recent additions
        except (TypeError, AttributeError):
            pass  # Handle cases where date arithmetic fails
    
    # Factor 2: Random jitter to add daily variation (no history tracking yet)
    jitter = rng.random() * 0.03
    
    return trend_boost + jitter


def diverse_candidate_score(
    candidate: CandidateTrack,
    *,
    original_index: int,
    selected_position: int,
    artist_counts: dict[str, int],
    last_artist_position: dict[str, int],
    pool_size: int,
    min_score: float,
    score_span: float,
    min_artist_gap: int,
    recent_age_by_key: dict[str, int] | None = None,
    rng: random.Random | Any = random,
) -> float:
    artist_key = normalize_text(candidate.artist)
    normalized_score = (candidate.score - min_score) / score_span
    rank_score = 1.0 - (original_index / max(pool_size - 1, 1))
    repeat_count = artist_counts.get(artist_key, 0)
    repeat_penalty = repeat_count * 0.18
    gap_penalty = 0.0
    last_position = last_artist_position.get(artist_key)
    if last_position is not None:
        gap = selected_position - last_position
        if gap < min_artist_gap:
            gap_penalty = ((min_artist_gap - gap) / min_artist_gap) * 0.14
    history_age = (recent_age_by_key or {}).get(track_identity(candidate))
    history_penalty = ROTATION_AGE_PENALTIES.get(history_age, 0.0)
    
    # Trend boost for freshness and daily variation
    trend_boost = calculate_trend_score(candidate, rng=rng)
    
    # Base jitter retained from old code (very small)
    jitter = rng.random() * 0.000001
    
    return (
        normalized_score * 0.75      # Reduced from 0.82 to emphasize trends
        + rank_score * 0.15          # Slightly reduced from 0.18
        - repeat_penalty
        - gap_penalty
        - history_penalty
        - original_index * 0.0000001
        + jitter
        + trend_boost                # NEW: Freshness and randomness boost
    )


def format_beijing_generated_time(generated_at: datetime) -> str:
    beijing_time = generated_at.astimezone(BEIJING).replace(second=0, microsecond=0)
    return f"生成时间：{beijing_time:%Y-%m-%d %H:%M}（北京时间）"


def build_top_playlist(
    candidates: list[CandidateTrack],
    generated_at: datetime,
) -> dict[str, Any]:
    return {
        "id": "daily-top-100",
        "title": {
            "zh-Hans": "今日趋势",
            "zh-Hant": "今日趨勢",
            "en-US": "Today's Trending",
        },
        "subtitle": format_beijing_generated_time(generated_at),
        "tracks": [serialize_candidate(c) for c in candidates[:TOP_PLAYLIST_LIMIT]],
    }


def build_sections(
    ranked_candidates: list[CandidateTrack],
    rotation_history: RotationHistory,
) -> list[dict[str, Any]]:
    sections: list[dict[str, Any]] = []
    
    # NEW: Trending Hot section for breakout independent tracks
    hot_upcoming = [c for c in ranked_candidates[:40] if has_any_source(c, ("audius",))]
    if len(hot_upcoming) >= 4:
        # Use dynamic artist limits
        dynamic_artist_limit = min(
            ARTIST_PER_SECTION_MAX,
            max(
                ARTIST_PER_SECTION_MIN,
                len(ranked_candidates) // 20,
                12 // 15  # ~1/15 of limit
            )
        )
        hot_tracks = build_diverse_playlist(
            hot_upcoming,
            limit=12,
            artist_limit=dynamic_artist_limit,
            lookahead=SECTION_LOOKAHEAD,
            min_artist_gap=SECTION_MIN_ARTIST_GAP,
            rng=random,
            yesterday_track_keys=rotation_history.yesterday_keys,
            recent_age_by_key=rotation_history.recent_age_by_key,
            allow_yesterday_fallback=False,
        )
        if len(hot_tracks) >= 4:
            sections.append(
                {
                    "id": "trending-hot",
                    "title": {"zh-Hans": "趋势飙升", "zh-Hant": "趨勢升飆", "en-US": "Hot Rising"},
                    "subtitle": "Breaking tracks from independent artists",
                    "tracks": [serialize_candidate(c) for c in hot_tracks],
                }
            )
    
    return [
        *sections,
        *build_source_sections(ranked_candidates, rotation_history),
        *build_style_sections(ranked_candidates, rotation_history),
    ]


def build_source_sections(
    ranked_candidates: list[CandidateTrack],
    rotation_history: RotationHistory,
) -> list[dict[str, Any]]:
    sections: list[dict[str, Any]] = []
    
    # Use dynamic artist limits for source sections
    dynamic_artist_limit = min(
        ARTIST_PER_SECTION_MAX,
        max(
            ARTIST_PER_SECTION_MIN,
            len(ranked_candidates) // 20,
            SECTION_TRACK_LIMIT // 15
        )
    )
    
    for section_id, title, subtitle, predicate in SOURCE_SECTION_DEFINITIONS:
        section_candidates = [c for c in ranked_candidates if predicate(c)]
        tracks = build_diverse_playlist(
            section_candidates,
            limit=SECTION_TRACK_LIMIT,
            artist_limit=dynamic_artist_limit,
            lookahead=SECTION_LOOKAHEAD,
            min_artist_gap=SECTION_MIN_ARTIST_GAP,
            rng=random,
            yesterday_track_keys=rotation_history.yesterday_keys,
            recent_age_by_key=rotation_history.recent_age_by_key,
            allow_yesterday_fallback=False,
        )
        if len(tracks) < MIN_SECTION_TRACKS:
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


def build_style_sections(
    ranked_candidates: list[CandidateTrack],
    rotation_history: RotationHistory,
) -> list[dict[str, Any]]:
    sections: list[dict[str, Any]] = []
    used_keys: set[str] = set()
    
    # Dynamic artist limit for style sections
    dynamic_artist_limit_style = min(
        ARTIST_PER_SECTION_MAX,
        max(
            ARTIST_PER_SECTION_MIN,
            len(ranked_candidates) // 20,
            SECTION_TRACK_LIMIT // 15
        )
    )
    
    for section_id, title, subtitle, source_needles in STYLE_SECTION_DEFINITIONS:
        section_candidates = [
            c for c in ranked_candidates if has_any_source(c, source_needles)
        ]
        tracks = build_diverse_playlist(
            section_candidates,
            limit=SECTION_TRACK_LIMIT,
            artist_limit=dynamic_artist_limit_style,
            lookahead=SECTION_LOOKAHEAD,
            min_artist_gap=SECTION_MIN_ARTIST_GAP,
            rng=random,
            excluded_track_keys=used_keys,
            yesterday_track_keys=rotation_history.yesterday_keys,
            recent_age_by_key=rotation_history.recent_age_by_key,
            allow_yesterday_fallback=False,
        )
        if len(tracks) < MIN_SECTION_TRACKS:
            continue
        used_keys.update(track_identity(c) for c in tracks)
        sections.append(
            {
                "id": section_id,
                "title": title,
                "subtitle": subtitle,
                "tracks": [serialize_candidate(c) for c in tracks],
            }
        )
    return sections


def build_rotation_snapshot(
    top_playlist: dict[str, Any],
    sections: list[dict[str, Any]],
    rotation_history: RotationHistory,
) -> dict[str, int]:
    current_keys = payload_track_identities(
        {
            "topPlaylist": top_playlist,
            "sections": sections,
        }
    )
    previous_day_overlap = current_keys & rotation_history.yesterday_keys
    recent_reuse = current_keys & rotation_history.recent_age_by_key.keys()
    return {
        "historyDaysLoaded": rotation_history.history_days_loaded,
        "yesterdayTrackCount": len(rotation_history.yesterday_keys),
        "previousDayOverlapCount": len(previous_day_overlap),
        "recentReuseCount": len(recent_reuse),
        "fallbackReuseCount": len(previous_day_overlap),
    }


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
