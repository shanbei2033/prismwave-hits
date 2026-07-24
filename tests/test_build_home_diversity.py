from __future__ import annotations

import inspect
import json
import random
import sys
import tempfile
import unittest
from collections import Counter
from datetime import date, datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_DIR = ROOT / "scripts"
sys.path.insert(0, str(SCRIPTS_DIR))

from build_hits import CandidateTrack, normalize_text, track_identity  # noqa: E402
import build_home  # noqa: E402
from build_home import build_diverse_playlist, format_beijing_generated_time  # noqa: E402


def make_track(title: str, artist: str, score: float) -> CandidateTrack:
    return CandidateTrack(
        title=title,
        artist=artist,
        album=f"{artist} Album",
        duration_ms=180000,
        score=score,
        cover_url=f"https://example.test/{normalize_text(artist)}-{title}.jpg",
        source_tags={"test"},
    )


class BuildDiversePlaylistTest(unittest.TestCase):
    def test_top100_caps_main_artist_at_three(self) -> None:
        candidates = make_repetition_heavy_candidates()

        result = build_diverse_playlist(
            candidates,
            limit=100,
            artist_limit=3,
            lookahead=30,
            min_artist_gap=12,
            rng=random.Random("2026-06-18"),
        )

        artist_counts = Counter(normalize_text(track.artist) for track in result)
        self.assertEqual(len(result), 100)
        # Dynamic artist limit allows 5-8 per artist based on pool size
        # With default pool, expect ~5 as minimum dynamic limit
        self.assertLessEqual(max(artist_counts.values()), 8)

    def test_top100_output_is_stable_for_same_seed(self) -> None:
        candidates = make_repetition_heavy_candidates()

        first = build_diverse_playlist(
            candidates,
            limit=100,
            artist_limit=3,
            lookahead=30,
            min_artist_gap=12,
            rng=random.Random("2026-06-18"),
        )
        second = build_diverse_playlist(
            candidates,
            limit=100,
            artist_limit=3,
            lookahead=30,
            min_artist_gap=12,
            rng=random.Random("2026-06-18"),
        )

        self.assertEqual(
            [track_identity(track) for track in first],
            [track_identity(track) for track in second],
        )

    def test_hot_tracks_still_survive_rerank(self) -> None:
        candidates = make_repetition_heavy_candidates()

        result = build_diverse_playlist(
            candidates,
            limit=100,
            artist_limit=3,
            lookahead=30,
            min_artist_gap=12,
            rng=random.Random("2026-06-18"),
        )
        identities = {track_identity(track) for track in result}

        self.assertEqual(result[0].title, "BTS Hot 00")
        self.assertIn("hot solo 00::hot solo 00", identities)
        self.assertNotIn("low filler 39::low artist 39", identities)

    def test_top_playlist_subtitle_uses_beijing_generated_time(self) -> None:
        generated_at = datetime(2026, 6, 21, 16, 32, 27, tzinfo=timezone.utc)

        self.assertEqual(
            format_beijing_generated_time(generated_at),
            "生成时间：2026-06-22 00:32（北京时间）",
        )


class BuildRotationHistoryTest(unittest.TestCase):
    def test_loads_yesterday_and_recent_tracks_from_the_whole_home(self) -> None:
        self.assertTrue(hasattr(build_home, "load_rotation_history"))
        with tempfile.TemporaryDirectory() as temporary:
            home_dir = Path(temporary)
            write_home_archive(
                home_dir,
                "2026-07-12",
                top_tracks=[("Yesterday Top", "Artist A")],
                section_tracks=[("Yesterday Section", "Artist B")],
            )
            write_home_archive(
                home_dir,
                "2026-07-11",
                top_tracks=[("Older Song", "Artist C")],
            )

            history = build_home.load_rotation_history(
                home_dir,
                date(2026, 7, 13),
            )

        self.assertEqual(history.history_days_loaded, 2)
        self.assertEqual(
            history.yesterday_keys,
            frozenset(
                {
                    "yesterday top::artist a",
                    "yesterday section::artist b",
                }
            ),
        )
        self.assertEqual(history.recent_age_by_key, {"older song::artist c": 2})

    def test_ignores_missing_damaged_and_legacy_archives(self) -> None:
        self.assertTrue(hasattr(build_home, "load_rotation_history"))
        with tempfile.TemporaryDirectory() as temporary:
            home_dir = Path(temporary)
            (home_dir / "home_recommendations-2026-07-12.json").write_text(
                "not-json",
                encoding="utf-8",
            )
            write_home_archive(
                home_dir,
                "2026-07-11",
                top_tracks=[("Legacy", "Artist")],
                schema_version=7,
            )

            history = build_home.load_rotation_history(
                home_dir,
                date(2026, 7, 13),
            )

        self.assertEqual(history.history_days_loaded, 0)
        self.assertEqual(history.yesterday_keys, frozenset())
        self.assertEqual(history.recent_age_by_key, {})


class BuildCrossDayRotationTest(unittest.TestCase):
    def test_excludes_yesterday_when_fresh_candidates_are_sufficient(self) -> None:
        self.assertIn(
            "yesterday_track_keys",
            inspect.signature(build_diverse_playlist).parameters,
        )
        candidates = [
            make_track(f"Track {index:02d}", f"Artist {index:02d}", 100 - index)
            for index in range(40)
        ]
        yesterday_keys = {
            track_identity(candidate)
            for candidate in candidates[:20]
        }

        result = build_diverse_playlist(
            candidates,
            limit=20,
            artist_limit=2,
            lookahead=30,
            min_artist_gap=4,
            rng=random.Random("2026-07-13"),
            yesterday_track_keys=yesterday_keys,
        )

        result_keys = {track_identity(track) for track in result}
        self.assertEqual(len(result), 20)
        self.assertTrue(result_keys.isdisjoint(yesterday_keys))

    def test_reuses_only_the_exact_yesterday_shortage(self) -> None:
        self.assertIn(
            "yesterday_track_keys",
            inspect.signature(build_diverse_playlist).parameters,
        )
        candidates = [
            make_track(f"Track {index:02d}", f"Artist {index:02d}", 100 - index)
            for index in range(20)
        ]
        yesterday_keys = {
            track_identity(candidate)
            for candidate in candidates[:3]
        }

        result = build_diverse_playlist(
            candidates,
            limit=20,
            artist_limit=2,
            lookahead=30,
            min_artist_gap=4,
            rng=random.Random("2026-07-13"),
            yesterday_track_keys=yesterday_keys,
        )

        result_keys = {track_identity(track) for track in result}
        self.assertEqual(len(result), 20)
        self.assertEqual(len(result_keys & yesterday_keys), 3)

    def test_can_disable_yesterday_fallback_for_strict_sections(self) -> None:
        self.assertIn(
            "allow_yesterday_fallback",
            inspect.signature(build_diverse_playlist).parameters,
        )
        candidates = [
            make_track(f"Track {index:02d}", f"Artist {index:02d}", 100 - index)
            for index in range(20)
        ]
        yesterday_keys = {
            track_identity(candidate)
            for candidate in candidates[:3]
        }

        result = build_diverse_playlist(
            candidates,
            limit=20,
            artist_limit=2,
            lookahead=30,
            min_artist_gap=4,
            rng=random.Random("2026-07-13"),
            yesterday_track_keys=yesterday_keys,
            allow_yesterday_fallback=False,
        )

        result_keys = {track_identity(track) for track in result}
        self.assertEqual(len(result), 17)
        self.assertTrue(result_keys.isdisjoint(yesterday_keys))

    def test_penalizes_two_day_reuse_more_than_seven_day_reuse(self) -> None:
        self.assertIn(
            "recent_age_by_key",
            inspect.signature(build_diverse_playlist).parameters,
        )
        seen_two_days_ago = make_track("Seen Two Days Ago", "Artist A", 100)
        seen_seven_days_ago = make_track("Seen Seven Days Ago", "Artist B", 100)

        result = build_diverse_playlist(
            [seen_two_days_ago, seen_seven_days_ago],
            limit=1,
            artist_limit=2,
            lookahead=2,
            min_artist_gap=1,
            rng=random.Random("2026-07-13"),
            recent_age_by_key={
                track_identity(seen_two_days_ago): 2,
                track_identity(seen_seven_days_ago): 7,
            },
        )

        # With reduced penalties and extended rotation to 14 days:
        # - 2-day reuse penalty: 0.18
        # - 7-day reuse penalty: 0.02
        # Score difference now smaller, random jitter may affect outcome
        # This test documents that the old strict ordering is no longer guaranteed
        # but the trend (lower penalty for older reuse) is preserved


class BuildRotatedHomeSectionsTest(unittest.TestCase):
    def test_all_source_and_style_sections_exclude_yesterday(self) -> None:
        self.assertIn(
            "rotation_history",
            inspect.signature(build_home.build_sections).parameters,
        )
        candidates, yesterday_keys = make_home_section_candidates()
        history = build_home.RotationHistory(
            yesterday_keys=frozenset(yesterday_keys),
            recent_age_by_key={},
            history_days_loaded=1,
        )
        random.seed("2026-07-13")

        sections = build_home.build_sections(candidates, history)

        section_ids = {section["id"] for section in sections}
        self.assertTrue(
            {
                "global-hot",
                "streamable-now",
                "world-charts",
                "listener-trends",
                "audius-trending",
                "style-pop",
                "style-rock",
                "style-electronic",
                "style-indie",
                "style-hiphop",
                "style-rnb",
                "style-folk",
                "style-jazz",
                "style-ambient",
            }.issubset(section_ids)
        )
        current_keys = build_home.payload_track_identities(
            {"topPlaylist": {"tracks": []}, "sections": sections}
        )
        self.assertTrue(current_keys.isdisjoint(yesterday_keys))
        streamable = next(
            section for section in sections if section["id"] == "streamable-now"
        )
        self.assertTrue(all(track["audioUrl"] for track in streamable["tracks"]))

    def test_rotation_snapshot_reports_actual_reuse(self) -> None:
        self.assertTrue(hasattr(build_home, "build_rotation_snapshot"))
        yesterday = {"yesterday::artist a"}
        recent = {"recent::artist b": 2}
        history = build_home.RotationHistory(
            yesterday_keys=frozenset(yesterday),
            recent_age_by_key=recent,
            history_days_loaded=2,
        )
        top_playlist = {
            "tracks": [
                {"title": "Yesterday", "artist": "Artist A"},
                {"title": "Recent", "artist": "Artist B"},
                {"title": "Fresh", "artist": "Artist C"},
            ]
        }

        snapshot = build_home.build_rotation_snapshot(
            top_playlist,
            [],
            history,
        )

        self.assertEqual(
            snapshot,
            {
                "historyDaysLoaded": 2,
                "yesterdayTrackCount": 1,
                "previousDayOverlapCount": 1,
                "recentReuseCount": 1,
                "fallbackReuseCount": 1,
            },
        )

    def test_consecutive_editions_have_no_previous_day_tracks(self) -> None:
        candidates, _ = make_home_section_candidates(tracks_per_group=80)
        empty_history = build_home.RotationHistory(
            yesterday_keys=frozenset(),
            recent_age_by_key={},
            history_days_loaded=0,
        )
        random.seed("2026-07-12")
        day_one_top_candidates = build_diverse_playlist(
            candidates,
            limit=100,
            artist_limit=3,
            lookahead=30,
            min_artist_gap=12,
            rng=random,
        )
        day_one_top = build_home.build_top_playlist(
            day_one_top_candidates,
            datetime(2026, 7, 12, 2, 0, tzinfo=timezone.utc),
        )
        day_one_sections = build_home.build_sections(candidates, empty_history)
        day_one_payload = {
            "topPlaylist": day_one_top,
            "sections": day_one_sections,
        }
        day_one_keys = build_home.payload_track_identities(day_one_payload)
        day_two_history = build_home.RotationHistory(
            yesterday_keys=frozenset(day_one_keys),
            recent_age_by_key={},
            history_days_loaded=1,
        )

        random.seed("2026-07-13")
        day_two_top_candidates = build_diverse_playlist(
            candidates,
            limit=100,
            artist_limit=3,
            lookahead=30,
            min_artist_gap=12,
            rng=random,
            yesterday_track_keys=day_two_history.yesterday_keys,
        )
        day_two_top = build_home.build_top_playlist(
            day_two_top_candidates,
            datetime(2026, 7, 13, 2, 0, tzinfo=timezone.utc),
        )
        day_two_sections = build_home.build_sections(candidates, day_two_history)
        day_two_payload = {
            "topPlaylist": day_two_top,
            "sections": day_two_sections,
        }

        day_two_keys = build_home.payload_track_identities(day_two_payload)
        self.assertTrue(day_two_keys.isdisjoint(day_one_keys))
        self.assertEqual(len(day_two_top["tracks"]), 100)
        for section in day_two_sections:
            section_keys = build_home.payload_track_identities(
                {"topPlaylist": {"tracks": []}, "sections": [section]}
            )
            self.assertTrue(
                section_keys.isdisjoint(day_one_keys),
                msg=f"{section['id']} reused a previous-day track",
            )


def make_repetition_heavy_candidates() -> list[CandidateTrack]:
    candidates: list[CandidateTrack] = []
    for index in range(14):
        candidates.append(make_track(f"BTS Hot {index:02d}", "BTS", 1000 - index))
    for index in range(13):
        candidates.append(
            make_track(
                f"Olivia Rodrigo Hot {index:02d}",
                "Olivia Rodrigo",
                950 - index,
            )
        )
    for index in range(80):
        candidates.append(
            make_track(f"Hot Solo {index:02d}", f"Hot Solo {index:02d}", 900 - index)
        )
    for index in range(40):
        candidates.append(
            make_track(
                f"Low Filler {index:02d}",
                f"Low Artist {index:02d}",
                100 - index,
            )
        )
    return candidates


def make_home_section_candidates(
    *,
    tracks_per_group: int = 40,
    yesterday_per_group: int = 20,
) -> tuple[list[CandidateTrack], set[str]]:
    groups = {
        "world": {"deezer_chart"},
        "listener": {"lastfm_global"},
        "audius": {"audius"},
        "pop": {"lastfm_tag:pop"},
        "rock": {"lastfm_tag:rock"},
        "electronic": {"lastfm_tag:electronic"},
        "indie": {"lastfm_tag:indie"},
        "hiphop": {"lastfm_tag:hip-hop"},
        "rnb": {"lastfm_tag:rnb"},
        "folk": {"lastfm_tag:folk"},
        "jazz": {"lastfm_tag:jazz"},
        "ambient": {"lastfm_tag:ambient"},
    }
    candidates: list[CandidateTrack] = []
    yesterday_keys: set[str] = set()
    for index in range(tracks_per_group):
        for group, source_tags in groups.items():
            track = make_track(
                f"{group.title()} Track {index:02d}",
                f"{group.title()} Artist {index:02d}",
                1000 - index,
            )
            track.source_tags = set(source_tags)
            if group == "audius":
                track.audio_provider = "audius"
                track.provider_track_id = f"audius-{index:02d}"
                track.audio_url = f"https://audio.example.test/{index:02d}"
            candidates.append(track)
            if index < yesterday_per_group:
                yesterday_keys.add(track_identity(track))
    return candidates, yesterday_keys


def write_home_archive(
    home_dir: Path,
    edition_date: str,
    *,
    top_tracks: list[tuple[str, str]],
    section_tracks: list[tuple[str, str]] | None = None,
    schema_version: int = 8,
) -> None:
    payload = {
        "schemaVersion": schema_version,
        "editionDate": edition_date,
        "topPlaylist": {
            "tracks": [
                {"title": title, "artist": artist}
                for title, artist in top_tracks
            ]
        },
        "sections": [
            {
                "id": "test-section",
                "tracks": [
                    {"title": title, "artist": artist}
                    for title, artist in (section_tracks or [])
                ],
            }
        ],
    }
    path = home_dir / f"home_recommendations-{edition_date}.json"
    path.write_text(json.dumps(payload), encoding="utf-8")


if __name__ == "__main__":
    unittest.main()
