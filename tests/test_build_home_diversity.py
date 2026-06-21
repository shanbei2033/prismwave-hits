from __future__ import annotations

import random
import sys
import unittest
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_DIR = ROOT / "scripts"
sys.path.insert(0, str(SCRIPTS_DIR))

from build_hits import CandidateTrack, normalize_text, track_identity  # noqa: E402
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
        self.assertLessEqual(max(artist_counts.values()), 3)

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


if __name__ == "__main__":
    unittest.main()
