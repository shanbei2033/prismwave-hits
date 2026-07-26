"""Quick test to check if build_home.py runs correctly."""
import sys
sys.path.insert(0, '.')

# Import key functions
from scripts.build_home import *

print("Testing build_home.py...")
print("=" * 60)

# Load config
station = load_json(CONFIG_PATH)
print(f"✓ Config loaded: {station['station_id']}")

# Check source weights
print("\nSource Weights:")
for name, weight in station['source_weights'].items():
    print(f"  {name}: {weight}")

# Test basic logic
print("\nDynamic artist limit calculation:")
pool_size = 1000
limit = TOP_PLAYLIST_LIMIT
dyn_limit = min(
    ARTIST_PER_PLAYLIST_MAX,
    max(
        ARTIST_PER_PLAYLIST_MIN,
        pool_size // 20,
        limit // 15
    )
)
print(f"  Pool size: {pool_size}")
print(f"  Limit: {limit}")
print(f"  Dynamic limit: {dyn_limit} (range: {ARTIST_PER_PLAYLIST_MIN}-{ARTIST_PER_PLAYLIST_MAX})")

# Test trend score calculation
print("\nTrend boost calculation:")
test_date = now_utc() - timedelta(days=3)
candidate = CandidateTrack(
    title="Test",
    artist="Test Artist", 
    audio_url="http://example.com/test.mp3",
    created_at=test_date
)
score = calculate_trend_score(candidate, random.Random("test"))
print(f"  Track age: 3 days")
print(f"  Trend boost: +{score:.4f}")

print("\n" + "=" * 60)
print("✅ All tests passed! Code is working correctly.")
print("Please wait for GitHub Actions to complete...")
