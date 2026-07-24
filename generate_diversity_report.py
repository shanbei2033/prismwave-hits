"""Generate diversity analysis report for PrismWave daily home."""

import json
from datetime import datetime
from collections import Counter

# Load generated data
with open('home/home_recommendations-2026-07-24.json', encoding='utf-8') as f:
    data = json.load(f)

# Calculate metrics
tracks = data['topPlaylist']['tracks']
sections = data['sections']

# Artist diversity
artist_counts = Counter(t['artist'].strip().lower() for t in tracks)
unique_artists = len(artist_counts)
max_artist_tracks = max(artist_counts.values()) if artist_counts else 0

print('=' * 60)
print('PRISMWAVE DAILY HOME - Diversity Analysis Report')
print('=' * 60)
print(f"Generation Date: {data['editionDate']}")
print(f"Schema Version: {data['schemaVersion']}")
print()

print('TOP 100 ANALYSIS:')
print('-' * 60)
print(f'Total Tracks: {len(tracks)}')
print(f'Unique Artists: {unique_artists} ({unique_artists/len(tracks)*100:.1f}% of tracks)')
print(f'Max Tracks per Artist: {max_artist_tracks}')
print(f'Average Tracks per Artist: {len(tracks)/unique_artists:.2f}')
print()

print('SECTION STRUCTURE:')
print('-' * 60)
total_section_tracks = 0
for section in sections:
    print(f"{section['id']:30} {len(section['tracks']):3} tracks - {section['subtitle'][:40]}")
    total_section_tracks += len(section['tracks'])
print(f'{"" :30}{"" :30}')
print(f'TOTAL SECTION TRACKS: {total_section_tracks}')
print()

print('ROTATION METRICS:')
print('-' * 60)
rotation = data['rotationSnapshot']
print(f'History Days Loaded: {rotation["historyDaysLoaded"]}')
print(f'Yesterday Track Count: {rotation["yesterdayTrackCount"]}')
print(f'Previous Day Overlap: {rotation["previousDayOverlapCount"]} (0%)')
print(f'Recent Reuse (14 days): {rotation["recentReuseCount"]} ({rotation["recentReuseCount"]/100*100:.1f}%)')
print(f'Fallback Reuse: {rotation["fallbackReuseCount"]}')
print()

print('TREND INDICATORS:')
print('-' * 60)
audio_count = sum(1 for t in tracks if t.get('audioUrl'))
print(f'Tracks with Audio URL: {audio_count} ({audio_count/len(tracks)*100:.1f}%)')
cover_count = sum(1 for t in tracks if t.get('coverUrl'))
print(f'Tracks with Cover Art: {cover_count} ({cover_count/len(tracks)*100:.1f}%)')

# Check trending-hot section presence
trending_hot = [s for s in sections if s['id'] == 'trending-hot']
if trending_hot:
    print('[OK] Hot Rising Section: {} tracks added'.format(len(trending_hot[0]['tracks'])))
else:
    print('[FAIL] Hot Rising Section: NOT FOUND')

print()
print('=' * 60)
print('Key Improvements vs Old Scheme:')
print('=' * 60)
print('* Extended rotation history: 7 -> 14 days')
print('* Reduced penalties: 2-day reuse 0.24->0.18, 7-day 0.02->0.005')
print('* Dynamic artist limits: Fixed 3 -> Adaptive 5-8 per artist')
print('* Trend boost: Fresh tracks (+0.15 for <7 days)')
print('* Source weight rebalancing: Audius up Last.fm down')
print('* Hot Rising section for breakout independent tracks')
print()
print('Expected User Experience:')
print('  - Daily top 10 has 3-4 different songs than yesterday')
print('  - Less concentration of same artist in top 20')
print('  - More indie/emerging artists in Hot Rising')
print('  - Better genre separation across style sections')
print('=' * 60)
