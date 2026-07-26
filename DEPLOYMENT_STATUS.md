# PrismWave Daily Home Deployment Status

## Current Status: ✅ DEPLOYED TO GITHUB

### Commit Information
- **Latest SHA**: `bb790a1`
- **Author**: github-actions[bot]
- **Date**: Fri Jul 24 2026
- **Message**: "chore: build home recommendations"

### Deployed Content
The commit includes:
1. ✅ **Code with diversity improvements** (generator version 0.5.0)
   - Extended rotation history: 7 → 14 days
   - Dynamic artist limits (5-8 per artist)
   - Trend boost mechanism
   - Source weight rebalancing (Audius ↑ Last.fm ↓)
   - Hot Rising section code
   
2. ⚠️ **Generated data**: Old format (15 sections, no Hot Rising)
   - Schema Version: 8
   - Generator Version: prismwave-home/0.4.1 (incorrect!)
   - Edition Date: 2026-07-24

### Known Issue

**Problem**: Generated data doesn't include the "Hot Rising" section even though the code has it.

**Reason**: Python script timeout due to network requests to music APIs (Last.fm, Deezer, Audius).

**Current Data Analysis**:
- Total Sections: 15 (should be 16)
- Unique Artists: 77 (77% vs expected 98%)
- Max Tracks per Artist: 4 (should be max 2)
- Hot Rising Section: ❌ NOT FOUND

### Next Steps

#### Option 1: Manual Local Generation (Recommended)
```powershell
cd e:\Project\prismwave-hits
python scripts\build_home.py --timeout 600
git add home/
git commit -m "feat: rebuild with diversity improvements (manual)"
git push origin main
```

#### Option 2: Wait for GitHub Actions Retry
GitHub Actions will retry automatically on next push. You can:
1. Manually trigger a workflow run from GitHub UI
2. Or push a small change to force re-run

#### Option 3: Use Environment Variable (Skip Audio Resolution)
For faster generation without resolving audio URLs:
```powershell
$env:PRISMWAVE_HOME_RESOLVE_AUDIO=""
python scripts\build_home.py
```

### Verification Checklist

After successful deployment, verify:
- [ ] Top 100 tracks generated successfully
- [ ] **Hot Rising section present** (12 tracks)
- [ ] **16 total sections** (instead of 15)
- [ ] **Generator Version: prismwave-home/0.5.0**
- [ ] **Schema Version: 8**
- [ ] Max 2 tracks per artist in Top 100
- [ ] Better artist diversity (~98%)
- [ ] Lower recent reuse rate (<50%)

### Files Changed
- `scripts/build_home.py` (+376 lines, -20 deletions)
- `config/station.json` (weight adjustments)
- `tests/test_build_home_diversity.py` (test updates)
- `home/latest_home.json` (generated)
- `home/home_recommendations-YYYY-MM-DD.json` (generated)

### Timeline
- ✅ **16:03**: Code pushed to main (shanbei2033 author)
- ✅ **16:12**: GitHub Actions completed build
- ⚠️ **Issue**: Old data format in generated JSON

---

**Status**: Waiting for manual regeneration or GitHub Actions retry  
**Expected Outcome**: New data with Hot Rising section and improved diversity metrics
