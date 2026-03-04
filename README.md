
# ETF iNAV–NAV + LTP Premium Dashboard (India)

**Live site (after you enable GitHub Pages):**
```
https://Shamparoy21.github.io/etf-dashboard/
```

## What this shows
- iNAV and Previous Day NAV published by AMCs (Mirae Asset, Nippon India, Zerodha Fund House)
- **True trading premium/discount** = (LTP − iNAV) / iNAV from **NSE ETF Market Watch**
- Volumes (NSE), category filters, search, sortable columns, CSV export
- Mini iNAV sparklines (last ~10 snapshots)

## Sources
- NSE ETF Market Watch (LTP, Volume): see NSE’s ETF Market Watch page.  
- Mirae Asset iNAV page (iNAV + previous-day NAV).  
- Nippon India ETF real-time iNAV page.  
- Zerodha Fund House iNAV summary.

> iNAV is indicative and compared with the prior trading day’s official NAV.

## How it auto-updates
- GitHub Actions runs **hourly** (and on-demand) to fetch NSE + AMC pages and rebuild `data/data.json`.
- `index.html` fetches `data/data.json` at load; if missing, it uses a built-in fallback.

## First-time setup
1. Put this repo under your GitHub account (`etf-dashboard`).
2. Ensure these files exist:
   - `index.html`
   - `scripts/fetch_and_build.py`
   - `.github/workflows/build.yml`
3. Enable **Pages**: Settings ▸ Pages ▸ Source = *Deploy from a branch* ▸ Branch = `main` ▸ Folder = `/ (root)` ▸ **Save**.

## Manual refresh
If you need an immediate refresh, run the workflow manually (Actions ▸ *Build ETF Dashboard* ▸ **Run workflow**).
