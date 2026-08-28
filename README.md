# RigPulse

**Local ASIC Fleet Monitoring**

RigPulse is a local-first dashboard for mixed ASIC mining fleets. It polls miners
directly over the LAN and keeps monitoring data local.

## v0.5.10

This release adds separate ntfy sound channels for Best Share, Block Found, and
warnings, plus corrected miner-card badge positioning.

See `RELEASE-NOTES-v0.5.10.md` for details.

## v0.5.9

This release adds optional ntfy phone notifications for new miner and SoloPool
best shares, including saved settings and a test-notification button.

See `RELEASE-NOTES-v0.5.9.md` for details.

## v0.5.8

This emergency compatibility release rolls back the v0.5.7 miner-card
structure and restores the proven v0.5.6 layout across every theme.

See `RELEASE-NOTES-v0.5.8.md` for details.

## v0.5.7

This corrective release rebuilds local miner cards using the complete approved
RigPulse instrument layout. Live Hashrate and a more pronounced Best Share now
sit together, with dedicated thermal gauges and operating readings.

See `RELEASE-NOTES-v0.5.7.md` for details.

## v0.5.6

This release renames the Nerd Console theme to RigPulse Console and applies the
approved contemporary prototype direction: modern typography, softer dark
green instrumentation, and a redesigned live-hashrate surface on every miner
card. Existing telemetry and low-flicker in-place updates are retained.

See `RELEASE-NOTES-v0.5.6.md` for details.

## v0.5.5

This release rebuilds Nerd Console miner cards as ASIC instrument faces. It adds
custom Rajdhani and Share Tech Mono typography, real telemetry icons, circular
fan and temperature gauges, off-green LCD displays, restrained chassis colors,
and clearer visual hierarchy across miner data.

### Current miner families

- LuxOS / Antminer
- Avalon Nano / CGMiner
- AxeOS / Bitaxe / NerdOCTAXE
- IceRiver AL-series
- Goldshell
- Generic CGMiner / compatible JSON endpoints

### Dashboard features

- Separate SHA-256 and Blake3 fleet totals
- Live share celebrations and configurable emojis
- Best-share tracking and Bitcoin difficulty comparison
- Fleet health and alert thresholds
- Historical hashrate / temperature / power data
- Pool telemetry when firmware exposes it
- Public SoloPool account and per-worker monitoring
- Bitcoin block and mempool status
- Dashboard themes and transparency controls
- SQLite persistence

## Run locally on Windows

```powershell
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
.\run-local.ps1
```

Open `http://localhost:8080`.

## Import a v0.2.x database

If you want to migrate an older development database:

```powershell
.\migrate-v0.2-data.ps1 -OldDataDir "C:\path\to\old\hashwatcher-local\data"
```

RigPulse itself also automatically copies `hashwatcher.db` to `rigpulse.db` when
both files are in the same data directory and a RigPulse database does not exist.

## Run with Docker

```bash
docker compose up -d --build
```

Open `http://localhost:8080`.

Persistent data lives in `./data`.

## Umbrel

The Umbrel Community App Store is published separately from this source repository.

The production Umbrel package expects a public GHCR image:

```text
ghcr.io/loud-roar/rigpulse:0.5.10
```

Source repository: `https://github.com/Loud-Roar/rigpulse`

## Security

RigPulse is read-only by default. It does not store miner passwords for the
currently supported monitoring paths. Diagnostic output sanitizes known secret
fields. Miner control actions are intentionally not part of this release.
