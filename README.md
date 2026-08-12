# RigPulse

**Local ASIC Fleet Monitoring**

RigPulse is a local-first dashboard for mixed ASIC mining fleets. It polls miners
directly over the LAN and keeps monitoring data local.

## v0.3.0

This is the first RigPulse-branded release.

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
ghcr.io/loud-roar/rigpulse:0.3.0
```

Source repository: `https://github.com/Loud-Roar/rigpulse`

## Security

RigPulse is read-only by default. It does not store miner passwords for the
currently supported monitoring paths. Diagnostic output sanitizes known secret
fields. Miner control actions are intentionally not part of this release.
