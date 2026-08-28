# RigPulse v0.5.10

Custom ntfy sound channels and badge cleanup.

## Changes

- Keeps the existing ntfy topic for miner and SoloPool Best Share alerts.
- Adds a `-block` topic for high-priority Block Found notifications.
- Adds a `-warning` topic for miner-offline, pool-disconnected, and temperature warnings.
- Adds individual test buttons for all three notification channels.
- Adds separate enable switches for Best Share, Block Found, and warning alerts.
- Moves Block Found and Fleet Best badges inside miner cards so they are no longer clipped by card boundaries.
- Preserves the current dashboard themes and miner-card layout.
