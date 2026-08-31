# RigPulse v0.6.4

- Adds a password-free SoloPool share fallback for Canaan Nano miners with frozen TCP counters.
- Displays the matched worker's clearly labeled rolling `Pool 6h` share count on its local miner card.
- Generates live share-stream events from increases in the SoloPool worker count when the local counter is stale.
- Deduplicates nearby local and pool share events to avoid double celebrations.
- Keeps RigPulse read-only and avoids storing miner dashboard passwords.
