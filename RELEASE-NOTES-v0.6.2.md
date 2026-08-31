# RigPulse v0.6.2

- Fixes Today and Session share totals becoming stuck at zero after a miner counter reset.
- Counts only positive accepted-share changes, safely continuing across counter rollbacks and miner restarts.
- Initializes a fresh session baseline whenever RigPulse starts.
- Restores live share detection immediately after a RigPulse restart by comparing against the last stored sample.
- Records accepted-share events in the RigPulse event log for easier troubleshooting.
