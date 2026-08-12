# RigPulse v0.3.5

- Moves Probe, Diagnose, and IceRiver Raw controls from dashboard cards into
  the miner detail view.
- Adds reported fan speed to main dashboard miner cards.
- Shows readable server errors when diagnostic endpoints do not return JSON.
- Stops telemetry WebSocket messages from rebuilding the entire dashboard,
  eliminating sparkline flashes during share celebrations.
