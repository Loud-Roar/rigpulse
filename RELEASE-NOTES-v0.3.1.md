# RigPulse v0.3.1

- Fixes `sqlite3.OperationalError: unable to open database file` on fresh
  Umbrel installations.
- Prepares the persistent data directory at container startup, then drops
  privileges before starting RigPulse.
- Retains the six-card single-row desktop dashboard layout fix.
