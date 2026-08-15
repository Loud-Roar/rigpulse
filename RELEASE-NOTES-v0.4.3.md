# RigPulse v0.4.3

- Corrects **Current Share** so it no longer duplicates lifetime accepted shares.
- Reads the newest retained share difficulty from the official AxeOS scoreboard API.
- Formats current-share difficulty using compact `K`, `M`, `G`, `T`, `P`, and `E` units.
- Shows `--` on firmware that does not expose an individual share difficulty.
