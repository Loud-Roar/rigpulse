# RigPulse v0.6.3

- Fixes stale share reporting on Canaan Nano 3 miners whose TCP cgminer API lags behind the web dashboard.
- Reads Accepted, Rejected, Best Share, and Elapsed from `cglog.cgi` while retaining the existing hardware telemetry adapter.
- Adds cache-busting and no-cache headers to every Nano web-counter request.
- Falls back safely to the TCP cgminer values if the web endpoint is unavailable.
