# RigPulse v0.5.9

Optional ntfy phone notifications for best-share milestones.

## Changes

- Adds saved ntfy server, topic, and optional access-token settings.
- Adds a Save & Send Test Notification button in Customization.
- Sends phone notifications when an individual miner reports a higher best share.
- Sends phone notifications when BTC or BCH SoloPool reports a higher account best share.
- Stores the initial SoloPool reading as a baseline to prevent an old best share from producing a false alert after installation.
- Adds SoloPool best-share events to the live dashboard and Alerts history.
- Keeps notification monitoring active while the RigPulse dashboard is closed.
- Does not change dashboard themes or miner-card layouts.
