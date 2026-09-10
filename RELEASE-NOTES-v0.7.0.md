# RigPulse v0.7.0

This release adds a read-only Home Assistant publisher.

- Configure a Home Assistant URL and long-lived access token in Customization.
- Test the connection directly from RigPulse.
- Publish one online entity and one telemetry entity per miner every minute.
- Publish fleet, configured wallet, and configured BTC/BCH SoloPool entities.
- Fire `rigpulse_best_share`, `rigpulse_block_found`, and `rigpulse_warning`
  events for Home Assistant automations.
- Keep the Home Assistant token in RigPulse's local settings database and use it
  only for authenticated calls to the configured Home Assistant URL.
