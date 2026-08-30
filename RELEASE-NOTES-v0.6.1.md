# RigPulse v0.6.1

- Refreshes BTC, BCH, and ALPH wallet balances every six hours instead of every minute.
- Saves the last successful wallet balances in the RigPulse database so they survive restarts and updates.
- Preserves cached balances when a public wallet API times out or is temporarily unavailable.
- Shows when balances were last updated and clearly marks a cached result after a failed refresh.
- Still refreshes immediately at startup and whenever wallet settings are saved.
