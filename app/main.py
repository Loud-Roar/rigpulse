from __future__ import annotations

import asyncio
import json
import math
import os
import re
import shutil
import socket
import sqlite3
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any
from urllib.parse import quote

import httpx
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, HTMLResponse, Response
from pydantic import BaseModel, Field


DATA_DIR = Path(
    os.getenv("RIGPULSE_DATA_DIR")
    or os.getenv("HASHWATCHER_DATA_DIR")  # v0.2.x compatibility
    or "./data"
)
DATA_DIR.mkdir(parents=True, exist_ok=True)

DB = DATA_DIR / "rigpulse.db"
LEGACY_DB = DATA_DIR / "hashwatcher.db"
if not DB.exists() and LEGACY_DB.exists():
    shutil.copy2(LEGACY_DB, DB)

POLL_SECONDS = int(
    os.getenv("RIGPULSE_POLL_SECONDS")
    or os.getenv("HASHWATCHER_POLL_SECONDS")  # v0.2.x compatibility
    or "10"
)


@dataclass
class Telemetry:
    # "online" now means usable telemetry was obtained, not merely that port 80 answered.
    online: bool = False
    reachable: bool | None = None
    authenticated: bool | None = None
    telemetry_available: bool = False
    hashrate: float | None = None
    hashrate_unit: str = ""
    avg_hashrate: float | None = None
    avg_hashrate_unit: str = ""
    temp_c: float | None = None
    exhaust_temp_f: float | None = None
    power_w: float | None = None
    efficiency: float | None = None
    efficiency_unit: str = ""
    accepted: int | None = None
    rejected: int | None = None
    best_share: str | None = None
    current_share: str | None = None
    found_blocks: int | None = None
    pool_key: str | None = None
    pool_alive: bool | None = None
    uptime_s: int | None = None
    fan_rpm: int | None = None
    work_mode: str | None = None
    raw: dict[str, Any] | None = None
    error: str | None = None


class MinerIn(BaseModel):
    name: str = Field(min_length=1, max_length=64)
    ip: str = Field(min_length=1, max_length=128)
    model: str = ""
    family: str = "auto"
    algorithm: str = "SHA-256"


class SettingsIn(BaseModel):
    share_emoji: str = "🎉"
    share_emoji_sha256: str = ""
    share_emoji_blake3: str = ""
    share_emoji_default: str = ""
    animation_density: int = Field(default=7, ge=1, le=30)
    celebrate_rejected: bool = False


class AlertSettingsIn(BaseModel):
    enabled: bool = True
    offline_seconds: int = Field(default=60, ge=10, le=3600)
    hashrate_drop_pct: float = Field(default=30, ge=1, le=99)
    temperature_c: float = Field(default=75, ge=1, le=150)
    reject_pct: float = Field(default=2.0, ge=0, le=100)
    pool_disconnect: bool = True


class CustomizationIn(BaseModel):
    theme: str = "midnight"
    card_opacity: float = Field(default=0.82, ge=0.30, le=1.0)
    blur_px: int = Field(default=14, ge=0, le=30)
    background_intensity: float = Field(default=1.0, ge=0.2, le=2.0)
    compact_cards: bool = False
    block_api_base: str = "https://mempool.space/api"
    btc_wallet_address: str = ""
    bch_wallet_address: str = ""
    btc_solopool_address: str = ""
    bch_solopool_address: str = ""
    btc_solo_hashrate: float = Field(default=0, ge=0)
    btc_solo_hashrate_unit: str = "TH"
    bch_solo_hashrate: float = Field(default=0, ge=0)
    bch_solo_hashrate_unit: str = "TH"


class WSManager:
    def __init__(self):
        self.clients: set[WebSocket] = set()

    async def connect(self, ws: WebSocket):
        await ws.accept()
        self.clients.add(ws)

    def disconnect(self, ws: WebSocket):
        self.clients.discard(ws)

    async def broadcast(self, msg: dict[str, Any]):
        dead = []
        for ws in self.clients:
            try:
                await ws.send_json(msg)
            except Exception:
                dead.append(ws)
        for ws in dead:
            self.clients.discard(ws)


manager = WSManager()
latest_telemetry: dict[int, Telemetry] = {}
latest_block: dict[str, Any] = {"available": False, "source": "mempool.space"}
latest_network: dict[str, Any] = {"available": False, "source": "mempool.space"}
latest_bch: dict[str, Any] = {"available": False, "source": "blockchair.com"}
latest_wallets: dict[str, Any] = {"btc": None, "bch": None, "updated_at": None}
latest_prices: dict[str, Any] = {"btc": None, "bch": None, "alph": None, "updated_at": None, "source": "CoinGecko"}
latest_solopool: dict[str, Any] = {"btc": None, "bch": None, "updated_at": None, "errors": {}}
app = FastAPI(title="RigPulse", version="0.5.4")


def db():
    conn = sqlite3.connect(DB)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    with db() as c:
        c.executescript("""
        CREATE TABLE IF NOT EXISTS miners (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            ip TEXT NOT NULL UNIQUE,
            model TEXT NOT NULL DEFAULT '',
            family TEXT NOT NULL DEFAULT 'auto',
            algorithm TEXT NOT NULL DEFAULT 'SHA-256',
            created_at INTEGER NOT NULL
        );
        CREATE TABLE IF NOT EXISTS samples (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            miner_id INTEGER NOT NULL,
            ts INTEGER NOT NULL,
            hashrate REAL,
            hashrate_unit TEXT,
            temp_c REAL,
            power_w REAL,
            accepted INTEGER,
            rejected INTEGER,
            best_share TEXT,
            online INTEGER,
            FOREIGN KEY(miner_id) REFERENCES miners(id)
        );
        CREATE INDEX IF NOT EXISTS idx_samples_miner_ts ON samples(miner_id, ts);
        CREATE TABLE IF NOT EXISTS settings (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS events (
            id INTEGER PRIMARY KEY AUTOINCREMENT, miner_id INTEGER, ts INTEGER NOT NULL,
            event_type TEXT NOT NULL, value_text TEXT, value_num REAL, details TEXT,
            FOREIGN KEY(miner_id) REFERENCES miners(id)
        );
        CREATE INDEX IF NOT EXISTS idx_events_ts ON events(ts);
        CREATE INDEX IF NOT EXISTS idx_events_miner_ts ON events(miner_id, ts);
        CREATE TABLE IF NOT EXISTS session_baselines (
            miner_id INTEGER PRIMARY KEY, accepted INTEGER, rejected INTEGER, started_at INTEGER NOT NULL,
            FOREIGN KEY(miner_id) REFERENCES miners(id)
        );
        CREATE TABLE IF NOT EXISTS block_claims (
            miner_id INTEGER PRIMARY KEY, found_count INTEGER NOT NULL DEFAULT 0,
            pool_key TEXT NOT NULL DEFAULT '', found_at INTEGER, active INTEGER NOT NULL DEFAULT 0,
            FOREIGN KEY(miner_id) REFERENCES miners(id)
        );
        CREATE TABLE IF NOT EXISTS pool_block_claims (
            miner_id INTEGER PRIMARY KEY, pool_address TEXT NOT NULL, block_hash TEXT NOT NULL,
            block_height INTEGER, found_at INTEGER, active INTEGER NOT NULL DEFAULT 1,
            FOREIGN KEY(miner_id) REFERENCES miners(id)
        );
        """)
        defaults = {
            "share_emoji": "🎉",
            "share_emoji_sha256": "",
            "share_emoji_blake3": "",
            "share_emoji_default": "",
            "animation_density": "7",
            "celebrate_rejected": "false",
            "theme": "midnight",
            "card_opacity": "0.82",
            "blur_px": "14",
            "background_intensity": "1.0",
            "compact_cards": "false",
            "block_api_base": "https://mempool.space/api",
            "btc_wallet_address": "",
            "bch_wallet_address": "",
            "btc_solopool_address": "",
            "bch_solopool_address": "",
            "btc_solo_hashrate": "0",
            "btc_solo_hashrate_unit": "TH",
            "bch_solo_hashrate": "0",
            "bch_solo_hashrate_unit": "TH",
        }
        for k,v in defaults.items():
            c.execute("INSERT OR IGNORE INTO settings(key,value) VALUES(?,?)", (k,v))


def get_settings():
    with db() as c:
        rows = {r["key"]: r["value"] for r in c.execute("SELECT key,value FROM settings")}
    legacy = rows.get("share_emoji", "🎉")
    return {
        "share_emoji": legacy,
        "share_emoji_sha256": rows.get("share_emoji_sha256") or legacy,
        "share_emoji_blake3": rows.get("share_emoji_blake3") or legacy,
        "share_emoji_default": rows.get("share_emoji_default") or legacy,
        "animation_density": int(rows.get("animation_density", "7")),
        "celebrate_rejected": rows.get("celebrate_rejected", "false") == "true",
    }


def get_customization():
    with db() as c:
        rows = {r["key"]: r["value"] for r in c.execute("SELECT key,value FROM settings")}
    return {
        "theme": rows.get("theme", "midnight"),
        "card_opacity": float(rows.get("card_opacity", "0.82")),
        "blur_px": int(rows.get("blur_px", "14")),
        "background_intensity": float(rows.get("background_intensity", "1.0")),
        "compact_cards": rows.get("compact_cards", "false") == "true",
        "block_api_base": rows.get("block_api_base", "https://mempool.space/api").rstrip("/"),
        "btc_wallet_address": rows.get("btc_wallet_address", "").strip(),
        "bch_wallet_address": rows.get("bch_wallet_address", "").strip(),
        "btc_solopool_address": rows.get("btc_solopool_address", "").strip(),
        "bch_solopool_address": rows.get("bch_solopool_address", "").strip(),
        "btc_solo_hashrate": float(rows.get("btc_solo_hashrate", "0") or 0),
        "btc_solo_hashrate_unit": rows.get("btc_solo_hashrate_unit", "TH").upper(),
        "bch_solo_hashrate": float(rows.get("bch_solo_hashrate", "0") or 0),
        "bch_solo_hashrate_unit": rows.get("bch_solo_hashrate_unit", "TH").upper(),
    }


def save_customization(body: CustomizationIn):
    data = body.model_dump()
    with db() as c:
        for k, v in data.items():
            if isinstance(v, bool):
                value = "true" if v else "false"
            else:
                value = str(v)
            c.execute("INSERT OR REPLACE INTO settings(key,value) VALUES(?,?)", (k, value))
    return get_customization()


def num(v):
    try:
        if v is None:
            return None
        if isinstance(v, str):
            v = v.strip().replace(",", "")
        return float(v)
    except Exception:
        return None


def find_number(obj: Any, keys: list[str]) -> float | None:
    wanted = {k.lower().replace("_","").replace("-","") for k in keys}
    def walk(x):
        if isinstance(x, dict):
            for k, v in x.items():
                nk = str(k).lower().replace("_","").replace("-","")
                if nk in wanted:
                    n = num(v)
                    if n is not None:
                        return n
            for v in x.values():
                z = walk(v)
                if z is not None:
                    return z
        elif isinstance(x, list):
            for v in x:
                z = walk(v)
                if z is not None:
                    return z
        return None
    return walk(obj)


def find_value(obj: Any, keys: list[str]):
    wanted = {k.lower().replace("_","").replace("-","") for k in keys}
    def walk(x):
        if isinstance(x, dict):
            for k, v in x.items():
                nk = str(k).lower().replace("_","").replace("-","")
                if nk in wanted:
                    return v
            for v in x.values():
                z = walk(v)
                if z is not None:
                    return z
        elif isinstance(x, list):
            for v in x:
                z = walk(v)
                if z is not None:
                    return z
        return None
    return walk(obj)


def clean_ip(value: str) -> str:
    """Accept plain IPs, URLs, and accidental Markdown-style links."""
    s = (value or "").strip()
    # Markdown link: [http://192.168.0.13/](http://192.168.0.13/)
    m = re.search(r"\]\((https?://[^)]+)\)", s)
    if m:
        s = m.group(1)
    s = s.strip("[]()<> ")
    s = re.sub(r"^https?://", "", s, flags=re.I)
    return s.strip("/")


def _first_dict(container: dict[str, Any], key: str) -> dict[str, Any]:
    v = container.get(key)
    if isinstance(v, list) and v and isinstance(v[0], dict):
        return v[0]
    return {}


def parse_avalon_mm_status(text: str) -> dict[str, Any]:
    out: dict[str, Any] = {}
    if not text:
        return out
    patterns = {
        "temp_c": r"TAvg\[(\d+(?:\.\d+)?)\]",
        "temp_max_c": r"TMax\[(\d+(?:\.\d+)?)\]",
        "ambient_c": r"Temp\[(\d+(?:\.\d+)?)\]",
        "exhaust_c": r"OTemp\[(\d+(?:\.\d+)?)\]",
        "fan_rpm": r"Fan1\[(\d+)\]",
        "fan_pct": r"FanR\[(\d+)%\]",
        "ghs_spd": r"GHSspd\[(\d+(?:\.\d+)?)\]",
        "ghs_avg": r"GHSavg\[(\d+(?:\.\d+)?)\]",
        "ghs_mm": r"GHSmm\[(\d+(?:\.\d+)?)\]",
        "worklevel": r"WORKLEVEL\[(\d+)\]",
    }
    for k, pat in patterns.items():
        m = re.search(pat, text)
        if m:
            out[k] = float(m.group(1)) if "." in m.group(1) else int(m.group(1))
    # PS[...] seems to contain current power in watts as the penultimate useful value on Nano 3.
    # Example observed: PS[0 0 0 4 2697 126 348] -> 126 W
    m = re.search(r"PS\[([^\]]+)\]", text)
    if m:
        vals = []
        for token in m.group(1).split():
            try:
                vals.append(float(token))
            except Exception:
                pass
        if len(vals) >= 2:
            out["power_w"] = vals[-2]
    # Preserve firmware strings if present.
    m = re.search(r"Ver\[([^\]]+)\]", text)
    if m:
        out["fw_version"] = m.group(1)
    return out


def parse_avalon(parts: dict[str, Any]) -> Telemetry:
    summary = _first_dict(parts.get("summary", {}), "SUMMARY")
    dev = _first_dict(parts.get("devs", {}), "DEVS")
    pools = parts.get("pools", {}).get("POOLS") if isinstance(parts.get("pools"), dict) else None
    pools = pools if isinstance(pools, list) else []
    stats_rows = parts.get("stats", {}).get("STATS") if isinstance(parts.get("stats"), dict) else None
    stats_rows = stats_rows if isinstance(stats_rows, list) else []

    mm_text = ""
    for row in stats_rows:
        if isinstance(row, dict) and row.get("ID") == "AVANANO0":
            mm_text = str(row.get("MM ID0", ""))
            break
    mm = parse_avalon_mm_status(mm_text)

    # Prefer the 5-minute cgminer rate for a stable "real hashrate" display.
    # cgminer reports Nano 3 rates in MH/s; divide by 1,000,000 to get TH/s.
    mhs = summary.get("MHS 5m")
    if mhs is None:
        mhs = dev.get("MHS 5m")
    if mhs is None:
        mhs = summary.get("MHS av")
    hashrate_th = float(mhs) / 1_000_000 if mhs is not None else None

    accepted = summary.get("Accepted", dev.get("Accepted"))
    rejected = summary.get("Rejected", dev.get("Rejected"))
    best = summary.get("Best Share", summary.get("Best Session Share"))
    if best is None:
        best = dev.get("Best Share", dev.get("Best Session Share"))
    if best is None:
        best = find_value(parts, ["Best Share", "Best Session Share"])
    uptime = summary.get("Elapsed", dev.get("Device Elapsed"))

    primary_pool = None
    for p in pools:
        if isinstance(p, dict) and p.get("Priority") == 0:
            primary_pool = p
            break
    if primary_pool is None and pools:
        primary_pool = pools[0] if isinstance(pools[0], dict) else None

    pool_alive = None
    if primary_pool:
        pool_alive = str(primary_pool.get("Status", "")).lower() == "alive"

    power = mm.get("power_w")
    efficiency = (power / hashrate_th) if power and hashrate_th and hashrate_th > 0 else None
    exhaust_f = None
    if mm.get("exhaust_c") is not None:
        exhaust_f = (float(mm["exhaust_c"]) * 9 / 5) + 32

    work_mode = None
    wl = mm.get("worklevel")
    if wl is not None:
        work_mode = {0: "LOW", 1: "MEDIUM", 2: "HIGH"}.get(int(wl), f"LEVEL {wl}")

    return Telemetry(
        online=True,
        hashrate=hashrate_th,
        hashrate_unit="TH/s",
        temp_c=float(mm["temp_c"]) if mm.get("temp_c") is not None else None,
        exhaust_temp_f=exhaust_f,
        power_w=float(power) if power is not None else None,
        efficiency=efficiency,
        efficiency_unit="W/TH",
        accepted=int(accepted) if accepted is not None else None,
        rejected=int(rejected) if rejected is not None else None,
        best_share=str(best) if best is not None else None,
        pool_alive=pool_alive,
        uptime_s=int(uptime) if uptime is not None else None,
        fan_rpm=int(mm["fan_rpm"]) if mm.get("fan_rpm") is not None else None,
        work_mode=work_mode,
        raw={
            "summary": summary,
            "device": dev,
            "avalon_mm": mm,
            # Nano 3 reports the assigned pool target as "Last Share
            # Difficulty". It is not the actual hash difficulty of the last
            # submitted share, so it must not populate Current Share Difficulty.
            "ignore_reported_current_share": True,
            "primary_pool": primary_pool,
        },
    )



def parse_axeos(raw: dict[str, Any]) -> Telemetry:
    """AxeOS / ESP-Miner adapter for Bitaxe, NerdQaxe and NerdOCTAXE devices."""
    model = str(raw.get("deviceModel", ""))
    hash_gh = num(raw.get("hashRate_10m"))
    if hash_gh is None:
        hash_gh = num(raw.get("hashRate_1m"))
    if hash_gh is None:
        hash_gh = num(raw.get("hashRate"))
    hashrate_th = (hash_gh / 1000.0) if hash_gh is not None else None

    power = num(raw.get("power"))
    temp = num(raw.get("temp"))
    vr_temp = num(raw.get("vrTemp"))
    accepted = num(raw.get("sharesAccepted"))
    rejected = num(raw.get("sharesRejected"))
    best = raw.get("bestDiff")
    if best is None:
        best = raw.get("bestSessionDiff")
    uptime = num(raw.get("uptimeSeconds"))

    fan_values = [num(raw.get("fanrpm")), num(raw.get("fan2rpm"))]
    fan_values = [f for f in fan_values if f is not None and f > 0]
    fan = sum(fan_values) / len(fan_values) if fan_values else None

    # Build a normalized primary-pool object from AxeOS system info.
    primary_pool = None
    pools = raw.get("pools")
    try:
        primary_idx = int(raw.get("primaryPoolIndex", 0))
    except Exception:
        primary_idx = 0
    if isinstance(pools, list) and pools and 0 <= primary_idx < len(pools) and isinstance(pools[primary_idx], dict):
        primary_pool = dict(pools[primary_idx])
    if primary_pool is None and raw.get("stratumURL"):
        primary_pool = {
            "stratumURL": raw.get("stratumURL"),
            "stratumPort": raw.get("stratumPort"),
            "stratumUser": raw.get("stratumUser"),
        }
    if primary_pool:
        url = primary_pool.get("stratumURL") or raw.get("stratumURL")
        port = primary_pool.get("stratumPort") or raw.get("stratumPort")
        if url and port:
            url_s = str(url)
            primary_pool["displayURL"] = (
                url_s if ":" in url_s.rsplit("/", 1)[-1]
                else f"stratum+tcp://{url_s}:{port}"
            )
        else:
            primary_pool["displayURL"] = url
        primary_pool["poolDifficulty"] = raw.get("poolDifficulty")
        primary_pool["Accepted"] = int(accepted) if accepted is not None else None
        primary_pool["Rejected"] = int(rejected) if rejected is not None else None
        primary_pool["Status"] = "Alive"

    pool_alive = True if hashrate_th is not None and hashrate_th > 0 else None
    efficiency = power / hashrate_th if power is not None and hashrate_th and hashrate_th > 0 else None

    return Telemetry(
        online=True,
        hashrate=hashrate_th,
        hashrate_unit="TH/s",
        temp_c=temp,
        power_w=power,
        efficiency=efficiency,
        efficiency_unit="W/TH",
        accepted=int(accepted) if accepted is not None else None,
        rejected=int(rejected) if rejected is not None else None,
        best_share=str(best) if best is not None else None,
        pool_alive=pool_alive,
        uptime_s=int(uptime) if uptime is not None else None,
        fan_rpm=int(fan) if fan is not None else None,
        raw={
            "deviceModel": model,
            "ASICModel": raw.get("ASICModel"),
            "hostip": raw.get("hostip") or raw.get("ipv4"),
            "wifiRSSI": raw.get("wifiRSSI"),
            "asicCount": raw.get("asicCount"),
            "smallCoreCount": raw.get("smallCoreCount"),
            "power": power,
            "currentA": raw.get("currentA"),
            "temp": temp,
            "vrTemp": vr_temp,
            "fanrpm": raw.get("fanrpm"),
            "fan2rpm": raw.get("fan2rpm"),
            "hashRate": raw.get("hashRate"),
            "hashRate_1m": raw.get("hashRate_1m"),
            "hashRate_10m": raw.get("hashRate_10m"),
            "hashRate_1h": raw.get("hashRate_1h"),
            "hashRate_1d": raw.get("hashRate_1d"),
            "bestDiff": raw.get("bestDiff"),
            "bestSessionDiff": raw.get("bestSessionDiff"),
            "poolDifficulty": raw.get("poolDifficulty"),
            "networkDifficulty": raw.get("networkDifficulty"),
            "blockHeight": raw.get("blockHeight"),
            "blockFound": raw.get("blockFound"),
            "stratumURL": raw.get("stratumURL"),
            "stratumPort": raw.get("stratumPort"),
            "stratumUser": raw.get("stratumUser"),
            "primary_pool": primary_pool,
            "axeOSVersion": raw.get("axeOSVersion"),
            "version": raw.get("version"),
            "coreVoltage": raw.get("coreVoltage"),
            "frequency": raw.get("frequency"),
            "full": raw,
        },
    )


def parse_luxos(parts: dict[str, Any]) -> Telemetry:
    summary = _first_dict(parts.get("summary", {}), "SUMMARY")
    stats_rows = parts.get("stats", {}).get("STATS") if isinstance(parts.get("stats"), dict) else []
    stats_rows = stats_rows if isinstance(stats_rows, list) else []
    pools = parts.get("pools", {}).get("POOLS") if isinstance(parts.get("pools"), dict) else []
    pools = pools if isinstance(pools, list) else []
    version = _first_dict(parts.get("version", {}), "VERSION")

    ghs = summary.get("GHS 5m")
    if ghs is None:
        ghs = summary.get("GHS av")
    hashrate_th = float(ghs) / 1000.0 if ghs is not None else None

    miner_stats = {}
    for row in stats_rows:
        if isinstance(row, dict) and str(row.get("ID","")).startswith("BTM_SOC"):
            miner_stats = row
            break

    temps, fans = [], []
    for k, v in miner_stats.items():
        lk = str(k).lower()
        if re.fullmatch(r"temp\d+", lk):
            n = num(v)
            if n is not None and n > 0:
                temps.append(n)
        if re.fullmatch(r"fan\d+", lk):
            n = num(v)
            if n is not None and n > 0:
                fans.append(n)

    # LuxOS exposes power as its own LUXminer command on TCP 4028:
    # {"command":"power"} -> {"POWER":[{"PSU": true/false, "Watts": ...}]}
    power_info = _first_dict(parts.get("power", {}), "POWER")
    power = num(power_info.get("Watts")) if power_info else None
    power_is_psu_reported = power_info.get("PSU") if power_info else None

    # Backward-compatible fallbacks for firmware variants that may include watts elsewhere.
    if power is None:
        power = find_number(miner_stats, ["power", "power_w", "wattage", "watts", "powerusage"])
    if power is None:
        power = find_number(parts, ["power_w", "wattage", "watts", "powerusage"])

    primary_pool = None
    for p in pools:
        if isinstance(p, dict) and p.get("Priority") == 0:
            primary_pool = p
            break
    if primary_pool is None and pools:
        primary_pool = pools[0] if isinstance(pools[0], dict) else None

    pool_alive = str(primary_pool.get("Status","")).lower() == "alive" if primary_pool else None
    accepted = summary.get("Accepted")
    rejected = summary.get("Rejected")
    best = summary.get("Best Session Share", summary.get("Best Share"))
    uptime = summary.get("Elapsed")
    efficiency = (power / hashrate_th) if power and hashrate_th else None

    return Telemetry(
        online=True,
        hashrate=hashrate_th,
        hashrate_unit="TH/s",
        temp_c=(sum(temps)/len(temps)) if temps else None,
        power_w=power,
        efficiency=efficiency,
        efficiency_unit="W/TH",
        accepted=int(accepted) if accepted is not None else None,
        rejected=int(rejected) if rejected is not None else None,
        best_share=str(best) if best is not None else None,
        pool_alive=pool_alive,
        uptime_s=int(uptime) if uptime is not None else None,
        fan_rpm=int(sum(fans)/len(fans)) if fans else None,
        work_mode=str(miner_stats.get("Mode")) if miner_stats.get("Mode") is not None else None,
        raw={
            "type": version.get("Type"),
            "luxminer": version.get("LUXminer"),
            "miner_stats": miner_stats,
            "primary_pool": primary_pool,
            "stale": summary.get("Stale"),
            "hardware_errors": summary.get("Hardware Errors"),
            "power_w": power,
            "power_is_psu_reported": power_is_psu_reported,
            "power_source": "PSU" if power_is_psu_reported is True else ("LuxOS estimate" if power_is_psu_reported is False else None),
            "full": parts,
        }
    )


def parse_goldshell(dev_http: dict[str, Any] | None, parts: dict[str, Any]) -> Telemetry:
    summary = _first_dict(parts.get("summary", {}), "SUMMARY")
    pools = parts.get("pools", {}).get("POOLS") if isinstance(parts.get("pools"), dict) else []
    pools = pools if isinstance(pools, list) else []
    devs = parts.get("devs", {}).get("DEVS") if isinstance(parts.get("devs"), dict) else []
    devs = devs if isinstance(devs, list) else []

    http_dev = {}
    if isinstance(dev_http, dict):
        data = dev_http.get("data")
        if isinstance(data, list) and data and isinstance(data[0], dict):
            http_dev = data[0]

    cgdev = devs[0] if devs and isinstance(devs[0], dict) else {}

    mhs = num(http_dev.get("av_hashrate"))
    if mhs is None:
        mhs = num(summary.get("MHS av"))
    hashrate_gh = (mhs / 1000.0) if mhs is not None else None

    temp = None
    raw_temp = http_dev.get("temp")
    if raw_temp is not None:
        m = re.search(r"(-?\d+(?:\.\d+)?)", str(raw_temp))
        if m:
            temp = float(m.group(1))
    if temp is None:
        temp = find_number(cgdev, ["tstemp-2", "temperature", "temp"])

    fan = None
    fan_s = http_dev.get("fanspeed")
    if fan_s:
        nums = [int(x) for x in re.findall(r"(\d+)\s*rpm", str(fan_s), flags=re.I)]
        if nums:
            fan = int(sum(nums)/len(nums))
    if fan is None:
        f0, f1 = num(cgdev.get("fan0")), num(cgdev.get("fan1"))
        fs = [x for x in (f0,f1) if x is not None and x > 0]
        if fs:
            fan = int(sum(fs)/len(fs))

    accepted = http_dev.get("accepted", summary.get("Accepted"))
    rejected = http_dev.get("rejected", summary.get("Rejected"))
    best = summary.get("Best Share")
    uptime = http_dev.get("time", summary.get("Elapsed"))

    primary_pool = None
    for p in pools:
        if isinstance(p, dict) and p.get("Priority") == 0:
            primary_pool = p
            break
    if primary_pool is None and pools:
        primary_pool = pools[0] if isinstance(pools[0], dict) else None

    pool_alive = str(primary_pool.get("Status","")).lower()=="alive" if primary_pool else None

    return Telemetry(
        online=True,
        hashrate=hashrate_gh,
        hashrate_unit="GH/s",
        temp_c=temp,
        power_w=None,
        efficiency=None,
        efficiency_unit="W/GH",
        accepted=int(accepted) if accepted is not None else None,
        rejected=int(rejected) if rejected is not None else None,
        best_share=str(best) if best is not None else None,
        pool_alive=pool_alive,
        uptime_s=int(uptime) if uptime is not None else None,
        fan_rpm=fan,
        raw={
            "http_dev": http_dev,
            "cgminer_dev": cgdev,
            "primary_pool": primary_pool,
            "hardware_errors": http_dev.get("hwerrors", cgdev.get("Hardware Errors")),
            "hw_error_ratio": http_dev.get("hwerr_ration", cgdev.get("hwerr-ration")),
            "clock": cgdev.get("clock"),
            "voltage": cgdev.get("voltage"),
            "full": parts,
        }
    )


def parse_iceriver(raw: dict[str, Any], endpoint: str) -> Telemetry:
    """
    IceRiver stock UI parser. Stock firmware commonly exposes telemetry through
    /user/machine and related /user/* JSON routes.
    Field names vary between firmware revisions, so this adapter uses a narrow
    set of mining-specific aliases while keeping the full response for inspection.
    """

    # Hashrate aliases seen across IceRiver web UIs/firmware generations.
    h = find_number(raw, [
        "hashrate", "hash_rate", "rt_hashrate", "real_hashrate",
        "current_hashrate", "instant_hashrate", "avg_hashrate",
        "hashrate_avg", "hashrate_10m", "hashrate_1h", "total_hashrate"
    ])

    # IceRiver AL-series dashboards generally display Blake3 hashrate in GH/s.
    # If the API exposes MH/s-specific field names, normalize to GH/s.
    h_unit = "GH/s"
    mh = find_number(raw, [
        "mhs", "mhs_av", "mhs_5s", "mhs_avg", "hashrate_mhs"
    ])
    gh = find_number(raw, [
        "ghs", "ghs_av", "ghs_5s", "ghs_avg", "hashrate_ghs"
    ])
    if gh is not None:
        h = gh
        h_unit = "GH/s"
    elif mh is not None:
        h = mh / 1000.0
        h_unit = "GH/s"

    temp = find_number(raw, [
        "temp", "temperature", "chip_temp", "asic_temp",
        "temp_chip", "temperature_chip", "max_temp"
    ])
    power = find_number(raw, [
        "power", "power_w", "power_consumption", "watt", "watts"
    ])
    accepted = find_number(raw, [
        "accepted", "accepted_shares", "shares_accepted", "accept"
    ])
    rejected = find_number(raw, [
        "rejected", "rejected_shares", "shares_rejected", "reject"
    ])
    best = find_value(raw, [
        "best_share", "bestshare", "best_diff", "bestdifficulty", "best_difficulty"
    ])
    uptime = find_number(raw, [
        "uptime", "uptime_s", "elapsed", "running_time", "runtime"
    ])
    fan = find_number(raw, [
        "fan_rpm", "fanrpm", "fan_speed", "fanspeed", "fan1"
    ])

    pool_alive = None
    pool_status = find_value(raw, [
        "pool_status", "poolstatus", "stratum_status", "stratumconnected",
        "pool_alive", "pool_state"
    ])
    if isinstance(pool_status, bool):
        pool_alive = pool_status
    elif pool_status is not None:
        s = str(pool_status).strip().lower()
        if s in ("alive", "connected", "true", "1", "online", "ok"):
            pool_alive = True
        elif s in ("dead", "disconnected", "false", "0", "offline", "error"):
            pool_alive = False

    efficiency = (power / h) if power and h and h > 0 else None

    return Telemetry(
        online=True,
        hashrate=h,
        hashrate_unit=h_unit,
        temp_c=temp,
        power_w=power,
        efficiency=efficiency,
        efficiency_unit="W/GH",
        accepted=int(accepted) if accepted is not None else None,
        rejected=int(rejected) if rejected is not None else None,
        best_share=str(best) if best is not None else None,
        pool_alive=pool_alive,
        uptime_s=int(uptime) if uptime is not None else None,
        fan_rpm=int(fan) if fan is not None else None,
        raw={"endpoint": endpoint, "full": raw},
    )


def _parse_hash_to_gh(value: Any, declared_unit: str | None = None) -> float | None:
    """Normalize IceRiver string/numeric hashrates to GH/s without hard-coding a model rate."""
    if value is None:
        return None
    s = str(value).strip()
    if not s:
        return None

    # Prefer an explicit suffix in the value itself.
    m = re.search(r"(-?\d+(?:\.\d+)?)\s*([KMGTPE]?)(?:H(?:/s)?)?", s, flags=re.I)
    if not m:
        return num(value)
    n = float(m.group(1))
    prefix = (m.group(2) or "").upper()

    unit = (declared_unit or "").strip().upper()
    if prefix:
        unit = prefix
    unit = unit.replace("H/S", "").replace("H", "").replace("/S", "").strip()

    # Convert all supported display prefixes to GH/s.
    factors_to_gh = {
        "": 1e-9, "K": 1e-6, "M": 1e-3, "G": 1.0,
        "T": 1e3, "P": 1e6, "E": 1e9,
    }
    return n * factors_to_gh.get(unit, 1.0)


def _iceriver_pool_summary(data: dict[str, Any]) -> tuple[int | None, int | None, bool | None, dict[str, Any] | None]:
    accepted = rejected = 0
    saw_share_counts = False
    primary = None
    connected = None
    pools = data.get("pools")
    if isinstance(pools, list):
        for p in pools:
            if not isinstance(p, dict):
                continue
            if primary is None or p.get("priority") == 0 or p.get("no") == 1:
                primary = p
            a = num(p.get("accepted"))
            r = num(p.get("rejected"))
            if a is not None:
                accepted += int(a); saw_share_counts = True
            if r is not None:
                rejected += int(r); saw_share_counts = True
        if primary:
            c = primary.get("connect")
            if c == 1 or c is True:
                connected = True
            elif c is not None:
                connected = False
    return (
        accepted if saw_share_counts else None,
        rejected if saw_share_counts else None,
        connected,
        primary
    )


def parse_iceriver_post4(envelope: dict[str, Any], diag: dict[str, Any] | None = None) -> Telemetry:
    """
    Parse the exact stock-firmware envelope returned by:
      POST /user/userpanel
      application/x-www-form-urlencoded: post=4

    The firmware's own index.js sends re.data to content1..content7.
    """
    error = envelope.get("error")
    data = envelope.get("data")

    # Server answered, but a successful HTTP response is not enough.
    if error not in (0, "0", None) or not isinstance(data, dict):
        msg = envelope.get("message") or "IceRiver post=4 did not return telemetry data"
        return Telemetry(
            online=False,
            reachable=True,
            authenticated=False if error not in (0, "0", None) else None,
            telemetry_available=False,
            error=str(msg),
            raw={"post4_envelope": envelope, "diagnostic": diag or {}},
        )

    # Hashrate: prefer average/current named fields, then firmware chart/display fields.
    declared_unit = str(data.get("pows_unit") or data.get("unit") or "")
    realtime_candidates = [
        "rtpow", "rt_pow", "realtimepow", "realpow", "real_time_hash",
        "hashrate", "hash_rate", "current_hashrate", "currentpow"
    ]
    avg_candidates = [
        "avgpow", "avg_pow", "averagepow", "average_hashrate",
        "avg_hashrate", "totalpow", "total_hashrate"
    ]
    h_gh = None
    chosen_hash_field = None
    for key in realtime_candidates + avg_candidates:
        if key in data and data.get(key) not in (None, ""):
            h_gh = _parse_hash_to_gh(data.get(key), declared_unit)
            chosen_hash_field = key
            if h_gh is not None:
                break

    # Some firmware keeps current/average values on boards.
    boards = data.get("boards") if isinstance(data.get("boards"), list) else []
    if h_gh is None and boards:
        board_total = 0.0
        board_seen = False
        for b in boards:
            if not isinstance(b, dict):
                continue
            v = b.get("avgpow", b.get("rtpow"))
            gh = _parse_hash_to_gh(v, declared_unit)
            if gh is not None:
                board_total += gh
                board_seen = True
        if board_seen:
            h_gh = board_total
            chosen_hash_field = "boards[].avgpow/rtpow"

    # Temperatures from boards. Keep inlet/outlet arrays in raw diagnostics.
    inlet_temps, outlet_temps = [], []
    hw_errors = 0
    hw_seen = False
    for b in boards:
        if not isinstance(b, dict):
            continue
        it = num(b.get("intmp"))
        ot = num(b.get("outtmp"))
        if it is not None: inlet_temps.append(it)
        if ot is not None: outlet_temps.append(ot)
        he = num(b.get("hwerr", b.get("hwerrors", b.get("error"))))
        if he is not None:
            hw_errors += int(he); hw_seen = True

    # The stock UI labels "Miner temp." plus board inlet/outlet temperatures.
    temp = find_number(data, ["temp", "temperature", "miner_temp", "minertemp"])
    if temp is None:
        # Stock AL0 exposes board intmp/outtmp. Use the hottest reported board sensor
        # for the fleet "Avg Temperature" safety-oriented summary instead of arbitrarily
        # preferring one label whose physical placement can vary by firmware/hardware.
        sensor_temps = inlet_temps + outlet_temps
        if sensor_temps:
            temp = max(sensor_temps)

    # Fans may be objects/list or numbered scalar fields.
    fan_values = []
    fans = data.get("fans")
    if isinstance(fans, list):
        for f in fans:
            if isinstance(f, dict):
                v = find_number(f, ["speed", "rpm", "fanspeed"])
            else:
                v = num(f)
            if v is not None and v > 0:
                fan_values.append(v)
    for k, v in data.items():
        if re.fullmatch(r"fan\d+", str(k).lower()):
            n = num(v)
            if n is not None and n > 0:
                fan_values.append(n)
    fan_rpm = int(sum(fan_values)/len(fan_values)) if fan_values else None

    accepted, rejected, pool_alive, primary_pool = _iceriver_pool_summary(data)

    # Uptime on stock UI is "Miner running time"; support numeric and simple D/H/M/S strings.
    uptime = None
    raw_uptime = find_value(data, ["runtime", "uptime", "runningtime", "running_time", "elapsed", "up_time", "run_time", "miner_running_time"])
    if raw_uptime not in (None, ""):
        s = str(raw_uptime).strip().lower()
        # Observed stock AL0 format: DD:HH:MM:SS, e.g. 52:03:48:39.
        if re.fullmatch(r"\d+:\d{1,2}:\d{1,2}:\d{1,2}", s):
            d, h, m, sec = [int(x) for x in s.split(":")]
            uptime = d*86400 + h*3600 + m*60 + sec
        else:
            n = num(raw_uptime)
            if n is not None:
                uptime = n
            else:
                total = 0
                for value, suffix in re.findall(r"(\d+)\s*([dhms])", s):
                    total += int(value) * {"d":86400,"h":3600,"m":60,"s":1}[suffix]
                if total:
                    uptime = total

    avg_h_gh = None
    if data.get("avgpow") not in (None, ""):
        avg_h_gh = _parse_hash_to_gh(data.get("avgpow"), declared_unit)
    if avg_h_gh is None and boards:
        vals = []
        for b in boards:
            if isinstance(b, dict) and b.get("avgpow") not in (None, ""):
                v = _parse_hash_to_gh(b.get("avgpow"), declared_unit)
                if v is not None:
                    vals.append(v)
        if vals:
            avg_h_gh = sum(vals)

    power = find_number(data, ["power", "power_w", "watts", "watt", "power_consumption"])
    efficiency = (power / h_gh) if power and h_gh and h_gh > 0 else None
    best = find_value(data, ["best_share", "bestshare", "best_diff", "bestdifficulty", "best_difficulty"])

    # Preserve richer windows/metadata without pretending they are standardized.
    raw_details = {
        "chosen_hash_field": chosen_hash_field,
        "declared_hash_unit": declared_unit,
        "model_reported": data.get("model"),
        "algo_reported": data.get("algo"),
        "runtime_raw": data.get("runtime"),
        "rtpow_raw": data.get("rtpow"),
        "avgpow_raw": data.get("avgpow"),
        "firmware": data.get("firmware") or data.get("firmware_version"),
        "firmtype": data.get("firmtype"),
        "powstate": data.get("powstate"),
        "netstate": data.get("netstate"),
        "pows_5m": data.get("pows_5m") or data.get("pow5m") or data.get("hashrate_5m"),
        "pows_15m": data.get("pows_15m") or data.get("pow15m") or data.get("hashrate_15m"),
        "pows_30m": data.get("pows_30m") or data.get("pow30m") or data.get("hashrate_30m"),
        "inlet_temps": inlet_temps,
        "outlet_temps": outlet_temps,
        "hardware_errors": hw_errors if hw_seen else None,
        "boards": boards,
        "primary_pool": primary_pool,
        "all_unmapped_data": data,
        "post4_diagnostic": diag or {},
    }

    return Telemetry(
        online=True,
        reachable=True,
        authenticated=True,
        telemetry_available=True,
        hashrate=h_gh,
        hashrate_unit="GH/s",
        avg_hashrate=avg_h_gh,
        avg_hashrate_unit="GH/s",
        temp_c=temp,
        power_w=power,
        efficiency=efficiency,
        efficiency_unit="W/GH",
        accepted=accepted,
        rejected=rejected,
        best_share=str(best) if best is not None else None,
        pool_alive=pool_alive,
        uptime_s=int(uptime) if uptime is not None else None,
        fan_rpm=fan_rpm,
        raw=raw_details,
    )


def sanitize_iceriver_json(obj: Any) -> Any:
    """Remove secrets from diagnostics while preserving useful telemetry."""
    secret_tokens = ("pass", "password", "pwd", "token", "secret", "cookie", "auth")
    if isinstance(obj, dict):
        out = {}
        for k, v in obj.items():
            lk = str(k).lower()
            if any(tok in lk for tok in secret_tokens):
                out[k] = "***REDACTED***"
            else:
                out[k] = sanitize_iceriver_json(v)
        return out
    if isinstance(obj, list):
        return [sanitize_iceriver_json(x) for x in obj]
    return obj


async def iceriver_post4_request(ip: str) -> tuple[Telemetry, dict[str, Any]]:
    """
    Primary IceRiver AL-series telemetry request.
    No broad discovery and no write/config calls.
    """
    ip = clean_ip(ip)
    diag: dict[str, Any] = {
        "request": {
            "method": "POST",
            "url": f"http://{ip}/user/userpanel",
            "form": {"post": 4},
        }
    }
    try:
        async with httpx.AsyncClient(
            timeout=httpx.Timeout(8.0, connect=2.0),
            follow_redirects=False
        ) as client:
            r = await client.post(
                f"http://{ip}/user/userpanel",
                data={"post": "4"},
                headers={"X-Requested-With": "XMLHttpRequest"},
            )
            diag["response"] = {
                "status": r.status_code,
                "content_type": r.headers.get("content-type", ""),
                "set_cookie_names": sorted(list(r.cookies.keys())),
            }
            try:
                payload = r.json()
            except Exception:
                payload = None
                diag["response"]["body_preview"] = r.text[:2000]

            if not isinstance(payload, dict):
                return Telemetry(
                    online=False, reachable=True, authenticated=None,
                    telemetry_available=False,
                    error="IceRiver post=4 response was not JSON",
                    raw={"diagnostic": diag},
                ), diag

            safe = sanitize_iceriver_json(payload)
            diag["response"]["json"] = safe

            # Authentication detection is based on the firmware envelope, not HTTP reachability.
            err = payload.get("error")
            msg = str(payload.get("message") or "")
            if err not in (0, "0", None) and not isinstance(payload.get("data"), dict):
                auth_words = ("login", "auth", "password", "登录", "未登录")
                auth_needed = any(w.lower() in msg.lower() for w in auth_words if w.isascii()) or any(w in msg for w in auth_words if not w.isascii())
                t = parse_iceriver_post4(payload, diag)
                if auth_needed:
                    t.authenticated = False
                    t.error = "IceRiver authentication required: " + (msg or f"error={err}")
                return t, diag

            return parse_iceriver_post4(payload, diag), diag

    except Exception as e:
        diag["transport_error"] = f"{type(e).__name__}: {e}"
        return Telemetry(
            online=False, reachable=False, authenticated=None,
            telemetry_available=False, error=str(e), raw={"diagnostic": diag},
        ), diag

def normalize_http(raw: dict[str, Any], algorithm: str) -> Telemetry:
    h = find_number(raw, ["hashrate","real_hashrate","hash_rate","rt_hashrate","mhs_5s","ghs_5s","ths_5s","hashrate_5s"])
    unit = str(find_value(raw, ["hashrate_unit","unit"]) or "")
    temp = find_number(raw, ["temperature","temp","temp_c","chip_temp","chiptemperature","asic_temp"])
    power = find_number(raw, ["power","power_w","watt","watts"])
    accepted = find_number(raw, ["accepted","accepted_shares","shares_accepted","acn"])
    rejected = find_number(raw, ["rejected","rejected_shares","shares_rejected"])
    best = find_value(raw, ["best_share","bestshare","best_diff","bestdifficulty"])
    uptime = find_number(raw, ["uptime","uptime_s","elapsed"])
    fan = find_number(raw, ["fan_rpm","fan1","fan_speed_rpm","fanrpm"])

    # Infer units from key names when possible
    lower_dump = json.dumps(raw).lower()
    if not unit:
        if "ths" in lower_dump or algorithm.upper() == "SHA-256":
            unit = "TH/s"
        elif "ghs" in lower_dump:
            unit = "GH/s"
        else:
            unit = "MH/s"

    # Conservative normalization for common API conventions.
    if h is not None and unit.lower() in ("mh/s","mhs"):
        pass
    elif h is not None and unit.lower() in ("gh/s","ghs"):
        pass
    elif h is not None and unit.lower() in ("th/s","ths"):
        pass

    efficiency = None
    efficiency_unit = ""
    if power and h and h > 0:
        efficiency = power / h
        if unit.lower().startswith("th"):
            efficiency_unit = "W/TH"
        elif unit.lower().startswith("gh"):
            efficiency_unit = "W/GH"
        else:
            efficiency_unit = "W/MH"

    return Telemetry(
        online=True, hashrate=h, hashrate_unit=unit, temp_c=temp, power_w=power,
        efficiency=efficiency, efficiency_unit=efficiency_unit,
        accepted=int(accepted) if accepted is not None else None,
        rejected=int(rejected) if rejected is not None else None,
        best_share=str(best) if best is not None else None,
        pool_alive=True, uptime_s=int(uptime) if uptime is not None else None,
        fan_rpm=int(fan) if fan is not None else None, raw=raw
    )


async def http_probe(ip: str, algorithm: str, family: str) -> Telemetry | None:
    # These endpoints are intentionally broad. Different firmware versions expose
    # different paths; successful JSON is normalized heuristically.
    endpoint_sets = {
        "axeos": ["/api/system/info", "/api/system", "/api/info"],
        "iceriver": [
            "/user/machine", "/user/userpanel", "/user/system", "/user/ip",
            "/api/user/overview", "/api/overview", "/api/status", "/api/system/info"
        ],
        "goldshell": ["/mcb/cgminer?cgminercmd=summary", "/mcb/cgminer?cgminercmd=devs", "/api/status", "/api/system/info"],
        "auto": ["/api/system/info", "/api/user/overview", "/api/overview", "/api/status", "/mcb/cgminer?cgminercmd=summary"],
    }
    paths = endpoint_sets.get(family, endpoint_sets["auto"])
    async with httpx.AsyncClient(timeout=2.0, follow_redirects=True) as client:
        for scheme in ("http", "https"):
            for path in paths:
                try:
                    r = await client.get(f"{scheme}://{ip}{path}")
                    if r.status_code < 400:
                        ct = r.headers.get("content-type","")
                        if "json" in ct.lower() or r.text.lstrip().startswith(("{","[")):
                            data = r.json()
                            if isinstance(data, list):
                                data = {"data": data}
                            if isinstance(data, dict):
                                axeos_signature = (
                                    path == "/api/system/info"
                                    and ("hashRate" in data or "hashRate_10m" in data)
                                    and ("deviceModel" in data or "ASICModel" in data)
                                )
                                if (family == "axeos" and path == "/api/system/info") or axeos_signature:
                                    t = parse_axeos(data)
                                    # AxeOS keeps individual share difficulty records in
                                    # its scoreboard. The entry with the lowest age is the
                                    # newest share currently retained by the firmware.
                                    try:
                                        sr = await client.get(f"{scheme}://{ip}/api/system/scoreboard")
                                        if sr.status_code < 400:
                                            scoreboard = sr.json()
                                            if isinstance(scoreboard, dict):
                                                scoreboard = scoreboard.get("scoreboard") or scoreboard.get("data") or []
                                            entries = [x for x in scoreboard if isinstance(x, dict)] if isinstance(scoreboard, list) else []
                                            newest = min(
                                                (x for x in entries if num(x.get("difficulty")) is not None and num(x.get("since")) is not None),
                                                key=lambda x: num(x.get("since")),
                                                default=None,
                                            )
                                            if newest is not None:
                                                t.current_share = str(newest["difficulty"])
                                                t.raw = {**(t.raw or {}), "latest_scoreboard_share": newest}
                                    except Exception:
                                        pass
                                    t.raw = {"endpoint": path, **(t.raw or {})}
                                    return t
                                if family == "iceriver" and path.startswith("/user/"):
                                    t = parse_iceriver(data, path)
                                    return t
                                t = normalize_http(data, algorithm)
                                t.raw = {"endpoint": path, "data": data}
                                return t
                except Exception:
                    continue
    return None


def cgminer_request(ip: str, command: str) -> dict[str, Any] | None:
    payload = json.dumps({"command": command}).encode()
    try:
        with socket.create_connection((ip, 4028), timeout=2.0) as s:
            s.sendall(payload)
            chunks = []
            while True:
                b = s.recv(65536)
                if not b:
                    break
                chunks.append(b)
        raw = b"".join(chunks).replace(b"\x00", b"").decode(errors="ignore").strip()
        if not raw:
            return None
        try:
            return json.loads(raw)
        except Exception:
            return {"text": raw}
    except Exception:
        return None


async def cgminer_probe(ip: str, algorithm: str, family: str = "") -> Telemetry | None:
    ip = clean_ip(ip)
    loop = asyncio.get_running_loop()
    parts = {}
    commands = ["summary", "stats", "pools", "devs", "version"]
    if family == "luxos":
        commands.append("power")
    for cmd in commands:
        r = await loop.run_in_executor(None, cgminer_request, ip, cmd)
        if r:
            parts[cmd] = r
    if not parts:
        return None

    # Exact family adapters from observed live diagnostics.
    if family == "luxos":
        return parse_luxos(parts)

    # Exact Avalon Nano adapter from the observed cgminer 4.11.1 / API 3.7 response.
    version = parts.get("version", {})
    prod = find_value(version, ["PROD"])
    model = find_value(version, ["MODEL"])
    if family == "avalon" or str(prod).lower() == "avalonnano" or str(model).lower() == "nano3":
        return parse_avalon(parts)

    t = normalize_http(parts, algorithm)
    summary = _first_dict(parts.get("summary", {}), "SUMMARY")
    pools = parts.get("pools", {}).get("POOLS") if isinstance(parts.get("pools"), dict) else []
    pools = pools if isinstance(pools, list) else []
    primary_pool = next((p for p in pools if isinstance(p, dict) and p.get("Priority") == 0), None)
    if primary_pool is None and pools:
        primary_pool = pools[0] if isinstance(pools[0], dict) else None

    if t.accepted is None and summary.get("Accepted") is not None:
        t.accepted = int(summary["Accepted"])
    if t.rejected is None and summary.get("Rejected") is not None:
        t.rejected = int(summary["Rejected"])
    if t.best_share is None:
        generic_best = summary.get("Best Share", summary.get("Best Session Share"))
        if generic_best is not None:
            t.best_share = str(generic_best)
    if primary_pool:
        t.pool_alive = str(primary_pool.get("Status", "")).lower() == "alive"

    t.raw = {"primary_pool": primary_pool, "full": parts}
    for key, unit in [
        ("THS 5m","TH/s"), ("GHS 5m","GH/s"), ("MHS 5m","MH/s"),
        ("THS av","TH/s"), ("GHS av","GH/s"), ("MHS av","MH/s"),
    ]:
        v = find_number(parts,[key])
        if v is not None:
            t.hashrate, t.hashrate_unit = v, unit
            break
    if t.power_w and t.hashrate:
        t.efficiency = t.power_w / t.hashrate
        t.efficiency_unit = {"TH/s":"W/TH","GH/s":"W/GH","MH/s":"W/MH"}.get(t.hashrate_unit,"")
    return t


async def goldshell_probe(ip: str, algorithm: str) -> Telemetry | None:
    ip = clean_ip(ip)
    dev_http = None
    try:
        async with httpx.AsyncClient(timeout=2.0) as client:
            r = await client.get(f"http://{ip}/mcb/cgminer?cgminercmd=devs")
            if r.status_code < 400:
                dev_http = r.json()
    except Exception:
        pass

    loop = asyncio.get_running_loop()
    parts = {}
    for cmd in ("summary", "pools", "devs", "version"):
        r = await loop.run_in_executor(None, cgminer_request, ip, cmd)
        if r:
            parts[cmd] = r

    if not dev_http and not parts:
        return None
    return parse_goldshell(dev_http, parts)


async def poll_miner(miner: sqlite3.Row) -> Telemetry:
    family = miner["family"]
    algorithm = miner["algorithm"]
    ip = clean_ip(miner["ip"])

    try:
        if family in ("cgminer","avalon","luxos"):
            t = await cgminer_probe(ip, algorithm, family)
            if t: return t
            t = await http_probe(ip, algorithm, "auto")
            if t: return t
        elif family == "goldshell":
            t = await goldshell_probe(ip, algorithm)
            if t: return t
        elif family == "iceriver":
            t, _diag = await iceriver_post4_request(ip)
            return t
        elif family == "axeos":
            t = await http_probe(ip, algorithm, family)
            if t: return t
            t = await cgminer_probe(ip, algorithm, family)
            if t: return t
        else:
            t = await http_probe(ip, algorithm, "auto")
            if t: return t
            t = await cgminer_probe(ip, algorithm, family)
            if t: return t
        return Telemetry(error="No supported local API responded")
    except Exception as e:
        return Telemetry(error=str(e))


def record_event(miner_id: int | None, event_type: str, value_text: str | None = None, value_num: float | None = None, details: dict[str, Any] | None = None):
    with db() as c:
        c.execute("INSERT INTO events(miner_id,ts,event_type,value_text,value_num,details) VALUES(?,?,?,?,?,?)", (miner_id,int(time.time()),event_type,value_text,value_num,json.dumps(details or {},separators=(",",":"))))

def enrich_share_telemetry(t: Telemetry) -> Telemetry:
    raw = t.raw or {}
    current = find_value(raw, [
        "current_share", "currentshare", "last_share_difficulty", "lastsharedifficulty",
        "last_share_diff", "lastsharediff", "last_diff", "lastdiff", "current_diff", "Last Share Difficulty", "Last Share Diff"
    ])
    found = find_number(raw, [
        "found_blocks", "foundblocks", "blocks_found", "blocksfound", "block_found", "blockfound", "Found Blocks"
    ])
    primary = raw.get("primary_pool") if isinstance(raw, dict) else None
    if not isinstance(primary, dict):
        primary = {}
    pool_url = primary.get("displayURL") or primary.get("URL") or primary.get("url") or primary.get("stratumURL") or raw.get("stratumURL")
    pool_user = primary.get("User") or primary.get("user") or primary.get("stratumUser") or raw.get("stratumUser")
    if current not in (None, "") and not raw.get("ignore_reported_current_share"):
        t.current_share = str(current)
    t.found_blocks = max(0, int(found)) if found is not None else None
    t.pool_key = f"{pool_url or ''}|{pool_user or ''}" if pool_url or pool_user else None
    return t

async def reconcile_block_claim(miner: sqlite3.Row, t: Telemetry):
    if t.found_blocks is None:
        return
    now = int(time.time())
    with db() as c:
        row = c.execute("SELECT * FROM block_claims WHERE miner_id=?", (miner["id"],)).fetchone()
        old_count = int(row["found_count"]) if row else 0
        old_pool = str(row["pool_key"] or "") if row else ""
        new_pool = t.pool_key or old_pool
        pool_changed = bool(row and old_pool and t.pool_key and old_pool != t.pool_key)
        reset = bool(row and t.found_blocks < old_count)
        is_new = t.found_blocks > 0 and (row is None or (not pool_changed and not reset and t.found_blocks > old_count))
        if pool_changed or reset or t.found_blocks == 0:
            active = 0
            found_at = None
        elif is_new:
            active = 1
            found_at = now
        else:
            active = int(row["active"]) if row else 0
            found_at = row["found_at"] if row else None
        c.execute("""INSERT INTO block_claims(miner_id,found_count,pool_key,found_at,active)
                     VALUES(?,?,?,?,?) ON CONFLICT(miner_id) DO UPDATE SET
                     found_count=excluded.found_count,pool_key=excluded.pool_key,
                     found_at=excluded.found_at,active=excluded.active""",
                  (miner["id"], t.found_blocks, new_pool, found_at, active))
    if is_new:
        details = {"found_count": t.found_blocks, "pool_key": new_pool, "algorithm": miner["algorithm"]}
        record_event(miner["id"], "block_found", value_text=str(t.found_blocks), details=details)
        await manager.broadcast({"type":"block_found","miner_id":miner["id"],"miner_name":miner["name"],"algorithm":miner["algorithm"],"found_count":t.found_blocks,"ts":now})

def ensure_session_baseline(miner_id:int, accepted:int|None, rejected:int|None):
    with db() as c:
        if c.execute("SELECT 1 FROM session_baselines WHERE miner_id=?",(miner_id,)).fetchone() is None:
            c.execute("INSERT INTO session_baselines(miner_id,accepted,rejected,started_at) VALUES(?,?,?,?)",(miner_id,accepted,rejected,int(time.time())))

def today_start_epoch()->int:
    lt=time.localtime(); return int(time.mktime((lt.tm_year,lt.tm_mon,lt.tm_mday,0,0,0,lt.tm_wday,lt.tm_yday,lt.tm_isdst)))

def derive_today_shares(miner_id:int,current_accepted:int|None)->int|None:
    if current_accepted is None:return None
    with db() as c: row=c.execute("SELECT accepted FROM samples WHERE miner_id=? AND ts>=? AND accepted IS NOT NULL ORDER BY ts ASC LIMIT 1",(miner_id,today_start_epoch())).fetchone()
    return 0 if row is None or row["accepted"] is None else max(0,int(current_accepted)-int(row["accepted"]))

def derive_session_shares(miner_id:int,current_accepted:int|None)->int|None:
    if current_accepted is None:return None
    with db() as c: row=c.execute("SELECT accepted FROM session_baselines WHERE miner_id=?",(miner_id,)).fetchone()
    return 0 if row is None or row["accepted"] is None else max(0,int(current_accepted)-int(row["accepted"]))


async def collector():
    previous: dict[int, Telemetry] = {}
    while True:
        try:
            with db() as c:
                miners = list(c.execute("SELECT * FROM miners ORDER BY id"))
            for miner in miners:
                t = enrich_share_telemetry(await poll_miner(miner))
                ts = int(time.time())
                with db() as c:
                    c.execute("""INSERT INTO samples(miner_id,ts,hashrate,hashrate_unit,temp_c,power_w,accepted,rejected,best_share,online)
                                 VALUES(?,?,?,?,?,?,?,?,?,?)""",
                              (miner["id"], ts, t.hashrate, t.hashrate_unit, t.temp_c, t.power_w,
                               t.accepted, t.rejected, t.best_share, 1 if t.online else 0))
                    # keep 30 days at 10-second polling by pruning old samples
                    c.execute("DELETE FROM samples WHERE ts < ?", (ts - 30*86400,))

                old = previous.get(miner["id"])
                if old and old.accepted is not None and t.accepted is not None and t.accepted > old.accepted:
                    delta = min(t.accepted - old.accepted, 25)
                    await manager.broadcast({"type":"share","miner_id":miner["id"],"miner_name":miner["name"],"algorithm":miner["algorithm"],"count":delta})
                if old and old.best_share and t.best_share and t.best_share != old.best_share:
                    await manager.broadcast({"type":"best_share","miner_id":miner["id"],"miner_name":miner["name"],"value":t.best_share})
                await reconcile_block_claim(miner, t)
                pool_url, _pool_user = _telemetry_pool_identity(t)
                if pool_url and "solopool.org" not in pool_url.lower():
                    with db() as c: c.execute("UPDATE pool_block_claims SET active=0 WHERE miner_id=?", (miner["id"],))
                previous[miner["id"]] = t
                latest_telemetry[miner["id"]] = t
                await manager.broadcast({"type":"telemetry","miner_id":miner["id"]})
        except Exception:
            pass
        await asyncio.sleep(POLL_SECONDS)


@app.on_event("startup")
async def startup():
    init_db()
    asyncio.create_task(collector())
    asyncio.create_task(block_watcher())
    asyncio.create_task(network_watcher())
    asyncio.create_task(bch_watcher())
    asyncio.create_task(price_watcher())
    asyncio.create_task(solopool_watcher())


@app.get("/", response_class=HTMLResponse)
async def home():
    return INDEX_HTML


@app.get("/assets/mining-room.webp")
async def mining_room_background():
    return FileResponse(Path(__file__).parent / "assets" / "mining-room.webp", media_type="image/webp")

@app.get("/assets/nerd-console-grid.svg")
async def nerd_console_background():
    return FileResponse(Path(__file__).parent / "assets" / "nerd-console-grid.svg", media_type="image/svg+xml")


@app.get("/assets/liquid-data-center.webp")
async def liquid_data_center_background():
    return FileResponse(Path(__file__).parent / "assets" / "liquid-data-center.webp", media_type="image/webp")


@app.get("/favicon.svg")
async def favicon():
    return Response(content=FAVICON_SVG, media_type="image/svg+xml")


@app.websocket("/ws")
async def ws_endpoint(ws: WebSocket):
    await manager.connect(ws)
    try:
        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(ws)


@app.get("/api/settings")
def settings_get():
    return get_settings()


@app.put("/api/settings")
def settings_put(body: SettingsIn):
    with db() as c:
        c.execute("INSERT OR REPLACE INTO settings(key,value) VALUES('share_emoji',?)", (body.share_emoji,))
        c.execute("INSERT OR REPLACE INTO settings(key,value) VALUES('share_emoji_sha256',?)", (body.share_emoji_sha256 or body.share_emoji,))
        c.execute("INSERT OR REPLACE INTO settings(key,value) VALUES('share_emoji_blake3',?)", (body.share_emoji_blake3 or body.share_emoji,))
        c.execute("INSERT OR REPLACE INTO settings(key,value) VALUES('share_emoji_default',?)", (body.share_emoji_default or body.share_emoji,))
        c.execute("INSERT OR REPLACE INTO settings(key,value) VALUES('animation_density',?)", (str(body.animation_density),))
        c.execute("INSERT OR REPLACE INTO settings(key,value) VALUES('celebrate_rejected',?)", ("true" if body.celebrate_rejected else "false",))
    return get_settings()


@app.get("/api/miners")
def miners_get():
    with db() as c:
        miners = [dict(r) for r in c.execute("SELECT * FROM miners ORDER BY id")]
        out = []
        for m in miners:
            s = c.execute("SELECT * FROM samples WHERE miner_id=? ORDER BY ts DESC LIMIT 1",(m["id"],)).fetchone()
            rich = latest_telemetry.get(m["id"])
            if rich is not None:
                m["telemetry"] = asdict(rich)
            else:
                m["telemetry"] = dict(s) if s else None
            ca=(m["telemetry"] or {}).get("accepted"); cr=(m["telemetry"] or {}).get("rejected")
            m["shares_today"]=derive_today_shares(m["id"],ca); m["shares_session"]=derive_session_shares(m["id"],ca); m["shares_lifetime"]=ca
            m["reject_pct"]=(cr/(ca+cr)*100.0) if ca is not None and cr is not None and (ca+cr)>0 else None
            claim = c.execute("SELECT found_count,found_at,active FROM block_claims WHERE miner_id=?", (m["id"],)).fetchone()
            pool_claim = c.execute("SELECT found_at,active,block_height,block_hash FROM pool_block_claims WHERE miner_id=?", (m["id"],)).fetchone()
            m["block_found"] = bool((claim and claim["active"]) or (pool_claim and pool_claim["active"]))
            m["block_found_at"] = pool_claim["found_at"] if pool_claim and pool_claim["active"] else (claim["found_at"] if claim else None)
            m["block_found_height"] = pool_claim["block_height"] if pool_claim else None
            m["found_blocks"] = claim["found_count"] if claim else (m["telemetry"] or {}).get("found_blocks")
            out.append(m)
    return out


@app.post("/api/miners")
def miners_add(body: MinerIn):
    with db() as c:
        try:
            cur = c.execute("""INSERT INTO miners(name,ip,model,family,algorithm,created_at)
                             VALUES(?,?,?,?,?,?)""",
                            (body.name, clean_ip(body.ip), body.model, body.family, body.algorithm, int(time.time())))
            return {"id":cur.lastrowid, **body.model_dump()}
        except sqlite3.IntegrityError:
            raise HTTPException(409, "A miner with that IP already exists")



@app.put("/api/miners/{miner_id}")
def miners_update(miner_id: int, body: MinerIn):
    with db() as c:
        exists = c.execute("SELECT id FROM miners WHERE id=?", (miner_id,)).fetchone()
        if not exists:
            raise HTTPException(404, "Miner not found")
        try:
            c.execute("""UPDATE miners SET name=?, ip=?, model=?, family=?, algorithm=? WHERE id=?""",
                      (body.name, clean_ip(body.ip), body.model, body.family, body.algorithm, miner_id))
        except sqlite3.IntegrityError:
            raise HTTPException(409, "A miner with that IP already exists")
    return {"id": miner_id, **body.model_dump()}


@app.delete("/api/miners/{miner_id}")
def miners_delete(miner_id: int):
    with db() as c:
        c.execute("DELETE FROM samples WHERE miner_id=?", (miner_id,))
        c.execute("DELETE FROM miners WHERE id=?", (miner_id,))
    return {"ok":True}


@app.post("/api/miners/{miner_id}/probe")
async def miners_probe(miner_id: int):
    with db() as c:
        miner = c.execute("SELECT * FROM miners WHERE id=?", (miner_id,)).fetchone()
    if not miner:
        raise HTTPException(404, "Miner not found")
    return asdict(await poll_miner(miner))




@app.get("/api/miners/{miner_id}/iceriver-post4-diagnostic")
async def iceriver_post4_diagnostic(miner_id: int):
    with db() as c:
        miner = c.execute("SELECT * FROM miners WHERE id=?", (miner_id,)).fetchone()
    if not miner:
        raise HTTPException(404, "Miner not found")
    if miner["family"] != "iceriver":
        raise HTTPException(400, "This diagnostic is only for IceRiver miners")

    telemetry, diag = await iceriver_post4_request(miner["ip"])
    return {
        "miner": {
            "id": miner["id"],
            "name": miner["name"],
            "ip": clean_ip(miner["ip"]),
            "family": miner["family"],
            "algorithm": miner["algorithm"],
        },
        "state": {
            "reachable": telemetry.reachable,
            "authenticated": telemetry.authenticated,
            "telemetry_available": telemetry.telemetry_available,
            "online": telemetry.online,
            "error": telemetry.error,
        },
        "mapped": {
            "hashrate": telemetry.hashrate,
            "hashrate_unit": telemetry.hashrate_unit,
            "avg_hashrate": telemetry.avg_hashrate,
            "avg_hashrate_unit": telemetry.avg_hashrate_unit,
            "temp_c": telemetry.temp_c,
            "power_w": telemetry.power_w,
            "efficiency": telemetry.efficiency,
            "efficiency_unit": telemetry.efficiency_unit,
            "accepted": telemetry.accepted,
            "rejected": telemetry.rejected,
            "best_share": telemetry.best_share,
            "pool_alive": telemetry.pool_alive,
            "uptime_s": telemetry.uptime_s,
            "fan_rpm": telemetry.fan_rpm,
        },
        "diagnostic": sanitize_iceriver_json(diag),
        "raw_preserved": sanitize_iceriver_json((telemetry.raw or {}).get("all_unmapped_data", {})),
    }


@app.get("/api/miners/{miner_id}/diagnostics")
async def miner_diagnostics(miner_id: int):
    with db() as c:
        miner = c.execute("SELECT * FROM miners WHERE id=?", (miner_id,)).fetchone()
    if not miner:
        raise HTTPException(404, "Miner not found")

    ip = clean_ip(miner["ip"])
    results: list[dict[str, Any]] = []

    # Common read-only miner endpoints. We record status/content type and a short
    # response preview, but never submit settings or credentials.
    paths = [
        "/", "/api/system/info", "/api/system/scoreboard", "/api/system", "/api/info",
        "/api/user/overview", "/api/overview", "/api/status",
        "/api/v1/status", "/api/v1/system", "/api/v1/summary",
        "/mcb/cgminer?cgminercmd=summary",
        "/mcb/cgminer?cgminercmd=devs",
        "/mcb/cgminer?cgminercmd=pools",
    ]

    async with httpx.AsyncClient(timeout=httpx.Timeout(1.2, connect=0.8), follow_redirects=False) as client:
        async def fetch_one(scheme: str, path: str):
            url = f"{scheme}://{ip}{path}"
            try:
                r = await client.get(url)
                preview = r.text[:500].replace("\n", " ").replace("\r", " ")
                return {
                    "kind": "http",
                    "url": url,
                    "status": r.status_code,
                    "content_type": r.headers.get("content-type",""),
                    "server": r.headers.get("server",""),
                    "location": r.headers.get("location",""),
                    "preview": preview,
                }
            except Exception as e:
                return {
                    "kind": "http",
                    "url": url,
                    "error": type(e).__name__ + ": " + str(e)[:180],
                }

        http_results = await asyncio.gather(*[
            fetch_one(scheme, path)
            for scheme in ("http", "https")
            for path in paths
        ])
        results.extend(http_results)

    # Read-only CGMiner API test on TCP 4028.
    loop = asyncio.get_running_loop()
    diag_commands = ["summary", "stats", "pools", "devs", "version"]
    if miner["family"] == "luxos":
        diag_commands.append("power")
    for cmd in diag_commands:
        try:
            data = await loop.run_in_executor(None, cgminer_request, ip, cmd)
            results.append({
                "kind": "cgminer",
                "command": cmd,
                "port": 4028,
                "responded": data is not None,
                "preview": json.dumps(data, default=str)[:1200] if data is not None else "",
            })
        except Exception as e:
            results.append({
                "kind": "cgminer",
                "command": cmd,
                "port": 4028,
                "responded": False,
                "error": type(e).__name__ + ": " + str(e)[:180],
            })

    discovered_endpoints = []
    js_hints = []
    endpoint_results = []
    if miner["family"] == "iceriver":
        try:
            async with httpx.AsyncClient(timeout=2.0, follow_redirects=True) as client:
                home = await client.get(f"http://{ip}/")
                scripts = re.findall(r"<script[^>]+src=[\"']([^\"']+)[\"']", home.text, flags=re.I)
                for script in scripts[:20]:
                    if not script.startswith("/"):
                        script = "/" + script.lstrip("/")
                    try:
                        jr = await client.get(f"http://{ip}{script}")
                        if jr.status_code < 400:
                            candidates = re.findall(r"[\"']((?:/|\./)[A-Za-z0-9_./?=&-]{3,120})[\"']", jr.text)
                            for c in candidates:
                                lc = c.lower()
                                if any(tok in lc for tok in ("api", "miner", "machine", "status", "hash", "pool", "user")):
                                    discovered_endpoints.append(c)
                    except Exception:
                        pass
        except Exception:
            pass
        discovered_endpoints = sorted(set(discovered_endpoints))[:100]

        # Capture small JavaScript snippets that reveal how the stock UI asks for live data.
        try:
            async with httpx.AsyncClient(timeout=2.5, follow_redirects=True) as client:
                home = await client.get(f"http://{ip}/")
                scripts = re.findall(r"<script[^>]+src=[\"']([^\"']+)[\"']", home.text, flags=re.I)
                for script in scripts[:30]:
                    if not script.startswith("/"):
                        script = "/" + script.lstrip("/")
                    try:
                        jr = await client.get(f"http://{ip}{script}")
                        if jr.status_code >= 400:
                            continue
                        body = jr.text
                        # Search for likely live-data mechanics without dumping entire JS files.
                        patterns = [
                            r"\$\.ajax\s*\(",
                            r"\$\.get\s*\(",
                            r"\$\.post\s*\(",
                            r"fetch\s*\(",
                            r"XMLHttpRequest",
                            r"/user/userpanel",
                            r"/user/machine",
                            r"hashrate",
                            r"hash_rate",
                            r"temperature",
                            r"pool",
                            r"miner",
                        ]
                        spans = []
                        for pat in patterns:
                            for m in re.finditer(pat, body, flags=re.I):
                                start = max(0, m.start() - 450)
                                end = min(len(body), m.end() + 900)
                                spans.append((start, end))
                        # Merge overlapping spans and keep output bounded.
                        spans.sort()
                        merged = []
                        for s, e in spans:
                            if not merged or s > merged[-1][1] + 50:
                                merged.append([s, e])
                            else:
                                merged[-1][1] = max(merged[-1][1], e)
                        for s, e in merged[:10]:
                            snippet = body[s:e].replace("\r", " ").replace("\n", " ")
                            js_hints.append({
                                "script": script,
                                "snippet": snippet[:3000]
                            })
                    except Exception:
                        pass
        except Exception:
            pass
        js_hints = js_hints[:40]

        # Probe the mining-relevant discovered endpoints directly.
        interesting = [
            p for p in discovered_endpoints
            if p.split("?",1)[0] in (
                "/user/machine", "/user/userpanel", "/user/system", "/user/ip"
            )
        ]
        try:
            async with httpx.AsyncClient(timeout=2.0, follow_redirects=False) as client:
                for path in interesting[:12]:
                    try:
                        rr = await client.get(f"http://{ip}{path}")
                        endpoint_results.append({
                            "url": f"http://{ip}{path}",
                            "status": rr.status_code,
                            "content_type": rr.headers.get("content-type",""),
                            "preview": rr.text[:4000].replace("\r"," ").replace("\n"," ")
                        })
                    except Exception as e:
                        endpoint_results.append({
                            "url": f"http://{ip}{path}",
                            "error": type(e).__name__ + ": " + str(e)[:180]
                        })
        except Exception:
            endpoint_results = []
    useful = [
        r for r in results
        if (r.get("kind") == "http" and r.get("status") in (200, 401, 403))
        or (r.get("kind") == "cgminer" and r.get("responded"))
    ]
    return {
        "miner": {
            "id": miner["id"], "name": miner["name"], "ip": miner["ip"],
            "family": miner["family"], "algorithm": miner["algorithm"]
        },
        "useful_count": len(useful),
        "useful": useful,
        "all_results": results,
        "discovered_endpoints": discovered_endpoints,
        "endpoint_results": endpoint_results,
        "js_hints": js_hints,
    }




DEFAULT_ALERTS = {
    "enabled": True,
    "offline_seconds": 60,
    "hashrate_drop_pct": 30,
    "temperature_c": 75,
    "reject_pct": 2.0,
    "pool_disconnect": True
}

def get_alert_settings():
    with db() as c:
        row = c.execute("SELECT value FROM settings WHERE key='alert_settings'").fetchone()
    if not row:
        return DEFAULT_ALERTS.copy()
    try:
        data = json.loads(row["value"])
        merged = DEFAULT_ALERTS.copy()
        merged.update(data or {})
        return merged
    except Exception:
        return DEFAULT_ALERTS.copy()

def save_alert_settings(data: dict[str, Any]):
    clean = DEFAULT_ALERTS.copy()
    for k in clean:
        if k in data:
            clean[k] = data[k]
    with db() as c:
        c.execute(
            """INSERT INTO settings(key,value) VALUES('alert_settings',?)
               ON CONFLICT(key) DO UPDATE SET value=excluded.value""",
            (json.dumps(clean, separators=(",", ":")),)
        )
    return clean

@app.get("/api/alerts/settings")
def alerts_settings_get():
    return get_alert_settings()

@app.put("/api/alerts/settings")
def alerts_settings_put(body: AlertSettingsIn):
    return save_alert_settings(body.model_dump())

@app.get("/api/health")
def health():
    miners = miners_get()
    alerts = get_alert_settings()
    now = int(time.time())
    issues = []
    healthy = 0
    unknown = 0

    for m in miners:
        t = m.get("telemetry") or {}
        miner_issues = []

        if not t:
            miner_issues.append({"type":"no_telemetry","severity":"critical","message":"No telemetry"})
        else:
            if t.get("online") is False:
                miner_issues.append({"type":"offline","severity":"critical","message":"Miner offline"})
            if alerts.get("pool_disconnect") and t.get("pool_alive") is False:
                miner_issues.append({"type":"pool_down","severity":"warning","message":"Pool disconnected"})
            if t.get("temp_c") is not None and float(t["temp_c"]) >= float(alerts.get("temperature_c", 75)):
                miner_issues.append({
                    "type":"temperature","severity":"warning",
                    "message":f'Temperature {float(t["temp_c"]):.0f}°C ≥ {float(alerts.get("temperature_c", 75)):.0f}°C'
                })
            if m.get("reject_pct") is not None and float(m["reject_pct"]) >= float(alerts.get("reject_pct", 2.0)):
                miner_issues.append({
                    "type":"reject_rate","severity":"warning",
                    "message":f'Reject rate {float(m["reject_pct"]):.2f}%'
                })

            # Compare with recent historical median-ish baseline for hashrate drop.
            if t.get("hashrate") is not None:
                cutoff = now - 3600
                with db() as c:
                    rows = c.execute(
                        """SELECT hashrate FROM samples
                           WHERE miner_id=? AND ts>=? AND hashrate IS NOT NULL AND online=1
                           ORDER BY ts""",
                        (m["id"], cutoff)
                    ).fetchall()
                vals = [float(r["hashrate"]) for r in rows if r["hashrate"] is not None and float(r["hashrate"]) > 0]
                if len(vals) >= 6:
                    vals_sorted = sorted(vals)
                    baseline = vals_sorted[len(vals_sorted)//2]
                    drop_pct = float(alerts.get("hashrate_drop_pct", 30))
                    if baseline > 0 and float(t["hashrate"]) < baseline * (1.0 - drop_pct/100.0):
                        miner_issues.append({
                            "type":"hashrate_drop","severity":"warning",
                            "message":f'Hashrate {((baseline-float(t["hashrate"]))/baseline)*100:.0f}% below 1h median'
                        })

        if miner_issues:
            issues.append({
                "miner_id": m["id"], "miner_name": m["name"], "algorithm": m.get("algorithm"),
                "ip": m.get("ip"), "issues": miner_issues
            })
        else:
            if t:
                healthy += 1
            else:
                unknown += 1

    status = "healthy" if not issues else ("critical" if any(
        i["severity"] == "critical" for x in issues for i in x["issues"]
    ) else "warning")
    return {
        "status": status,
        "healthy": healthy,
        "issues_count": len(issues),
        "unknown": unknown,
        "total": len(miners),
        "issues": issues,
        "settings": alerts
    }

@app.get("/api/export/config")
def export_config():
    miners = miners_get()
    settings = {}
    with db() as c:
        for r in c.execute("SELECT key,value FROM settings").fetchall():
            settings[r["key"]] = r["value"]
    # Strip live telemetry; export only durable config.
    clean_miners = []
    for m in miners:
        clean_miners.append({
            "name": m.get("name"),
            "ip": m.get("ip"),
            "family": m.get("family"),
            "model": m.get("model"),
            "algorithm": m.get("algorithm"),
            "enabled": m.get("enabled", 1),
        })
    return {
        "version": "0.2.3",
        "exported_at": int(time.time()),
        "miners": clean_miners,
        "settings": settings
    }


@app.get("/api/customization")
def customization_get():
    return get_customization()


@app.put("/api/customization")
def customization_put(body: CustomizationIn):
    return save_customization(body)


@app.get("/api/block-status")
def block_status():
    return latest_block

@app.get("/api/chain-status")
def chain_status():
    cfg = get_customization()
    return {
        "btc": latest_block,
        "bch": latest_bch,
        "wallets": latest_wallets,
        "prices": latest_prices,
        "solo_chances": {
            "btc": _solo_chance(cfg.get("btc_solo_hashrate", 0), cfg.get("btc_solo_hashrate_unit", "TH"), latest_block.get("difficulty")),
            "bch": _solo_chance(cfg.get("bch_solo_hashrate", 0), cfg.get("bch_solo_hashrate_unit", "TH"), latest_bch.get("difficulty")),
        },
        "solopool": latest_solopool,
    }


def _solo_chance(value: float, unit: str, difficulty: Any) -> dict[str, Any] | None:
    try:
        value = float(value); difficulty = float(difficulty)
    except (TypeError, ValueError):
        return None
    if value <= 0 or difficulty <= 0:
        return None
    multipliers = {"TH": 1e12, "PH": 1e15}
    unit = str(unit or "TH").upper()
    hashrate_hs = value * multipliers.get(unit, 1e12)
    expected_seconds = difficulty * (2 ** 32) / hashrate_hs
    def chance(seconds: int) -> float:
        return (1 - math.exp(-seconds / expected_seconds)) * 100
    return {
        "hashrate": value, "unit": unit, "expected_seconds": expected_seconds,
        "chance_24h_pct": chance(86400), "chance_7d_pct": chance(7 * 86400),
        "chance_30d_pct": chance(30 * 86400), "chance_365d_pct": chance(365 * 86400),
    }

async def refresh_wallet_balances():
    global latest_wallets
    cfg = get_customization(); wallets = {"btc": None, "bch": None, "updated_at": int(time.time()), "errors": {}}
    async with httpx.AsyncClient(timeout=httpx.Timeout(10.0, connect=4.0)) as client:
        btc_address = cfg.get("btc_wallet_address", "")
        if btc_address:
            try:
                rb = await client.get(cfg.get("block_api_base", "https://mempool.space/api").rstrip("/") + "/address/" + btc_address)
                rb.raise_for_status(); a = rb.json(); chain = a.get("chain_stats") or {}; mem = a.get("mempool_stats") or {}
                sats = (chain.get("funded_txo_sum",0)-chain.get("spent_txo_sum",0)) + (mem.get("funded_txo_sum",0)-mem.get("spent_txo_sum",0))
                wallets["btc"] = {"address": btc_address, "balance": sats / 100_000_000, "unit": "BTC"}
            except Exception as e: wallets["errors"]["btc"] = str(e)
        bch_address = cfg.get("bch_wallet_address", "")
        if bch_address:
            try:
                normalized = bch_address if ":" in bch_address else "bitcoincash:" + bch_address
                rw = await client.get(
                    "https://blockbook.bch.zelcore.io/api/v2/address/"
                    + quote(normalized, safe="")
                    + "?details=basic"
                )
                rw.raise_for_status(); entry = rw.json(); sats = entry.get("balance")
                wallets["bch"] = {"address": bch_address, "balance": (int(sats)/100_000_000) if sats is not None else None, "unit": "BCH"}
            except Exception as e: wallets["errors"]["bch"] = str(e)
    latest_wallets = wallets
    return wallets

@app.post("/api/wallets/refresh")
async def wallets_refresh():
    return await refresh_wallet_balances()



def _safe_float(v):
    try:
        return float(v)
    except Exception:
        return None


def _best_share_number(v):
    if v is None:
        return None
    s = str(v).strip().replace(",", "")
    m = re.match(r"^(-?\d+(?:\.\d+)?)\s*([kKmMgGtTpPeE]?)$", s)
    if not m:
        try:
            return float(s)
        except Exception:
            return None
    n = float(m.group(1))
    suffix = m.group(2).upper()
    mult = {"":1, "K":1e3, "M":1e6, "G":1e9, "T":1e12, "P":1e15, "E":1e18}.get(suffix, 1)
    return n * mult


@app.get("/api/network-status")
def network_status():
    current_diff = _safe_float(latest_block.get("difficulty"))
    miners = miners_get()
    leaderboard = []
    fleet_best = None

    for m in miners:
        t = m.get("telemetry") or {}
        bnum = _best_share_number(t.get("best_share"))
        if bnum is None:
            continue
        progress = (bnum / current_diff * 100.0) if current_diff and current_diff > 0 else None
        one_in = (current_diff / bnum) if current_diff and bnum > 0 else None
        row = {
            "miner_id": m.get("id"),
            "miner_name": m.get("name"),
            "algorithm": m.get("algorithm"),
            "best_share": t.get("best_share"),
            "best_share_num": bnum,
            "difficulty_progress_pct": progress,
            "one_in": one_in,
        }
        leaderboard.append(row)
        if fleet_best is None or bnum > fleet_best["best_share_num"]:
            fleet_best = row

    leaderboard.sort(key=lambda x: x["best_share_num"], reverse=True)
    return {
        "block": latest_block,
        "network": latest_network,
        "difficulty": current_diff,
        "fleet_best": fleet_best,
        "leaderboard": leaderboard[:20],
    }


async def network_watcher():
    global latest_network
    while True:
        try:
            cfg = get_customization()
            base = cfg.get("block_api_base", "https://mempool.space/api").rstrip("/")
            async with httpx.AsyncClient(timeout=httpx.Timeout(8.0, connect=3.0)) as client:
                rm = await client.get(base + "/mempool")
                rf = await client.get(base + "/v1/fees/recommended")
                rm.raise_for_status()
                rf.raise_for_status()
                mem = rm.json()
                fees = rf.json()
            latest_network = {
                "available": True,
                "source": base,
                "mempool_count": mem.get("count"),
                "mempool_vsize": mem.get("vsize"),
                "mempool_total_fee": mem.get("total_fee"),
                "fastest_fee": fees.get("fastestFee"),
                "half_hour_fee": fees.get("halfHourFee"),
                "hour_fee": fees.get("hourFee"),
                "economy_fee": fees.get("economyFee"),
                "minimum_fee": fees.get("minimumFee"),
                "updated_at": int(time.time()),
            }
        except Exception as e:
            if latest_network.get("available"):
                latest_network = {**latest_network, "error": str(e), "updated_at": int(time.time())}
            else:
                latest_network = {
                    "available": False,
                    "source": get_customization().get("block_api_base", "https://mempool.space/api"),
                    "error": str(e),
                    "updated_at": int(time.time()),
                }
        await asyncio.sleep(30)

async def price_watcher():
    global latest_prices
    coin_ids = {"btc": "bitcoin", "bch": "bitcoin-cash", "alph": "alephium"}
    while True:
        try:
            async with httpx.AsyncClient(timeout=httpx.Timeout(10.0, connect=4.0)) as client:
                response = await client.get(
                    "https://api.coingecko.com/api/v3/simple/price",
                    params={
                        "ids": ",".join(coin_ids.values()),
                        "vs_currencies": "usd",
                        "include_24hr_change": "true",
                        "include_last_updated_at": "true",
                    },
                    headers={"accept": "application/json", "user-agent": "RigPulse/0.5.4"},
                )
                response.raise_for_status(); data = response.json()
            prices: dict[str, Any] = {"updated_at": int(time.time()), "source": "CoinGecko", "available": True}
            for symbol, coin_id in coin_ids.items():
                entry = data.get(coin_id) or {}
                prices[symbol] = {
                    "usd": entry.get("usd"), "change_24h": entry.get("usd_24h_change"),
                    "last_updated_at": entry.get("last_updated_at"),
                }
            latest_prices = prices
        except Exception as e:
            latest_prices = {**latest_prices, "available": any(latest_prices.get(k) for k in coin_ids), "error": str(e), "updated_at": int(time.time())}
        await asyncio.sleep(60)

async def bch_watcher():
    global latest_bch, latest_wallets
    while True:
        try:
            cfg = get_customization()
            async with httpx.AsyncClient(timeout=httpx.Timeout(10.0, connect=4.0)) as client:
                rs = await client.get("https://api.blockchair.com/bitcoin-cash/stats")
                rs.raise_for_status()
                stats = rs.json().get("data") or {}
                latest_bch = {
                    "available": True, "source": "api.blockchair.com",
                    "height": stats.get("blocks"), "hash": stats.get("best_block_hash"),
                    "timestamp": stats.get("best_block_time"), "difficulty": stats.get("difficulty"),
                    "transactions_24h": stats.get("transactions_24h"),
                    "mempool_transactions": stats.get("mempool_transactions"),
                    "updated_at": int(time.time()),
                }
            await refresh_wallet_balances()
        except Exception as e:
            if not latest_bch.get("available"):
                latest_bch = {"available": False, "source": "api.blockchair.com", "error": str(e), "updated_at": int(time.time())}
            latest_wallets = {**latest_wallets, "error": str(e), "updated_at": int(time.time())}
        await asyncio.sleep(60)

def _telemetry_pool_identity(t: Telemetry | None) -> tuple[str, str]:
    raw = (t.raw or {}) if t else {}
    primary = raw.get("primary_pool") if isinstance(raw, dict) else None
    if not isinstance(primary, dict): primary = {}
    url = str(primary.get("displayURL") or primary.get("URL") or primary.get("url") or primary.get("stratumURL") or raw.get("stratumURL") or "")
    user = str(primary.get("User") or primary.get("user") or primary.get("stratumUser") or raw.get("stratumUser") or "")
    return url, user

def _solopool_address(value: str) -> str:
    address = (value or "").strip().split(".", 1)[0]
    return re.sub(r"^(?:bitcoin|bitcoincash):", "", address, flags=re.I)

async def solopool_watcher():
    global latest_solopool
    while True:
        try:
            cfg = get_customization()
            addresses: dict[str, set[str]] = {"btc": set(), "bch": set()}
            for coin in ("btc", "bch"):
                configured = _solopool_address(cfg.get(f"{coin}_solopool_address", ""))
                if configured: addresses[coin].add(configured)
            with db() as c: miner_rows = list(c.execute("SELECT * FROM miners ORDER BY id"))
            for miner in miner_rows:
                url, user = _telemetry_pool_identity(latest_telemetry.get(miner["id"]))
                url_lower = url.lower()
                if "solopool.org" in url_lower and user:
                    coin = "bch" if "bch" in url_lower else "btc" if "btc" in url_lower else None
                    if coin and not addresses[coin]: addresses[coin].add(_solopool_address(user))
            statuses: dict[str, Any] = {"btc": None, "bch": None, "updated_at": int(time.time()), "errors": {}}
            async with httpx.AsyncClient(timeout=httpx.Timeout(10.0, connect=4.0)) as client:
                for coin, coin_addresses in addresses.items():
                    for address in coin_addresses:
                        try:
                            response = await client.get(f"https://{coin}.solopool.org/api/miners/{address}")
                            response.raise_for_status(); data = response.json()
                            if not isinstance(data, dict) or not data: continue
                            statuses[coin] = {
                                "address": address, "hashrate": data.get("hashrate"),
                                "average_hashrate": data.get("averageHashrate"),
                                "online_workers": data.get("onlineWorkers"), "offline_workers": data.get("offlineWorkers"),
                                "total_workers": data.get("totalWorkers"), "last_share": data.get("lastShare"),
                                "round_shares": data.get("roundShares"), "best_share": data.get("bestShare"),
                                "total_blocks": data.get("totalBlocks"), "last_block_time": data.get("lastBlockTime"),
                                "workers": [
                                    {"name": name, **worker}
                                    for name, worker in (data.get("workers") or {}).items()
                                    if isinstance(worker, dict)
                                ],
                                "source": f"{coin}.solopool.org",
                            }
                            blocks = data.get("blocks") or []
                            if not blocks: continue
                            block = max(blocks, key=lambda b: int(b.get("timestamp") or 0)); worker = str(block.get("worker") or "").strip()
                            winner = None
                            for miner in miner_rows:
                                _url, pool_user = _telemetry_pool_identity(latest_telemetry.get(miner["id"]))
                                worker_from_user = pool_user.rsplit(".",1)[-1] if "." in pool_user else ""
                                if worker and (str(miner["name"]).lower() == worker.lower() or worker_from_user.lower() == worker.lower()):
                                    winner = miner; break
                            if winner is None: continue
                            block_hash = str(block.get("hash") or block.get("tx") or "")
                            with db() as c:
                                old = c.execute("SELECT block_hash FROM pool_block_claims WHERE miner_id=?", (winner["id"],)).fetchone()
                                is_new = not old or old["block_hash"] != block_hash
                                c.execute("""INSERT INTO pool_block_claims(miner_id,pool_address,block_hash,block_height,found_at,active)
                                             VALUES(?,?,?,?,?,1) ON CONFLICT(miner_id) DO UPDATE SET
                                             pool_address=excluded.pool_address,block_hash=excluded.block_hash,
                                             block_height=excluded.block_height,found_at=excluded.found_at,active=1""",
                                          (winner["id"], address, block_hash, block.get("height"), block.get("timestamp")))
                            if is_new:
                                details = {"source":f"{coin}.solopool.org","pool_address":address,"worker":worker,"hash":block_hash,"height":block.get("height"),"share_diff":block.get("shareDiff"),"reward":block.get("minerReward"),"algorithm":winner["algorithm"]}
                                record_event(winner["id"], "block_found", value_text=str(block.get("height") or ""), details=details)
                                await manager.broadcast({"type":"block_found","miner_id":winner["id"],"miner_name":winner["name"],"algorithm":winner["algorithm"],"worker":worker,"height":block.get("height"),"share_diff":block.get("shareDiff"),"ts":block.get("timestamp")})
                        except Exception as e:
                            statuses["errors"][coin] = str(e)
            latest_solopool = statuses
        except Exception as e:
            latest_solopool = {**latest_solopool, "error": str(e), "updated_at": int(time.time())}
        await asyncio.sleep(20)


async def block_watcher():
    global latest_block
    previous_hash = None
    while True:
        try:
            cfg = get_customization()
            base = cfg.get("block_api_base", "https://mempool.space/api").rstrip("/")
            async with httpx.AsyncClient(timeout=httpx.Timeout(8.0, connect=3.0)) as client:
                rh = await client.get(base + "/blocks/tip/height")
                rha = await client.get(base + "/blocks/tip/hash")
                rh.raise_for_status()
                rha.raise_for_status()
                height = int(rh.text.strip())
                block_hash = rha.text.strip()
                rb = await client.get(base + f"/block/{block_hash}")
                rb.raise_for_status()
                b = rb.json()

            current = {
                "available": True,
                "source": base,
                "height": height,
                "hash": block_hash,
                "timestamp": b.get("timestamp"),
                "tx_count": b.get("tx_count"),
                "size": b.get("size"),
                "weight": b.get("weight"),
                "difficulty": b.get("difficulty"),
                "previousblockhash": b.get("previousblockhash"),
                "updated_at": int(time.time()),
            }
            latest_block = current

            if previous_hash is not None and block_hash != previous_hash:
                record_event(
                    None, "new_block", value_text=str(height),
                    details={"hash": block_hash, "tx_count": b.get("tx_count"), "source": base}
                )
                await manager.broadcast({
                    "type": "new_block",
                    "height": height,
                    "hash": block_hash,
                    "tx_count": b.get("tx_count"),
                    "timestamp": b.get("timestamp"),
                })
            previous_hash = block_hash
        except Exception as e:
            # Preserve the last good block but surface freshness/error state.
            if latest_block.get("available"):
                latest_block = {**latest_block, "error": str(e), "updated_at": int(time.time())}
            else:
                latest_block = {
                    "available": False,
                    "source": get_customization().get("block_api_base", "https://mempool.space/api"),
                    "error": str(e),
                    "updated_at": int(time.time()),
                }
        await asyncio.sleep(20)


@app.get("/api/fleet-history")
def fleet_history(seconds: int = 86400, limit_per_miner: int = 300):
    seconds = max(300, min(seconds, 30 * 86400))
    limit_per_miner = max(20, min(limit_per_miner, 1000))
    cutoff = int(time.time()) - seconds
    out = []
    with db() as c:
        miners = c.execute("SELECT id,name,algorithm,family,model,ip FROM miners ORDER BY name").fetchall()
        for m in miners:
            rows = c.execute(
                """SELECT ts,hashrate,temp_c,power_w,accepted,rejected,online
                   FROM samples WHERE miner_id=? AND ts>=? ORDER BY ts""",
                (m["id"], cutoff)
            ).fetchall()
            data = [dict(r) for r in rows]
            if len(data) > limit_per_miner:
                step = max(1, len(data) // limit_per_miner)
                data = data[::step]
            out.append({"miner": dict(m), "samples": data, "latest": data[-1] if data else None})
    return {"seconds": seconds, "miners": out}


def _human_si(value: Any) -> str | None:
    if value is None or isinstance(value, bool):
        return None
    match = re.search(r"([-+]?\d+(?:\.\d+)?)\s*([kKmMgGtTpPeE]?)", str(value).replace(",", ""))
    if not match:
        return None
    n = float(match.group(1)) * {"": 1, "K": 1e3, "M": 1e6, "G": 1e9, "T": 1e12, "P": 1e15, "E": 1e18}[match.group(2).upper()]
    for suffix, scale in (("E", 1e18), ("P", 1e15), ("T", 1e12), ("G", 1e9), ("M", 1e6), ("K", 1e3)):
        if abs(n) >= scale:
            return f"{n / scale:.3g}{suffix}"
    return f"{n:g}"


def _pool_from_telemetry(m: dict[str, Any]) -> dict[str, Any]:
    t = m.get("telemetry") or {}
    raw = t.get("raw") or {}
    p = raw.get("primary_pool") if isinstance(raw, dict) else None
    if not p and isinstance(raw, dict):
        pools = raw.get("pools")
        if isinstance(pools, list):
            p = next((x for x in pools if isinstance(x, dict) and x.get("connect") in (1, "1", True)), None)
            if not p:
                p = next((x for x in pools if isinstance(x, dict)), None)
    if not p and isinstance(raw, dict) and raw.get("stratumURL"):
        p = {
            "stratumURL": raw.get("stratumURL"),
            "stratumPort": raw.get("stratumPort"),
            "stratumUser": raw.get("stratumUser"),
            "poolDifficulty": raw.get("poolDifficulty"),
            "Status": "Alive" if t.get("pool_alive") is True else None,
            "Accepted": t.get("accepted"),
            "Rejected": t.get("rejected"),
        }
        if p.get("stratumURL") and p.get("stratumPort"):
            p["displayURL"] = f"stratum+tcp://{p['stratumURL']}:{p['stratumPort']}"
    if not p and isinstance(raw, dict):
        full = raw.get("full")
        if isinstance(full, dict):
            for key in ("POOLS", "pools", "Pools"):
                cand = full.get(key)
                if isinstance(cand, list) and cand:
                    p = cand[0]
                    break
    p = p if isinstance(p, dict) else {}
    return {
        "miner_id": m.get("id"), "miner_name": m.get("name"), "algorithm": m.get("algorithm"), "ip": m.get("ip"),
        "alive": t.get("pool_alive"),
        "url": p.get("displayURL") or p.get("URL") or p.get("url") or p.get("addr") or p.get("pool") or p.get("Pool URL") or p.get("stratum") or p.get("stratumURL"),
        "user": p.get("User") or p.get("user") or p.get("worker") or p.get("username") or p.get("stratumUser"),
        "status": p.get("Status") or p.get("status") or p.get("state"),
        "accepted": p.get("Accepted") if p.get("Accepted") is not None else p.get("accepted"),
        "rejected": p.get("Rejected") if p.get("Rejected") is not None else p.get("rejected"),
        "difficulty": _human_si(p.get("poolDifficulty") or p.get("Stratum Difficulty") or p.get("Difficulty") or p.get("diff") or p.get("diff1")),
    }


@app.get("/api/pools")
def pools_get():
    pools = [_pool_from_telemetry(m) for m in miners_get()]
    return {
        "miners": pools,
        "alive": sum(1 for p in pools if p["alive"] is True),
        "down": sum(1 for p in pools if p["alive"] is False),
        "unknown": sum(1 for p in pools if p["alive"] is None),
    }

@app.get("/api/events")
def events(limit:int=100, miner_id:int|None=None):
    limit=max(1,min(limit,500))
    with db() as c:
        if miner_id is None:
            rows=c.execute("SELECT e.*,m.name AS miner_name FROM events e LEFT JOIN miners m ON m.id=e.miner_id ORDER BY e.ts DESC LIMIT ?",(limit,)).fetchall()
        else:
            rows=c.execute("SELECT e.*,m.name AS miner_name FROM events e LEFT JOIN miners m ON m.id=e.miner_id WHERE e.miner_id=? ORDER BY e.ts DESC LIMIT ?",(miner_id,limit)).fetchall()
    out=[]
    for r in rows:
        d=dict(r)
        try:d["details"]=json.loads(d.get("details") or "{}")
        except:pass
        out.append(d)
    return out

@app.get("/api/block-found/latest")
def latest_block_found_event():
    with db() as c:
        row = c.execute("""SELECT e.*,m.name AS miner_name,m.algorithm,
                           CASE WHEN COALESCE(b.active,0)=1 OR COALESCE(p.active,0)=1 THEN 1 ELSE 0 END AS active FROM events e
                           LEFT JOIN miners m ON m.id=e.miner_id
                           LEFT JOIN block_claims b ON b.miner_id=e.miner_id
                           LEFT JOIN pool_block_claims p ON p.miner_id=e.miner_id
                           WHERE e.event_type='block_found' ORDER BY e.ts DESC LIMIT 1""").fetchone()
    if not row:
        return {"available": False}
    out = dict(row)
    try: out["details"] = json.loads(out.get("details") or "{}")
    except Exception: out["details"] = {}
    out["available"] = True
    return out

@app.get("/api/fleet-summary")
def fleet_summary():
    ms=miners_get(); alg={}; kp=0.0; unknown=0; temps=[]; online=0; today=session=lifetime=0; ht=hs=hl=False; fleet_best=None

    def parse_share(v):
        if v is None: return None
        s=str(v).strip().replace(",","")
        m=re.match(r"^(-?\d+(?:\.\d+)?)\s*([kKmMgGtTpPeE]?)$", s)
        if not m:
            try: return float(s)
            except Exception: return None
        n=float(m.group(1)); suffix=m.group(2).upper()
        return n * {"":1,"K":1e3,"M":1e6,"G":1e9,"T":1e12,"P":1e15,"E":1e18}.get(suffix,1)

    def canonical_hash(value, unit, algorithm):
        if value is None:
            return None, ""
        v=float(value); u=(unit or "").upper()
        if algorithm == "SHA-256":
            if u.startswith("MH"): v /= 1_000_000.0
            elif u.startswith("GH"): v /= 1_000.0
            elif u.startswith("PH"): v *= 1_000.0
            return v, "TH/s"
        if algorithm == "Blake3":
            if u.startswith("MH"): v /= 1_000.0
            elif u.startswith("TH"): v *= 1_000.0
            return v, "GH/s"
        if algorithm == "Scrypt":
            if u.startswith("GH"): v *= 1_000.0
            elif u.startswith("TH"): v *= 1_000_000.0
            return v, "MH/s"
        return v, unit or ""

    for m in ms:
        t=m.get("telemetry") or {}; a=m.get("algorithm") or "Unknown"
        if t.get("online"): online+=1
        alg.setdefault(a,{"count":0,"online":0,"hashrate":0.0,"unit":""}); alg[a]["count"]+=1
        if t.get("online"): alg[a]["online"]+=1
        hv, hu = canonical_hash(t.get("hashrate"), t.get("hashrate_unit"), a)
        if hv is not None:
            alg[a]["hashrate"] += hv
            alg[a]["unit"] = hu
        if t.get("power_w") is not None: kp+=float(t["power_w"])
        else: unknown+=1
        if t.get("temp_c") is not None: temps.append(float(t["temp_c"]))
        bnum=parse_share(t.get("best_share"))
        if bnum is not None and (fleet_best is None or bnum > fleet_best["value"]):
            fleet_best={"value":bnum,"raw":t.get("best_share"),"miner_name":m.get("name"),"miner_id":m.get("id")}
        if m.get("shares_today") is not None: today+=int(m["shares_today"]);ht=True
        if m.get("shares_session") is not None: session+=int(m["shares_session"]);hs=True
        if m.get("shares_lifetime") is not None: lifetime+=int(m["shares_lifetime"]);hl=True
    return {"algorithms":alg,"online":online,"total":len(ms),"known_power_w":kp,"unknown_power_miners":unknown,"avg_temp_c":sum(temps)/len(temps) if temps else None,"shares_today":today if ht else None,"shares_session":session if hs else None,"shares_lifetime":lifetime if hl else None,"fleet_best":fleet_best}


@app.get("/api/miners/{miner_id}/hardware")
def miner_hardware(miner_id: int):
    miners = miners_get()
    m = next((x for x in miners if x["id"] == miner_id), None)
    if not m:
        raise HTTPException(404, "Miner not found")
    t = m.get("telemetry") or {}
    raw = t.get("raw") or {}
    family = m.get("family")
    out = {
        "miner_id": miner_id,
        "family": family,
        "model": m.get("model"),
        "ip": m.get("ip"),
        "firmware": None,
        "fans": [],
        "temperatures": [],
        "boards": [],
        "hardware_errors": None,
        "work_mode": t.get("work_mode"),
        "pool": None,
        "power_source": None,
    }

    if isinstance(raw, dict):
        p = raw.get("primary_pool")
        if isinstance(p, dict):
            out["pool"] = {
                "url": p.get("URL") or p.get("url") or p.get("addr"),
                "user": p.get("User") or p.get("user"),
                "status": p.get("Status") or p.get("status") or p.get("state"),
                "difficulty": p.get("Stratum Difficulty") or p.get("Difficulty") or p.get("diff"),
            }

        if family == "luxos":
            out["firmware"] = raw.get("luxminer")
            out["hardware_errors"] = raw.get("hardware_errors")
            out["power_source"] = raw.get("power_source")
            ms = raw.get("miner_stats") or {}
            fans = []
            for k, v in ms.items():
                if str(k).lower().startswith("fan") and isinstance(v, (int, float)):
                    fans.append({"name": str(k), "rpm": v})
            out["fans"] = fans
            temps = []
            for k, v in ms.items():
                lk = str(k).lower()
                if "temp" in lk and isinstance(v, (int, float)):
                    temps.append({"name": str(k), "c": v})
            out["temperatures"] = temps[:24]
            full = raw.get("full") or {}
            devs = full.get("devs") or full.get("DEVS") or []
            if isinstance(devs, list):
                out["boards"] = devs[:8]

        elif family == "avalon":
            mm = raw.get("avalon_mm") or {}
            out["fans"] = [{"name":"Fan", "rpm": mm.get("fan_rpm")}] if mm.get("fan_rpm") is not None else []
            if mm.get("tmax") is not None: out["temperatures"].append({"name":"Max", "c": mm.get("tmax")})
            if mm.get("tavg") is not None: out["temperatures"].append({"name":"Average", "c": mm.get("tavg")})
            if mm.get("otemp") is not None: out["temperatures"].append({"name":"Outlet", "c": mm.get("otemp")})

        elif family == "axeos":
            out["firmware"] = raw.get("deviceModel")
            if raw.get("temp") is not None: out["temperatures"].append({"name":"ASIC", "c": raw.get("temp")})
            if raw.get("vrTemp") is not None: out["temperatures"].append({"name":"VR", "c": raw.get("vrTemp")})
            out["boards"] = [{
                "asic_count": raw.get("asicCount"),
                "small_core_count": raw.get("smallCoreCount"),
                "frequency": raw.get("frequency"),
                "core_voltage": raw.get("coreVoltage"),
            }]

        elif family == "goldshell":
            out["hardware_errors"] = raw.get("hardware_errors")
            out["firmware"] = ((raw.get("full") or {}).get("version") or {}).get("Miner")

        elif family == "iceriver":
            out["firmware"] = raw.get("firmware")
            out["hardware_errors"] = raw.get("hardware_errors")
            inlet = raw.get("inlet_temps") or []
            outlet = raw.get("outlet_temps") or []
            for i, v in enumerate(inlet): out["temperatures"].append({"name":f"Sensor A {i+1}", "c":v})
            for i, v in enumerate(outlet): out["temperatures"].append({"name":f"Sensor B {i+1}", "c":v})
            boards = raw.get("boards")
            if isinstance(boards, list): out["boards"] = boards[:8]

    return out


@app.get("/api/miners/{miner_id}/detail")
def miner_detail(miner_id:int):
    m=next((x for x in miners_get() if x["id"]==miner_id),None)
    if not m:raise HTTPException(404,"Miner not found")
    return m

@app.get("/api/miners/{miner_id}/history")
def history(miner_id: int, seconds: int = 3600):
    seconds=max(60,min(seconds,30*86400)); cutoff=int(time.time())-seconds
    with db() as c:
        rows=[dict(r) for r in c.execute("SELECT ts,hashrate,temp_c,power_w,accepted,rejected,online FROM samples WHERE miner_id=? AND ts>=? ORDER BY ts",(miner_id,cutoff))]
    if len(rows)>800:
        step=max(1,len(rows)//800); rows=rows[::step]
    return rows


FAVICON_SVG = """<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 64 64"><rect width="64" height="64" rx="14" fill="#06111e"/><path d="M32 8 53 20v24L32 56 11 44V20Z" fill="none" stroke="#48c8ff" stroke-width="4"/><path d="M13 33h10l4-12 7 23 5-16 4 5h9" fill="none" stroke="#6ee7ff" stroke-width="3.5" stroke-linecap="round" stroke-linejoin="round"/></svg>"""

INDEX_HTML = r"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width,initial-scale=1"/>
<title>RigPulse</title>
<link rel="icon" href="/favicon.svg" type="image/svg+xml"/>
<style>
:root{
  --bg:#050b14;--panel:#0b1523;--panel2:#101d2d;--line:#1b2d43;--text:#f5f8fb;
  --muted:#8da0b8;--blue:#38b6ff;--green:#31da7a;--red:#ff4d57;--orange:#ff9e2c;--purple:#c256ff;
  --card-opacity:.82;--glass-blur:14px;--bg-intensity:1;--accent-rgb:56,182,255;
}
*{box-sizing:border-box}
body{margin:0;color:var(--text);font-family:Inter,ui-sans-serif,system-ui,-apple-system,Segoe UI,sans-serif;background-color:var(--bg);min-height:100vh}
body::before{content:"";position:fixed;inset:0;z-index:-2;pointer-events:none;
 background:
 radial-gradient(circle at 82% 5%,rgba(var(--accent-rgb),calc(.20 * var(--bg-intensity))),transparent 30%),
 radial-gradient(circle at 18% 72%,rgba(194,86,255,calc(.10 * var(--bg-intensity))),transparent 36%),
 linear-gradient(135deg,#040912 0%,#07111d 48%,#040912 100%);
}
body::after{content:"";position:fixed;inset:0;z-index:-1;pointer-events:none;opacity:calc(.18 * var(--bg-intensity));
 background-image:linear-gradient(rgba(255,255,255,.025) 1px,transparent 1px),linear-gradient(90deg,rgba(255,255,255,.025) 1px,transparent 1px);
 background-size:42px 42px;mask-image:linear-gradient(to bottom,black,transparent 88%);
}
body.theme-aurora{--accent-rgb:65,255,183}
body.theme-bitcoin{--accent-rgb:247,147,26}
body.theme-matrix{--accent-rgb:50,255,112}
body.theme-ocean{--accent-rgb:45,132,255}
body.theme-mining-room{--accent-rgb:58,172,255}
body.theme-liquid-lab{--accent-rgb:71,205,255}
body.theme-nerd-console{--accent-rgb:74,227,255;--blue:#4ee4ff;--green:#62f58b;--orange:#ffe44f;--red:#ff5468;--card-opacity:.96;--glass-blur:0px;font-family:"Segoe UI",Arial,sans-serif}
body.theme-aurora::before{background:radial-gradient(circle at 80% 5%,rgba(56,255,184,calc(.22 * var(--bg-intensity))),transparent 31%),radial-gradient(circle at 15% 75%,rgba(126,82,255,calc(.17 * var(--bg-intensity))),transparent 37%),linear-gradient(135deg,#030b12,#07151b 50%,#050917)}
body.theme-bitcoin::before{background:radial-gradient(circle at 82% 4%,rgba(247,147,26,calc(.26 * var(--bg-intensity))),transparent 30%),radial-gradient(circle at 20% 80%,rgba(255,70,30,calc(.10 * var(--bg-intensity))),transparent 35%),linear-gradient(135deg,#090805,#120d07 48%,#05080e)}
body.theme-matrix::before{background:radial-gradient(circle at 76% 7%,rgba(45,255,105,calc(.18 * var(--bg-intensity))),transparent 32%),linear-gradient(135deg,#020805,#06110a 50%,#020706)}
body.theme-ocean::before{background:radial-gradient(circle at 84% 5%,rgba(37,130,255,calc(.28 * var(--bg-intensity))),transparent 30%),radial-gradient(circle at 10% 80%,rgba(0,210,255,calc(.10 * var(--bg-intensity))),transparent 36%),linear-gradient(135deg,#020714,#061325 50%,#030917)}
body.theme-mining-room::before{background:linear-gradient(rgba(2,8,18,.58),rgba(2,8,18,.68)),url('/assets/mining-room.webp') center/cover fixed no-repeat;filter:brightness(calc(.38 + var(--bg-intensity) * .18))}
body.theme-mining-room::after{opacity:.08}
body.theme-liquid-lab::before{background:linear-gradient(rgba(2,6,18,.56),rgba(2,6,18,.68)),url('/assets/liquid-data-center.webp') center/cover fixed no-repeat;filter:brightness(calc(.38 + var(--bg-intensity) * .18))}
body.theme-liquid-lab::after{opacity:.06}
body.theme-nerd-console::before{background:radial-gradient(circle at 50% 8%,rgba(19,90,118,.34),transparent 38%),linear-gradient(rgba(3,12,20,.96),rgba(1,7,13,.98)),repeating-linear-gradient(90deg,transparent 0 58px,rgba(65,226,255,.07) 59px 60px),repeating-linear-gradient(0deg,transparent 0 58px,rgba(65,226,255,.06) 59px 60px)}body.theme-nerd-console::after{opacity:.2;background-image:repeating-linear-gradient(0deg,rgba(255,255,255,.025) 0,rgba(255,255,255,.025) 1px,transparent 1px,transparent 4px)}
button,input,select{font:inherit}
.app{display:grid;grid-template-columns:180px 1fr;min-height:100vh}
.sidebar{border-right:1px solid rgba(90,130,180,.18);padding:22px 14px;position:sticky;top:0;height:100vh;background:rgba(5,13,24,.70);backdrop-filter:blur(var(--glass-blur))}
body.sidebar-hidden .app{grid-template-columns:1fr}body.sidebar-hidden .sidebar{display:none}body.sidebar-hidden .main{max-width:none}
.brand{font-size:23px;font-weight:800;margin:4px 8px 25px;color:white}.brand span{color:var(--blue)}
.nav button{width:100%;text-align:left;border:0;background:transparent;color:#c9d6e5;padding:12px 13px;border-radius:10px;margin:2px 0;cursor:pointer}.nav button.active,.nav button:hover{background:#0c2742;color:#55c1ff}
.main{padding:18px 22px 60px;max-width:1600px;width:100%}
.top{display:flex;justify-content:space-between;align-items:center;margin-bottom:14px}
.top h1{margin:0;font-size:24px}.pill,.btn{border:1px solid #22354a;background:#0d1827;color:white;padding:9px 12px;border-radius:10px}
.btn{cursor:pointer}.btn.primary{background:#0877e8;border-color:#0877e8}.btn.danger{background:#35161b;border-color:#70242c}
.metrics{display:grid;grid-template-columns:repeat(6,minmax(0,1fr));gap:10px}
.dashboard-panels{display:grid}.layout-box{position:relative}.layout-size-btn{display:none;position:absolute;z-index:12;right:8px;bottom:8px;border:1px solid #4b7399;background:#10243a;color:#bde8ff;border-radius:7px;padding:3px 7px;cursor:pointer;font-size:12px}.layout-edit .layout-box{outline:2px dashed rgba(95,200,255,.65);outline-offset:2px;cursor:grab}.layout-edit .layout-box:active{cursor:grabbing}.layout-edit .layout-size-btn{display:block}.layout-edit .layout-box.dragging{opacity:.48}.metrics>.layout-wide{grid-column:span 2}.metrics>.layout-full{grid-column:1/-1}.chain-strips>.layout-wide{grid-column:span 2}.chain-strips>.layout-full{grid-column:1/-1}
.card{background:linear-gradient(180deg,rgba(13,27,44,var(--card-opacity)),rgba(6,15,27,var(--card-opacity)));border:1px solid rgba(var(--accent-rgb),.20);border-radius:14px;box-shadow:0 12px 34px rgba(0,0,0,.28);backdrop-filter:blur(var(--glass-blur));-webkit-backdrop-filter:blur(var(--glass-blur))}
.metric{min-width:0;padding:14px}.metric .label{color:#b8c5d5;font-size:12px;line-height:1.25}.metric .value{font-size:clamp(20px,2vw,26px);font-weight:800;margin-top:7px;overflow-wrap:anywhere}.metric .sub{line-height:1.25}.blue{color:#5fc8ff}.green{color:var(--green)}.orange{color:var(--orange)}.purple{color:#d576ff}
.stream{padding:13px 16px;margin-top:12px;overflow:hidden;position:relative}.stream-title{display:flex;justify-content:space-between}.stream-row{display:flex;gap:20px;align-items:center;min-height:48px;color:#c8d4e2;overflow:hidden}
.stream-item{min-width:60px;text-align:center;font-size:25px}.stream-item small{display:block;font-size:10px;color:#8192a7}
.toolbar{display:flex;gap:8px;margin:14px 0;align-items:center}.toolbar .spacer{flex:1}
.grid{display:grid;grid-template-columns:repeat(3,minmax(250px,1fr));gap:12px}
.miner{padding:16px;position:relative;overflow:hidden}.miner.offline{border-color:#6f232a}.miner.online{border-color:#1b6f46}.miner-head{display:flex;justify-content:space-between;gap:10px}.miner h3{margin:0;font-size:18px}.sub{color:#8295ac;font-size:12px;margin-top:3px}.status{font-size:12px}.hash{font-size:30px;font-weight:800;margin:18px 0 8px;color:#6fd2ff}.spark{height:42px;width:100%;margin:4px 0 10px}.stats{display:grid;grid-template-columns:repeat(4,1fr);gap:8px;border-top:1px solid #15263a;padding-top:12px}.stats b{display:block;font-size:13px}.stats span{color:#8091a5;font-size:10px}
.miner button[onclick^="probe("],.miner button[onclick^="diagnose("],.miner button[onclick^="iceRaw("]{display:none}
.miner.fleet-best{overflow:visible;isolation:isolate}.miner.fleet-best::before{content:"";position:absolute;inset:-3px;z-index:2;border-radius:17px;padding:3px;background:conic-gradient(from var(--best-angle),#ff3b6b,#ffb629,#54ef78,#2ee7ff,#785bff,#ff3bce,#ff3b6b);-webkit-mask:linear-gradient(#000 0 0) content-box,linear-gradient(#000 0 0);-webkit-mask-composite:xor;mask-composite:exclude;pointer-events:none;animation:bestRing 4s linear infinite;filter:drop-shadow(0 0 6px rgba(70,190,255,.4))}.miner.fleet-best::after{content:"Fleet Best";position:absolute;z-index:3;right:12px;top:-10px;background:#07111d;border:1px solid #4dcfff;color:#9feaff;border-radius:999px;padding:3px 8px;font-size:10px;font-weight:800}.miner.fleet-best>.miner-head{position:relative}@property --best-angle{syntax:'<angle>';initial-value:0deg;inherits:false}@keyframes bestRing{to{--best-angle:360deg}}@media(prefers-reduced-motion:reduce){.miner.fleet-best::before{animation:none}}
.modal{display:none;position:fixed;inset:0;background:#000a;z-index:20;align-items:center;justify-content:center;padding:20px}.modal.show{display:flex}.dialog{width:min(540px,100%);padding:20px}.dialog h2{margin-top:0}.field{margin:12px 0}.field label{display:block;color:#91a4bb;font-size:12px;margin-bottom:5px}.field input,.field select{width:100%;background:#07111d;color:white;border:1px solid #24364a;border-radius:9px;padding:11px}
#diagModal{z-index:80;background:rgba(0,0,0,.82)}#diagModal .dialog{background:linear-gradient(180deg,rgba(9,22,37,.99),rgba(4,12,22,.99));box-shadow:0 24px 80px rgba(0,0,0,.8)}
.row{display:flex;gap:8px}.row>*{flex:1}.actions{display:flex;justify-content:flex-end;gap:8px;margin-top:18px}
.celebrate{position:fixed;pointer-events:none;z-index:50;font-size:30px;animation:fall 2.2s ease-out forwards}
@keyframes fall{0%{transform:translateY(-20px) rotate(0) scale(.7);opacity:0}10%{opacity:1}100%{transform:translateY(75vh) rotate(420deg) scale(1.25);opacity:0}}
.toast{position:fixed;top:15px;left:50%;transform:translateX(-50%);z-index:60;background:#101c2c;border:1px solid #2b4360;padding:10px 15px;border-radius:12px;box-shadow:0 8px 30px #0008;display:none}
.tabs{display:flex;gap:8px;flex-wrap:wrap}.section-title{font-size:18px;margin:20px 0 10px}.detail-grid{display:grid;grid-template-columns:repeat(4,1fr);gap:10px}.detail-stat{background:#07111d;border:1px solid #1b2d43;border-radius:10px;padding:12px}.detail-stat small{display:block;color:#8192a7;margin-bottom:5px}.detail-stat b{font-size:18px}.chart-wrap{background:#07111d;border:1px solid #1b2d43;border-radius:12px;padding:10px;margin-top:12px}.chart-canvas{width:100%;height:220px;display:block}
.health-banner{display:flex;align-items:center;gap:9px;padding:8px 11px;border-radius:10px;margin:8px 0 11px;border:1px solid #1c334c;background:rgba(10,21,34,.72);min-height:42px}
.health-banner.ok{border-color:#1f6f49}
.health-banner.warn{border-color:#8a6a20}
.health-banner.bad{border-color:#8a2e38}
.health-dot{font-size:14px}
.health-title{font-weight:800;font-size:13px;line-height:1.1}
.health-detail{color:#8fa4bb;font-size:10px;margin-top:2px}
.health-list{display:grid;gap:10px;margin-top:12px}
.health-item{background:#07111d;border:1px solid #1b2d43;border-radius:10px;padding:11px}
.alert-grid{display:grid;grid-template-columns:repeat(2,1fr);gap:10px}
.field label{display:block;color:#8da0b8;font-size:12px;margin-bottom:5px}
.field input,.field select{width:100%;box-sizing:border-box;background:#0b1725;color:white;border:1px solid #24384f;border-radius:9px;padding:9px}
@media(max-width:700px){.alert-grid{grid-template-columns:1fr}}
.event-table{width:100%;border-collapse:collapse}.event-table td,.event-table th{padding:8px;border-bottom:1px solid #172a40;text-align:left;font-size:12px}.event-table th{color:#8da0b8}.clickable{cursor:pointer}.sort-select{background:#0d1827;color:white;border:1px solid #22354a;padding:9px 12px;border-radius:10px}@media(max-width:800px){.detail-grid{grid-template-columns:repeat(2,1fr)}}

.chain-strips{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:10px}.block-strip{margin-top:12px;padding:14px 16px;display:grid;grid-template-columns:auto 1fr auto;gap:16px;align-items:center;overflow:hidden;position:relative;min-width:0}.wallet-strip{margin-top:10px;padding:10px 14px;display:flex;gap:18px;align-items:center;flex-wrap:wrap}.wallet-item{color:#8fa4bb;font-size:11px}.wallet-item b{color:#fff;font-size:14px;margin-left:5px}.market-price{color:#fff!important;font-size:14px;font-weight:900}.price-change{font-size:10px;margin-left:5px}.price-change.up{color:#37e488}.price-change.down{color:#ff6577}.alph-strip .block-icon{color:#d576ff;background:rgba(213,118,255,.12);border-color:rgba(213,118,255,.35)}.alph-strip .block-height{color:#d576ff}
.block-strip::after{content:"";position:absolute;inset:auto -10% -80% auto;width:240px;height:180px;background:radial-gradient(circle,rgba(247,147,26,.16),transparent 65%);pointer-events:none}
.block-icon{width:46px;height:46px;border-radius:12px;display:grid;place-items:center;font-size:25px;background:rgba(247,147,26,.12);border:1px solid rgba(247,147,26,.35)}
.block-main{display:flex;align-items:baseline;gap:12px;flex-wrap:wrap}.block-height{font-size:23px;font-weight:900}.block-hash{font-family:ui-monospace,SFMono-Regular,Consolas,monospace;color:#8fa4bb;font-size:11px}
.block-meta{display:flex;gap:16px;color:#91a4bb;font-size:12px;flex-wrap:wrap}.block-meta b{color:#fff}
.solo-odds{margin-top:9px;color:#9bb0c8;font-size:13px;line-height:1.4}.solo-odds b{color:#70d8ff;font-size:14px}.solopool-strip{margin-top:10px;padding:12px 14px;display:grid;grid-template-columns:auto repeat(2,minmax(0,1fr));gap:12px;align-items:stretch}.solopool-item{min-width:0;color:#8fa4bb;font-size:11px;background:rgba(5,16,29,.62);border:1px solid rgba(var(--accent-rgb),.18);border-radius:11px;padding:11px 13px}.solopool-item>b{color:#a9bdd2;font-size:11px}.solopool-title{font-weight:800;display:flex;align-items:center}.solopool-hash{font-size:clamp(20px,2vw,27px);font-weight:900;color:#5fc8ff;margin:5px 0 3px;line-height:1}.solopool-item.bch .solopool-hash{color:#d576ff}.solopool-stats{display:flex;gap:7px 12px;flex-wrap:wrap;margin-top:6px}.solopool-stats span{white-space:nowrap}.solopool-stats b{color:#fff}.solopool-highlights{display:flex;gap:8px;margin-top:9px}.solopool-highlight{flex:1;min-width:0;padding:8px 10px;border-radius:9px;background:rgba(17,29,45,.8);border:1px solid rgba(var(--accent-rgb),.24)}.solopool-highlight span{display:block;color:#8fa4bb;font-size:10px;text-transform:uppercase;letter-spacing:.05em}.solopool-highlight b{display:block;margin-top:3px;color:#37e488;font-size:clamp(18px,1.7vw,24px);font-weight:900;line-height:1}.solopool-item.bch .solopool-highlight b{color:#d576ff}.pool-miner-card .hash{color:#5fc8ff}.pool-miner-card.bch .hash{color:#d576ff}
.block-pulse{animation:blockPulse 1.8s ease}
@keyframes blockPulse{0%{box-shadow:0 0 0 0 rgba(247,147,26,.65)}100%{box-shadow:0 0 0 30px rgba(247,147,26,0)}}
.miner.block-winner{border-color:#ffd84d;box-shadow:0 0 18px rgba(255,190,30,.38)}.block-found-badge{position:absolute;z-index:4;left:12px;top:-10px;background:#5b2500;border:1px solid #ffcf40;color:#fff1a8;border-radius:999px;padding:3px 8px;font-size:10px;font-weight:900}.card-confetti{position:absolute;z-index:5;pointer-events:none;font-size:14px;animation:cardConfetti 1.8s linear forwards}@keyframes cardConfetti{from{transform:translateY(-20px) rotate(0);opacity:1}to{transform:translateY(210px) rotate(500deg);opacity:0}}
.block-party{z-index:100;background:rgba(0,0,0,.88)}.block-party-panel{text-align:center;max-width:620px;background:radial-gradient(circle at 50% 0,rgba(255,190,30,.28),rgba(7,17,29,.99) 58%)}.block-party-title{font-size:clamp(38px,8vw,78px);font-weight:1000;color:#ffd84d;text-shadow:0 0 22px rgba(255,190,30,.6);line-height:1}.block-party-miner{font-size:25px;font-weight:800;margin-top:14px}
.custom-preview{min-height:142px;border-radius:13px;border:1px solid rgba(var(--accent-rgb),.28);background:linear-gradient(135deg,rgba(var(--accent-rgb),.18),rgba(5,12,22,.45));padding:16px;display:flex;align-items:center;overflow:hidden}
.custom-preview-card{width:min(430px,72%);min-height:86px;border-radius:12px;background:rgba(8,20,34,var(--card-opacity));border:1px solid rgba(var(--accent-rgb),.3);backdrop-filter:blur(var(--glass-blur));padding:12px;display:flex;flex-direction:column;justify-content:center}.custom-preview-card .hash{font-size:22px;margin:7px 0 0;line-height:1.15}
.range-row{display:grid;grid-template-columns:1fr 70px;gap:10px;align-items:center}
.background-choices{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:9px;margin-top:8px}.background-choice{min-height:72px;border:1px solid #29405b;border-radius:11px;color:#fff;text-align:left;padding:10px;cursor:pointer;background:#07111d center/cover}.background-choice.active{border-color:#62d7ff;box-shadow:0 0 0 2px rgba(70,200,255,.22)}.background-choice span{display:inline-block;background:rgba(2,8,18,.78);border-radius:7px;padding:4px 7px;font-size:11px}.bg-mining{background-image:linear-gradient(#04102088,#04102088),url('/assets/mining-room.webp')}.bg-liquid{background-image:linear-gradient(#04102077,#04102077),url('/assets/liquid-data-center.webp')}.bg-colors{background-image:linear-gradient(135deg,#0a1f3d,#1a0e35,#063a39)}@media(max-width:650px){.background-choices{grid-template-columns:repeat(2,1fr)}}
.nav-sep{height:1px;background:#14243a;margin:8px 5px}
body.compact .miner{padding:12px} body.compact .miner .hash{margin:10px 0 5px;font-size:27px} body.compact .miner .spark{height:32px}


.emoji-picker{display:grid;grid-template-columns:repeat(9,1fr);gap:7px;margin-top:8px}
.emoji-picker button{background:rgba(12,28,47,.72);border:1px solid #29405b;border-radius:9px;padding:7px 3px;font-size:21px;cursor:pointer}
.emoji-picker button:hover{border-color:rgba(var(--accent-rgb),.8);transform:translateY(-1px)}
.history-toolbar{display:flex;gap:8px;align-items:center;flex-wrap:wrap;margin:12px 0}
.history-miners{display:grid;gap:12px}.history-row{background:rgba(5,16,29,.55);border:1px solid #1a3048;border-radius:12px;padding:12px}
.history-row-head{display:flex;justify-content:space-between;gap:10px;align-items:center}.history-stats{display:flex;gap:16px;flex-wrap:wrap;color:#8fa4bb;font-size:12px;margin-top:7px}.history-stats b{color:#fff}
.history-spark{height:72px;margin-top:7px}
.pool-grid{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:10px;margin-top:12px}.pool-card{background:rgba(5,16,29,.58);border:1px solid #1a3048;border-radius:12px;padding:13px;min-width:0}
.pool-url{font-family:ui-monospace,SFMono-Regular,Consolas,monospace;font-size:11px;color:#9bb1c7;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;margin:7px 0}
.pool-stats{display:grid;grid-template-columns:repeat(3,1fr);gap:8px;margin-top:10px}.pool-stats small{display:block;color:#778da5}.pool-stats b{font-size:12px}
.log-toolbar{display:flex;justify-content:space-between;gap:10px;align-items:center;margin-top:12px;flex-wrap:wrap}.log-badge{padding:3px 7px;border-radius:999px;background:#102038;color:#a8bdd2;font-size:11px}
@media(max-width:750px){.emoji-picker{grid-template-columns:repeat(6,1fr)}.pool-grid{grid-template-columns:1fr}}


.network-grid{display:grid;grid-template-columns:repeat(5,minmax(0,1fr));gap:10px;margin:12px 0}
.network-card{padding:13px}.network-card small{display:block;color:#7e93aa;margin-bottom:5px}.network-card b{font-size:18px}
.leaderboard{width:100%;border-collapse:collapse;margin-top:10px}.leaderboard td,.leaderboard th{padding:8px;border-bottom:1px solid #183049;text-align:left;font-size:12px}.leaderboard th{color:#8094ab}
.hardware-grid{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:10px;margin-top:10px}.hardware-card{background:rgba(5,16,29,.55);border:1px solid #1a3048;border-radius:11px;padding:10px}.hardware-card small{display:block;color:#7f93aa}.hardware-card b{font-size:13px}
.board-table{width:100%;border-collapse:collapse;margin-top:9px}.board-table td,.board-table th{padding:6px;border-bottom:1px solid #183049;text-align:left;font-size:11px}.board-table th{color:#7d91a7}
.detail-tabs{display:flex;gap:7px;flex-wrap:wrap;margin:12px 0}.detail-tab.active{border-color:rgba(var(--accent-rgb),.8);box-shadow:0 0 0 1px rgba(var(--accent-rgb),.18)}
@media(max-width:900px){.network-grid{grid-template-columns:repeat(2,1fr)}.hardware-grid{grid-template-columns:1fr 1fr}}

/* Nerd Console: appliance-style ASIC instrumentation across the full dashboard. */
body.theme-nerd-console .sidebar{background:linear-gradient(180deg,#081827,#030a11);border-right:2px solid #20516b;box-shadow:inset -4px 0 0 #06111c}body.theme-nerd-console .brand{font-style:italic;letter-spacing:.08em;text-transform:uppercase;text-shadow:0 0 12px #33dcff}body.theme-nerd-console .nav button{border-radius:2px;border-left:3px solid transparent;text-transform:uppercase;font-size:11px;font-weight:800;letter-spacing:.05em}body.theme-nerd-console .nav button:hover,body.theme-nerd-console .nav button.active{border-left-color:#4ee4ff;background:linear-gradient(90deg,#12364d,#071522);box-shadow:inset 0 0 18px #37cfff22}
body.theme-nerd-console .top h1{text-transform:uppercase;font-style:italic;letter-spacing:.09em;text-shadow:0 0 13px #39dfff77}body.theme-nerd-console .pill,body.theme-nerd-console .btn{border-radius:2px;text-transform:uppercase;font-weight:800;font-size:11px;letter-spacing:.035em;background:linear-gradient(180deg,#14304a,#081725);border-color:#3a789b;clip-path:polygon(7px 0,100% 0,100% calc(100% - 7px),calc(100% - 7px) 100%,0 100%,0 7px)}
body.theme-nerd-console .card{border-radius:3px;border:2px solid #275978;background:linear-gradient(145deg,rgba(14,38,57,.98),rgba(4,13,22,.98) 58%,rgba(8,29,43,.98));box-shadow:inset 0 0 0 2px #020810,0 0 0 1px #4adfff33,0 10px 24px #0009;clip-path:polygon(10px 0,calc(100% - 7px) 0,100% 7px,100% calc(100% - 10px),calc(100% - 10px) 100%,7px 100%,0 calc(100% - 7px),0 10px)}
body.theme-nerd-console .metrics{gap:7px}body.theme-nerd-console .metric{min-height:90px;padding:12px 13px;border-top-width:4px}body.theme-nerd-console .metric:nth-child(1){border-top-color:#4ee4ff}body.theme-nerd-console .metric:nth-child(2),body.theme-nerd-console .metric:nth-child(3){border-top-color:#dc60ff}body.theme-nerd-console .metric:nth-child(4){border-top-color:#53f59a}body.theme-nerd-console .metric:nth-child(5){border-top-color:#65aaff}body.theme-nerd-console .metric:nth-child(6){border-top-color:#ffe44f}body.theme-nerd-console .metric .label{text-transform:uppercase;font-size:10px;font-weight:900;letter-spacing:.09em;color:#acd4e7}body.theme-nerd-console .metric .value{font-family:Consolas,"Courier New",monospace;font-size:clamp(23px,2.25vw,32px);line-height:1;color:#f7fbff;text-shadow:0 0 10px currentColor}body.theme-nerd-console .metric .sub{text-transform:uppercase;font-size:9px;color:#7faac1}
body.theme-nerd-console .health-banner{border-radius:2px;border-width:2px;background:linear-gradient(90deg,#092338,#07121d);text-transform:uppercase;clip-path:polygon(8px 0,100% 0,100% calc(100% - 8px),calc(100% - 8px) 100%,0 100%,0 8px)}
body.theme-nerd-console .chain-strips{gap:7px}body.theme-nerd-console .block-strip{border-top:4px solid #ffe44f;background:linear-gradient(135deg,#0b2335 0 17%,#07131e 17% 66%,#102c40 66%);min-height:128px}body.theme-nerd-console .bch-strip{border-top-color:#62f58b}body.theme-nerd-console .alph-strip{border-top-color:#dc60ff}body.theme-nerd-console .block-icon{border-radius:50%;background:#07111c;border:3px solid #4ee4ff;box-shadow:0 0 12px #43dcff88;font-weight:900}body.theme-nerd-console .block-height{font-family:Consolas,"Courier New",monospace;text-transform:uppercase;font-size:21px;color:#fff}body.theme-nerd-console .block-meta{margin-top:7px;gap:7px 12px;text-transform:uppercase;font-size:10px}body.theme-nerd-console .block-meta span{background:#0d293d;border-left:3px solid #43dcff;padding:4px 6px}body.theme-nerd-console .solo-odds{border-top:1px solid #28506a;padding-top:7px;font-family:Consolas,"Courier New",monospace}
body.theme-nerd-console .wallet-strip,body.theme-nerd-console .solopool-strip,body.theme-nerd-console .stream{border-left:5px solid #45dfff}body.theme-nerd-console .solopool-item{border-radius:2px;background:#06121d;border-width:2px}body.theme-nerd-console .solopool-hash{font-family:Consolas,"Courier New",monospace;text-shadow:0 0 10px currentColor}body.theme-nerd-console .solopool-highlight{border-radius:2px;background:#102a3d}body.theme-nerd-console .stream-title{text-transform:uppercase;letter-spacing:.07em}
body.theme-nerd-console .miner{border-width:2px;border-top-width:5px;padding:13px;background:linear-gradient(135deg,#091a27 0 22%,#07111b 22% 68%,#0b2637 68%);min-height:300px}body.theme-nerd-console .miner.online{border-color:#2a6b82;border-top-color:#62f58b}body.theme-nerd-console .miner.offline{border-color:#772c3b;border-top-color:#ff5468}body.theme-nerd-console .miner h3{text-transform:uppercase;font-style:italic;letter-spacing:.06em;font-size:20px;color:#f3fbff}body.theme-nerd-console .miner-head .sub{text-transform:uppercase;color:#6fa8c2;font-size:10px}body.theme-nerd-console .miner .status{background:#07111c;border:1px solid currentColor;padding:4px 6px;text-transform:uppercase;font-weight:900;font-size:9px}body.theme-nerd-console .miner .hash{font-family:Consolas,"Courier New",monospace;font-size:clamp(29px,3vw,41px);color:#d9ff46;text-shadow:0 0 12px #aaff2266;background:linear-gradient(90deg,#b9dc35,#f4ff72);-webkit-background-clip:text;background-clip:text;-webkit-text-fill-color:transparent;margin:14px 0 5px}body.theme-nerd-console .miner .spark{background:#04101a;border:1px solid #183c52;margin-top:8px}body.theme-nerd-console .miner .stats{gap:5px;border:0;padding-top:5px}body.theme-nerd-console .miner .stats>div{background:#102a3d;border-left:3px solid #4ee4ff;padding:7px 6px;min-width:0}body.theme-nerd-console .miner .stats>div:nth-child(1){border-left-color:#62f58b;background:#0e332c}body.theme-nerd-console .miner .stats>div:nth-child(2){border-left-color:#65aaff;background:#10263d}body.theme-nerd-console .miner .stats>div:nth-child(4),body.theme-nerd-console .miner .stats>.current-share-stat{border-left-color:#ffe44f;background:#332f12}body.theme-nerd-console .miner .stats span{text-transform:uppercase;font-size:8px;letter-spacing:.04em;color:#91b7c9}body.theme-nerd-console .miner .stats b{font-family:Consolas,"Courier New",monospace;font-size:13px;color:#fff}body.theme-nerd-console .miner.fleet-best{clip-path:none}body.theme-nerd-console .miner.fleet-best::after{border-radius:2px;text-transform:uppercase;background:#111d27}
body.theme-nerd-console .dialog.card{clip-path:none;border-radius:4px}body.theme-nerd-console .custom-preview-card{border-radius:2px;border-width:2px;background:#07131e}.bg-nerd{background-image:linear-gradient(135deg,#07111c 0 25%,#16445c 25% 44%,#06101a 44% 70%,#84e83e 70% 76%,#0b2537 76%)}

/* Nerd Console v2: closer to the compact LCD appliance hierarchy and palette. */
body.theme-nerd-console{color:#edf8ff;background:#020609;font-family:"Bahnschrift Condensed","Arial Narrow","Trebuchet MS",sans-serif}body.theme-nerd-console::before{background:linear-gradient(rgba(1,5,8,.78),rgba(1,5,8,.9)),url('/assets/nerd-console-grid.svg') center top/cover fixed no-repeat;filter:none}body.theme-nerd-console .main{max-width:none}
body.theme-nerd-console .card{color:#eaf7ff;background:linear-gradient(145deg,#07131d,#0a1f2e 62%,#07131c);border:2px solid #9ed9ed;box-shadow:inset 0 0 0 3px #071019,0 0 0 1px #243f4d,0 8px 18px #000b}
body.theme-nerd-console .solopool-strip{order:-10;grid-template-columns:150px repeat(2,minmax(0,1fr));border:3px solid #d7edf5;background:linear-gradient(110deg,#d8e8ef 0 14%,#08131d 14%);padding:10px;gap:9px}body.theme-nerd-console .solopool-title{color:#07121b;font-size:18px;line-height:1;text-transform:uppercase;font-style:italic;letter-spacing:.04em;padding:12px}body.theme-nerd-console .solopool-title::after{content:"Mining Stats";display:block;color:#b51bff;font-size:11px;margin-top:5px}body.theme-nerd-console .solopool-item{background:linear-gradient(135deg,#07121c,#102b3d);border:2px solid #b9e5f4;padding:10px}body.theme-nerd-console .solopool-item>b{color:#d9f1fb;font-size:13px;letter-spacing:.08em}body.theme-nerd-console .solopool-console-main{display:grid;grid-template-columns:70px 1fr;gap:10px;align-items:center}body.theme-nerd-console .pool-gauge{--pool-pct:0%;width:62px;height:62px;border-radius:50%;display:grid;place-items:center;background:conic-gradient(#8ef24e var(--pool-pct),#ffdf42 var(--pool-pct) 78%,#ff496a 78%);box-shadow:0 0 12px #70f7ff66;position:relative}body.theme-nerd-console .pool-gauge::before{content:"";position:absolute;inset:7px;border-radius:50%;background:#06111a;border:2px solid #b9e5f4}body.theme-nerd-console .pool-gauge span{z-index:1;color:#fff;font:900 14px Consolas,monospace;text-align:center}body.theme-nerd-console .pool-gauge small{display:block;color:#88b6ca;font-size:7px;text-transform:uppercase}body.theme-nerd-console .solopool-hash{font-size:clamp(25px,2.4vw,36px);color:#d5ff43;text-shadow:0 0 13px #a8ed3288}body.theme-nerd-console .solopool-item.bch .solopool-hash{color:#f05cff}body.theme-nerd-console .solopool-highlights{display:grid;grid-template-columns:1fr 1fr}body.theme-nerd-console .solopool-highlight{background:linear-gradient(90deg,#e5edf0,#bcd5df);border:0;border-left:7px solid #d7ff43;color:#07111a;padding:7px 10px}body.theme-nerd-console .solopool-highlight:nth-child(2){border-left-color:#f05cff}body.theme-nerd-console .solopool-highlight span{color:#273f4a;font-weight:900}body.theme-nerd-console .solopool-highlight b,body.theme-nerd-console .solopool-item.bch .solopool-highlight b{color:#07111a;text-shadow:none;font-size:26px}
body.theme-nerd-console .wallet-strip{background:linear-gradient(100deg,#dce9ee,#9cc6d6);color:#07131d;border:3px solid #eefaff;gap:25px}body.theme-nerd-console .wallet-strip>b{font-size:17px;text-transform:uppercase}body.theme-nerd-console .wallet-item{color:#213b48;font-weight:900}body.theme-nerd-console .wallet-item b{color:#07131d;font:900 17px Consolas,monospace}
body.theme-nerd-console .toolbar{margin-top:14px;background:#07131d;border-top:3px solid #d5ff43;padding:8px}body.theme-nerd-console .grid{grid-template-columns:repeat(auto-fit,minmax(390px,1fr));gap:10px;align-items:start}body.theme-nerd-console .miner{color:#07131d;background:linear-gradient(145deg,#c9dbe2 0 18%,#a9c4cf 18% 72%,#779dac 72%);border:2px solid #d9f3fb;border-top:5px solid #d5ff43;box-shadow:inset 0 0 0 2px #334d58,0 6px 16px #0009;clip-path:polygon(11px 0,calc(100% - 7px) 0,100% 7px,100% calc(100% - 11px),calc(100% - 11px) 100%,7px 100%,0 calc(100% - 7px),0 11px)}body.theme-nerd-console .miner.offline{border-color:#ffe8eb;border-top-color:#ff496a}body.theme-nerd-console .miner h3{color:#07131d;text-shadow:none;font-size:20px}body.theme-nerd-console .miner-head .sub,body.theme-nerd-console .miner .sub{color:#284b59;font-weight:800}body.theme-nerd-console .miner .status{background:#07131d;color:#eaf8ff!important;border:2px solid #07131d}body.theme-nerd-console .miner .hash{display:block;color:#07120b;background:linear-gradient(180deg,#d9ff4f,#87d629);-webkit-background-clip:border-box;background-clip:border-box;-webkit-text-fill-color:#07120b;text-shadow:none;border:2px solid #496c25;padding:7px 9px;margin:8px 0 4px;text-align:right;font-size:clamp(25px,2vw,34px)}body.theme-nerd-console .miner .spark{background:#050b10;border:2px solid #3d5965}body.theme-nerd-console .pool-miner-card{min-height:0;padding:10px}body.theme-nerd-console .pool-miner-card .spark{display:none}body.theme-nerd-console .miner .stats{grid-template-columns:repeat(3,minmax(0,1fr));gap:4px}body.theme-nerd-console .miner .stats>div{position:relative;background:#c9dce3;border:1px solid #668794;border-left:5px solid #25cfe9;padding:6px 4px 6px 25px;min-height:40px}body.theme-nerd-console .miner .stats>div:nth-child(1){background:#69c6a3;border-left-color:#168d64}body.theme-nerd-console .miner .stats>div:nth-child(2){background:#accbe1;border-left-color:#317bc0}body.theme-nerd-console .miner .stats>div:nth-child(4),body.theme-nerd-console .miner .stats>.current-share-stat{background:#ead967;border-left-color:#c28e00}body.theme-nerd-console .miner .stats span{color:#243f4a;font-weight:900;font-size:9px}body.theme-nerd-console .miner .stats b{color:#07131d;font-size:14px}body.theme-nerd-console .console-stat-icon{display:grid;position:absolute;left:4px;top:7px;width:16px;height:16px;border-radius:50%;place-items:center;background:#07131d;color:#eaf8ff;font:900 10px Arial,sans-serif}body.theme-nerd-console .miner>div:last-child small{color:#264553!important}body.theme-nerd-console .miner.fleet-best{clip-path:none}body.theme-nerd-console .solopool-strip.single-pool{grid-template-columns:150px 1fr}body.theme-nerd-console .solopool-strip.single-pool .solopool-item.unconfigured{display:none}
body.theme-nerd-console .metrics{margin-top:13px}body.theme-nerd-console .metric{background:linear-gradient(160deg,#05090d 0 58%,#122c3c 58%);border-color:#d9eff7}body.theme-nerd-console .metric .label::before{display:inline-grid;width:20px;height:20px;border-radius:50%;place-items:center;margin-right:6px;background:#dbeaf0;color:#07131d;font:900 12px Arial;vertical-align:middle}body.theme-nerd-console .metric:nth-child(1) .label::before{content:"Σ"}body.theme-nerd-console .metric:nth-child(2) .label::before{content:"A"}body.theme-nerd-console .metric:nth-child(3) .label::before{content:"◆"}body.theme-nerd-console .metric:nth-child(4) .label::before{content:"°"}body.theme-nerd-console .metric:nth-child(5) .label::before{content:"ϟ"}body.theme-nerd-console .metric:nth-child(6) .label::before{content:"◎"}body.theme-nerd-console .metric .value{color:#fff}body.theme-nerd-console .metric:nth-child(1) .value,body.theme-nerd-console .metric:nth-child(6) .value{color:#d5ff43}body.theme-nerd-console .metric:nth-child(2) .value,body.theme-nerd-console .metric:nth-child(3) .value{color:#f05cff}
body.theme-nerd-console .chain-strips{margin-top:13px}body.theme-nerd-console .block-strip{background:linear-gradient(150deg,#030609 0 65%,#182c39 65%);border-color:#dcecf2;min-height:170px}body.theme-nerd-console .block-icon{width:58px;height:58px;background:#e3edf1;color:#07131d;border:5px solid #a6d3e2;box-shadow:0 0 0 4px #07131d,0 0 14px #67ecff;font-size:27px}body.theme-nerd-console .block-height{font-size:25px}body.theme-nerd-console .block-meta span{background:#d5e5eb;color:#29414b;border-left:7px solid #21d7f4;padding:5px 7px}body.theme-nerd-console .block-meta b{color:#07131d}body.theme-nerd-console #blockDifficulty,body.theme-nerd-console #bchDifficulty{font:900 19px Consolas,monospace;color:#7d1ba5}body.theme-nerd-console .market-price{color:#07131d!important}body.theme-nerd-console .solo-odds{color:#aec8d3}body.theme-nerd-console .solo-odds b{color:#d5ff43}body.theme-nerd-console .stream{background:#050b10;border:3px solid #d8edf5;border-left:8px solid #f05cff}
body.theme-nerd-console .health-banner{margin-top:10px;background:#07131d;border:2px solid #d9eef6}body.theme-nerd-console .health-title{font-size:15px}body.theme-nerd-console .health-detail{color:#91b6c5}
/* Nerd Console green phosphor palette: dark appliance chassis + readable LCD instrumentation. */
body.theme-nerd-console{--accent-rgb:83,255,89;--blue:#62ff68;--green:#62ff68;--orange:#dfff48;color:#e8f5ea;background:#020503;font-family:"Bahnschrift SemiCondensed","Arial Narrow","Segoe UI",sans-serif}body.theme-nerd-console::before{background:radial-gradient(circle at 50% 0,rgba(33,113,49,.22),transparent 38%),linear-gradient(rgba(1,5,2,.88),rgba(0,3,1,.97)),url('/assets/nerd-console-grid.svg') center top/cover fixed no-repeat}body.theme-nerd-console .sidebar{background:linear-gradient(180deg,#07120a,#010402);border-right-color:#245d2b;box-shadow:inset -4px 0 0 #020804}body.theme-nerd-console .brand{color:#f3fff4;text-shadow:0 0 12px #55ff69}body.theme-nerd-console .brand span{color:#62ff68}body.theme-nerd-console .nav button{color:#a8bdac}body.theme-nerd-console .nav button:hover,body.theme-nerd-console .nav button.active{color:#eaffec;border-left-color:#62ff68;background:linear-gradient(90deg,#14371b,#061009);box-shadow:inset 0 0 18px #4cff6322}body.theme-nerd-console .top h1{color:#effff0;text-shadow:0 0 12px #5dff7077}body.theme-nerd-console .pill,body.theme-nerd-console .btn{background:linear-gradient(180deg,#152c19,#071009);border-color:#387c42;color:#effff0}
body.theme-nerd-console .card{color:#e8f5ea;background:linear-gradient(145deg,#071009,#030704 58%,#0b180d);border-color:#397a42;box-shadow:inset 0 0 0 2px #010302,0 0 0 1px #68ff7730,0 8px 18px #000c}body.theme-nerd-console .health-banner{background:linear-gradient(90deg,#07150a,#030704);border-color:#34743d}body.theme-nerd-console .health-detail,body.theme-nerd-console .metric .sub{color:#83a789}body.theme-nerd-console .toolbar{background:#030704;border-top-color:#62ff68}
body.theme-nerd-console .solopool-strip{border-color:#397a42;background:linear-gradient(110deg,#102415 0 14%,#030704 14%)}body.theme-nerd-console .solopool-title{color:#eaffec}body.theme-nerd-console .solopool-title::after{color:#68ff72}body.theme-nerd-console .solopool-item{background:linear-gradient(135deg,#030704,#0a1a0d);border-color:#397a42}body.theme-nerd-console .solopool-item>b{color:#bdebc2}body.theme-nerd-console .pool-gauge{background:conic-gradient(#62ff68 var(--pool-pct),#dfff48 var(--pool-pct) 78%,#ff5266 78%);box-shadow:0 0 14px #55ff6966}body.theme-nerd-console .pool-gauge::before{background:#020603;border-color:#489752}body.theme-nerd-console .solopool-hash,body.theme-nerd-console .solopool-item.bch .solopool-hash{color:#8dff55;text-shadow:0 0 13px #65ff5688}body.theme-nerd-console .solopool-highlight{background:#08120a;border:1px solid #285c30;border-left:6px solid #62ff68;color:#eaffec}body.theme-nerd-console .solopool-highlight:nth-child(2){border-left-color:#a7ff3f}body.theme-nerd-console .solopool-highlight span{color:#83a789}body.theme-nerd-console .solopool-highlight b,body.theme-nerd-console .solopool-item.bch .solopool-highlight b{color:#a5ff59;text-shadow:0 0 8px #55ff4933}
body.theme-nerd-console .wallet-strip{background:linear-gradient(100deg,#071009,#102415);color:#eaffec;border-color:#397a42}body.theme-nerd-console .wallet-strip>b{color:#9dffa5}body.theme-nerd-console .wallet-item{color:#8cad91}body.theme-nerd-console .wallet-item b{color:#f2fff3}
body.theme-nerd-console .miner{color:#e8f5ea;background:linear-gradient(145deg,#071009 0 18%,#030704 18% 72%,#102415 72%);border-color:#397a42;border-top-color:#62ff68;box-shadow:inset 0 0 0 2px #142b18,0 6px 16px #000c}body.theme-nerd-console .miner.online{border-color:#397a42;border-top-color:#62ff68}body.theme-nerd-console .miner h3{color:#f3fff4;text-shadow:0 0 8px #5cff6833}body.theme-nerd-console .miner-head .sub,body.theme-nerd-console .miner .sub{color:#7fa486}body.theme-nerd-console .miner .status{background:#020503;color:#8cff96!important;border-color:#397a42}body.theme-nerd-console .miner .hash{font-family:Consolas,"Courier New",monospace;color:#061006;background:linear-gradient(180deg,#baff55,#68cf2d);-webkit-text-fill-color:#061006;border-color:#488a27;box-shadow:inset 0 0 12px #e8ff8a66,0 0 10px #6aff3c22;font-weight:900;letter-spacing:.025em}body.theme-nerd-console .miner .spark{background:#010402;border-color:#285c30}body.theme-nerd-console .miner .stats>div,body.theme-nerd-console .miner .stats>div:nth-child(1),body.theme-nerd-console .miner .stats>div:nth-child(2),body.theme-nerd-console .miner .stats>div:nth-child(4),body.theme-nerd-console .miner .stats>.current-share-stat{background:#08120a;border-color:#285c30;border-left-color:#62ff68}body.theme-nerd-console .miner .stats>div:nth-child(2){border-left-color:#8fcf65}body.theme-nerd-console .miner .stats>div:nth-child(4),body.theme-nerd-console .miner .stats>.current-share-stat{border-left-color:#dfff48}body.theme-nerd-console .miner .stats span{color:#7fa486}body.theme-nerd-console .miner .stats b{color:#effff0}body.theme-nerd-console .console-stat-icon{background:#17331c;color:#9dffa5}body.theme-nerd-console .miner>div:last-child small{color:#709078!important}
body.theme-nerd-console .metric{background:linear-gradient(160deg,#020503 0 58%,#102415 58%);border-color:#397a42}body.theme-nerd-console .metric:nth-child(n){border-top-color:#62ff68}body.theme-nerd-console .metric .label{color:#91ba97}body.theme-nerd-console .metric .label::before{background:#17331c;color:#a5ffad}body.theme-nerd-console .metric .value,body.theme-nerd-console .metric:nth-child(1) .value,body.theme-nerd-console .metric:nth-child(2) .value,body.theme-nerd-console .metric:nth-child(3) .value,body.theme-nerd-console .metric:nth-child(6) .value{color:#a5ff59;text-shadow:0 0 9px #62ff5544}
body.theme-nerd-console .block-strip{background:linear-gradient(150deg,#020503 0 65%,#102415 65%);border-color:#397a42}body.theme-nerd-console .block-strip:nth-child(n){border-top-color:#62ff68}body.theme-nerd-console .block-icon{background:#102415;color:#a5ff59;border-color:#397a42;box-shadow:0 0 0 3px #020503,0 0 14px #62ff6855}body.theme-nerd-console .block-height{color:#f3fff4}body.theme-nerd-console .block-meta span{background:#0b180d;color:#8cad91;border-left-color:#62ff68}body.theme-nerd-console .block-meta b,body.theme-nerd-console .market-price{color:#effff0!important}body.theme-nerd-console #blockDifficulty,body.theme-nerd-console #bchDifficulty{color:#a5ff59}body.theme-nerd-console .solo-odds{color:#8cad91;border-top-color:#285c30}body.theme-nerd-console .solo-odds b{color:#a5ff59}body.theme-nerd-console .stream{background:#020503;border-color:#397a42;border-left-color:#62ff68}
@media(max-width:720px){body.theme-nerd-console .solopool-strip{grid-template-columns:1fr 1fr;background:#030704}body.theme-nerd-console .solopool-title{grid-column:1/-1;color:#effff0;padding:4px}body.theme-nerd-console .solopool-console-main{grid-template-columns:44px 1fr}body.theme-nerd-console .pool-gauge{width:40px;height:40px}body.theme-nerd-console .pool-gauge span{font-size:9px}body.theme-nerd-console .solopool-hash{font-size:17px}body.theme-nerd-console .miner .stats{grid-template-columns:repeat(2,minmax(0,1fr))}body.theme-nerd-console .block-meta span:nth-child(5){display:block}body.theme-nerd-console .block-strip{min-height:0}}
.console-stat-icon{display:none}

@media(max-width:1100px){.metrics{grid-template-columns:repeat(3,1fr)}.chain-strips{grid-template-columns:repeat(2,minmax(0,1fr))}.grid{grid-template-columns:repeat(2,1fr)}}
@media(max-width:720px){
 body{overflow-x:hidden}
 .app{display:block}.sidebar{height:auto;position:sticky;z-index:15;display:block;padding:7px 9px;background:rgba(5,13,24,.94)}
 .brand{margin:0 4px 6px;font-size:18px}.nav{display:flex;gap:5px;overflow-x:auto;scrollbar-width:none;padding-bottom:2px}.nav::-webkit-scrollbar{display:none}
 .nav button{flex:0 0 auto;width:auto;margin:0;padding:7px 9px;font-size:11px;white-space:nowrap}.nav-sep{display:none}
 .main{padding:9px 8px 35px}.top{margin-bottom:8px;gap:8px}.top h1{font-size:19px}.top>div{display:flex;gap:5px}.top .pill,.top .btn{padding:7px 9px;font-size:11px}
 .health-banner{margin:6px 0 8px;padding:7px 9px}.metrics{grid-template-columns:repeat(3,minmax(0,1fr));gap:6px}
 .metrics>.layout-full,.chain-strips>.layout-full{grid-column:1/-1}.layout-size-btn{font-size:10px;padding:2px 5px}
 .metric{padding:9px 8px;min-height:78px}.metric .label{font-size:9px}.metric .value{font-size:17px;margin-top:4px;line-height:1.1}.metric .sub{font-size:9px;line-height:1.1}
 .chain-strips{grid-template-columns:repeat(2,minmax(0,1fr));gap:6px}.block-strip{padding:8px;grid-template-columns:1fr;gap:5px}.block-icon{width:30px;height:30px;font-size:17px}.block-height{font-size:13px}.block-hash{display:none}.block-meta{gap:4px 7px;font-size:8px}.block-meta span:nth-child(2),.block-meta span:nth-child(3),.block-meta span:nth-child(5){display:none}.solo-odds{font-size:10px;margin-top:6px;line-height:1.35}.solo-odds b{font-size:10px}.block-strip>.status{position:absolute;right:6px;top:6px;font-size:7px}.wallet-strip{padding:8px;gap:7px}.wallet-item{font-size:9px}.wallet-item b{font-size:11px}.market-price{font-size:11px}.price-change{font-size:8px}.solopool-strip{grid-template-columns:1fr 1fr;padding:8px;gap:7px}.solopool-title{grid-column:1/-1;font-size:11px}.solopool-item{font-size:8px;padding:8px}.solopool-hash{font-size:17px}.solopool-stats{gap:4px 7px}.solopool-highlights{gap:5px;margin-top:7px}.solopool-highlight{padding:6px}.solopool-highlight span{font-size:7px}.solopool-highlight b{font-size:15px}
 .stream{padding:9px;margin-top:8px}.stream-row{min-height:36px;gap:10px}.stream-item{min-width:42px;font-size:20px}
 .toolbar{gap:5px;margin:9px 0;overflow-x:auto;flex-wrap:nowrap;scrollbar-width:none}.toolbar::-webkit-scrollbar{display:none}.toolbar .pill{flex:0 0 auto;padding:7px 9px;font-size:10px}.toolbar .spacer{display:none}.sort-select{flex:0 0 auto;padding:7px;font-size:10px}.toolbar #onlineCount{flex:0 0 auto;font-size:10px;white-space:nowrap}
 .grid{grid-template-columns:repeat(2,minmax(0,1fr));gap:8px}.miner{padding:9px;min-width:0}.miner-head{display:block}.miner h3{font-size:14px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;padding-right:30px}.miner .status{font-size:9px;margin-top:4px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}.miner .sub{font-size:9px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}.miner .hash{font-size:19px;margin:7px 0 3px;white-space:nowrap}.miner .spark{height:21px;margin:1px 0 5px}.miner .spark svg{height:21px!important}.stats{grid-template-columns:repeat(2,minmax(0,1fr));gap:4px;padding-top:6px}.stats span{font-size:7px}.stats b{font-size:10px;overflow-wrap:anywhere}.stats>div:nth-child(3),.stats>div:nth-child(6),.stats>div:nth-child(7),.stats>div:nth-child(8){display:none}
 .miner.fleet-best::after{right:7px;top:-8px;padding:2px 5px;font-size:8px}.miner>div:last-child{margin-top:6px!important;display:block!important;text-align:right}.miner>div:last-child small{display:none}.miner>div:last-child .btn{padding:4px 5px!important;font-size:8px!important}
 .modal{padding:7px;align-items:flex-end}.dialog{padding:14px;width:100%!important;max-height:94dvh!important;border-radius:16px 16px 8px 8px!important}.dialog h2{font-size:20px}.row{display:grid;grid-template-columns:1fr}.actions{position:sticky;bottom:-14px;background:rgba(5,14,25,.96);padding:9px 0 14px;margin-top:12px}
 .detail-grid{grid-template-columns:repeat(3,minmax(0,1fr));gap:6px}.detail-stat{padding:8px}.detail-stat small{font-size:9px}.detail-stat b{font-size:13px}.chart-canvas{height:145px}.chart-wrap{padding:7px;margin-top:8px}
 .network-grid{grid-template-columns:repeat(2,1fr)}.hardware-grid{grid-template-columns:1fr}.emoji-picker{grid-template-columns:repeat(6,1fr)}.background-choices{grid-template-columns:repeat(2,1fr)}.background-choice{min-height:58px;padding:6px}.custom-preview{min-height:100px;padding:9px}.custom-preview-card{width:82%;min-height:70px;padding:9px}
 .event-table{display:block;overflow-x:auto}.toast{width:calc(100% - 20px);text-align:center}
}
@media(max-width:360px){.metrics{grid-template-columns:repeat(2,minmax(0,1fr))}.detail-grid{grid-template-columns:repeat(2,minmax(0,1fr))}.grid{gap:6px}.miner{padding:8px}.miner .hash{font-size:17px}}
</style>
</head>
<body>
<div class="toast" id="toast"></div>
<div class="app">
<aside class="sidebar">
  <div class="brand">◈ Rig<span>Pulse</span></div>
  <div class="nav">
    <button class="active">⌂ Dashboard</button><button onclick="document.getElementById('miners').scrollIntoView({behavior:'smooth'})">▣ Miners</button><button onclick="openHealth()">♢ Alerts</button>
    <button onclick="openHistory()">⌁ History</button><button onclick="openPools()">◎ Pools</button><button onclick="openNetwork()">₿ Network</button><div class="nav-sep"></div><button onclick="openCustomization()">⚙ Customization</button>
    <button onclick="openSettings()">☺ Emojis</button><button onclick="openLogs()">▤ Logs</button>
  </div>
</aside>
<main class="main">
  <div class="top"><h1>Fleet Dashboard</h1><div><button class="pill" id="menuToggleBtn" onclick="toggleSidebar()">☰ Hide Menu</button> <button class="pill" id="layoutToggleBtn" onclick="toggleLayoutMode()">↔ Arrange</button> <button class="pill" id="replayBlockBtn" onclick="replayLastBlock()" style="display:none">🎊 Replay Block</button> <button class="pill" onclick="openSettings()" id="emojiBtn">🎉 Emoji</button> <button class="btn primary" onclick="openAdd()">+ Add Miner</button></div></div>
  <div id="healthBanner" class="health-banner"><div class="health-dot">●</div><div><div class="health-title">Fleet Health</div><div class="health-detail">Checking miners…</div></div></div>
  <section class="metrics" id="summaryMetrics">
    <div class="card metric" id="metricSha"><div class="label">SHA-256 Hashrate</div><div class="value blue" id="shaHash">--</div><div class="sub" id="shaCount"></div></div>
    <div class="card metric" id="metricBlake"><div class="label">Blake3 / Alephium</div><div class="value purple" id="blakeHash">--</div><div class="sub" id="blakeCount"></div></div>
    <div class="card metric" id="metricShares"><div class="label">Shares Today</div><div class="value purple" id="sharesToday">--</div><div class="sub" id="sharesSession"></div></div>
    <div class="card metric" id="metricTemp"><div class="label">Avg Temperature</div><div class="value orange" id="temp">--°C</div><div class="sub" id="fleetOnline"></div></div>
    <div class="card metric" id="metricPower"><div class="label">Known Power</div><div class="value" id="power">--</div><div class="sub" id="unknownPower"></div></div>
    <div class="card metric" id="metricBest"><div class="label">Fleet Best Share</div><div class="value green" id="fleetBestShare">--</div><div class="sub" id="fleetBestMiner">No reported best share</div></div>
  </section>
  <div class="chain-strips" id="chainStrips"><section class="card block-strip" id="blockStrip">
    <div class="block-icon">₿</div>
    <div>
      <div class="block-main"><span class="block-height" id="blockHeight">Bitcoin block —</span><span class="block-hash" id="blockHash">waiting for network…</span></div>
      <div class="block-meta"><span>BTC Price <b class="market-price" id="btcPrice">—</b><i class="price-change" id="btcPriceChange"></i></span><span>Age <b id="blockAge">—</b></span><span>Transactions <b id="blockTx">—</b></span><span>Size <b id="blockSize">—</b></span><span>Difficulty <b id="blockDifficulty">—</b></span><span>Source <b id="blockSource">mempool.space</b></span></div><div class="solo-odds">Solo odds: <b id="btcSoloOdds">Configure hashrate</b> · Expected <b id="btcSoloExpected">—</b></div>
    </div>
    <div class="status green" id="blockState">● LIVE</div>
  </section><section class="card block-strip bch-strip" id="bchBlockStrip">
    <div class="block-icon">₿</div><div><div class="block-main"><span class="block-height" id="bchBlockHeight">BCH block —</span><span class="block-hash" id="bchBlockHash">waiting for network…</span></div><div class="block-meta"><span>BCH Price <b class="market-price" id="bchPrice">—</b><i class="price-change" id="bchPriceChange"></i></span><span>Updated <b id="bchBlockAge">—</b></span><span>24h Transactions <b id="bchBlockTx">—</b></span><span>Mempool <b id="bchMempool">—</b></span><span>Difficulty <b id="bchDifficulty">—</b></span><span>Source <b>Blockchair</b></span></div><div class="solo-odds">Solo odds: <b id="bchSoloOdds">Configure hashrate</b> · Expected <b id="bchSoloExpected">—</b></div></div><div class="status orange" id="bchBlockState">● WAITING</div>
  </section>
  <section class="card block-strip alph-strip" id="alphMarketBlock">
    <div class="block-icon">A</div><div><div class="block-main"><span class="block-height">Alephium</span><span class="block-hash">ALPH market</span></div><div class="block-meta"><span>ALPH Price <b class="market-price" id="alphPrice">—</b><i class="price-change" id="alphPriceChange"></i></span><span>Currency <b>USD</b></span><span>Source <b>CoinGecko</b></span></div></div><div class="status orange" id="alphPriceState">● WAITING</div>
  </section></div>
  <div class="dashboard-panels" id="dashboardPanels"><section class="card wallet-strip" id="walletStrip"><b>Public Wallet Balances</b><span class="wallet-item">BTC <b id="btcBalance">Not configured</b></span><span class="wallet-item">BCH <b id="bchBalance">Not configured</b></span></section>
  <section class="card solopool-strip" id="solopoolStrip"><div class="solopool-title">SoloPool Status</div><div class="solopool-item"><b>BTC POOL</b><div id="btcSoloPoolStatus"><div class="solopool-hash">—</div><div class="solopool-stats"><span>Not configured</span></div></div></div><div class="solopool-item bch"><b>BCH POOL</b><div id="bchSoloPoolStatus"><div class="solopool-hash">—</div><div class="solopool-stats"><span>Not configured</span></div></div></div></section>
  <section class="card stream" id="shareStream">
    <div class="stream-title"><b>🟢 Live Share Stream</b><small id="wsState">connecting…</small></div>
    <div class="stream-row" id="stream"><span style="color:#72859d">Waiting for submitted shares…</span></div>
  </section></div>
  <div class="toolbar">
    <button class="pill" onclick="filter='all';render()">All Miners</button>
    <button class="pill" onclick="filter='SHA-256';render()">SHA-256</button>
    <button class="pill" onclick="filter='Blake3';render()">Blake3 / Alephium</button>
    <button class="pill" onclick="filter='pool';render()">POOL Miners</button>
    <div class="spacer"></div><select class="sort-select" id="sortBy" onchange="sortBy=this.value;render()"><option value="name">Sort: Name</option><option value="hashrate">Sort: Hashrate</option><option value="temp">Sort: Temp</option><option value="power">Sort: Power</option><option value="efficiency">Sort: Efficiency</option></select><span id="onlineCount" style="color:#91a4bb"></span>
  </div>
  <section class="grid" id="miners"></section>
</main>
</div>

<div class="modal" id="addModal"><div class="dialog card">
<h2 id="minerModalTitle">Add Miner</h2>
<div class="field"><label>Name</label><input id="mName" placeholder="AL0-1"></div>
<div class="field"><label>IP address / hostname</label><input id="mIp" placeholder="192.168.0.123"></div>
<div class="row">
<div class="field"><label>Family</label><select id="mFamily">
<option value="auto">Auto detect</option><option value="avalon">Avalon / Nano (CGMiner)</option>
<option value="axeos">AxeOS / Nerd / Bitaxe</option><option value="luxos">Antminer / LuxOS</option>
<option value="iceriver">IceRiver</option><option value="goldshell">Goldshell</option>
</select></div>
<div class="field"><label>Algorithm</label><select id="mAlgo">
<option>SHA-256</option><option>Blake3</option><option>Scrypt</option>
</select></div></div>
<div class="field"><label>Model (optional)</label><input id="mModel" placeholder="IceRiver AL0"></div>
<div class="actions"><button class="btn" onclick="closeAdd()">Cancel</button><button class="btn primary" id="saveMinerBtn" onclick="saveMiner()">Add Miner</button></div>
</div></div>

<div class="modal" id="settingsModal"><div class="dialog card">
<h2>Celebration Settings</h2>
<div class="row"><div class="field"><label>SHA-256 emoji</label><input id="shareEmojiSha" value="₿" maxlength="16"></div><div class="field"><label>Blake3 / Alephium emoji</label><input id="shareEmojiBlake" value="⚡" maxlength="16"></div></div>
<div class="field"><label>Default / other algorithms</label><input id="shareEmojiDefault" value="🎉" maxlength="16"></div>
<div class="emoji-picker" id="emojiPicker">
<button onclick="pickEmoji('🎉')">🎉</button><button onclick="pickEmoji('🇺🇸')">🇺🇸</button><button onclick="pickEmoji('🪙')">🪙</button>
<button onclick="pickEmoji('₿')">₿</button><button onclick="pickEmoji('💰')">💰</button><button onclick="pickEmoji('💵')">💵</button>
<button onclick="pickEmoji('🚀')">🚀</button><button onclick="pickEmoji('🔥')">🔥</button><button onclick="pickEmoji('⚡')">⚡</button>
<button onclick="pickEmoji('⛏️')">⛏️</button><button onclick="pickEmoji('💎')">💎</button><button onclick="pickEmoji('🏆')">🏆</button>
<button onclick="pickEmoji('🟢')">🟢</button><button onclick="pickEmoji('⭐')">⭐</button><button onclick="pickEmoji('🐂')">🐂</button>
<button onclick="pickEmoji('🦅')">🦅</button><button onclick="pickEmoji('🌙')">🌙</button><button onclick="pickEmoji('🧱')">🧱</button>
</div>
<div style="color:#8da0b8;font-size:13px;margin-top:7px">Click a field, then pick a preset—or type/paste any emoji. Each algorithm can celebrate differently.</div>
<div class="field"><label>Animation amount (1–30)</label><input id="density" type="number" min="1" max="30" value="7"></div>
<div class="actions"><button class="btn" onclick="closeSettings()">Cancel</button><button class="btn primary" onclick="saveSettings()">Save</button></div>
</div></div>

<div class="modal" id="customModal"><div class="dialog card" style="width:min(760px,100%);max-height:90vh;overflow:auto">
<div style="display:flex;justify-content:space-between;align-items:center"><h2 style="margin:0">Dashboard Customization</h2><button class="btn" onclick="closeCustomization()">Close</button></div>
<div class="custom-preview" id="customPreview" style="margin-top:14px"><div class="custom-preview-card"><b>Miner Card</b><div class="sub">Glass + theme preview</div><div class="hash">38.2 TH/s</div></div></div>
<div class="field"><label>Dashboard background</label><div class="background-choices"><button type="button" class="background-choice bg-nerd" data-theme="nerd-console" onclick="chooseBackground('nerd-console')"><span>Nerd Console</span></button><button type="button" class="background-choice bg-mining" data-theme="mining-room" onclick="chooseBackground('mining-room')"><span>ASIC Mining Room</span></button><button type="button" class="background-choice bg-liquid" data-theme="liquid-lab" onclick="chooseBackground('liquid-lab')"><span>Liquid Cooling Lab</span></button><button type="button" class="background-choice bg-colors" data-theme="midnight" onclick="chooseBackground('midnight')"><span>Color Themes</span></button></div><select id="cTheme" style="margin-top:9px">
<option value="nerd-console">Nerd Console</option><option value="mining-room">ASIC Mining Room (Photo)</option><option value="liquid-lab">Liquid Cooling Lab (Photo)</option><option value="midnight">Midnight</option><option value="aurora">Aurora</option><option value="bitcoin">Bitcoin Orange</option><option value="matrix">Matrix</option><option value="ocean">Deep Ocean</option>
</select></div>
<div class="field"><label>Miner/card transparency</label><div class="range-row"><input id="cOpacity" type="range" min="0.30" max="1" step="0.02"><span id="cOpacityVal">82%</span></div></div>
<div class="field"><label>Glass blur</label><div class="range-row"><input id="cBlur" type="range" min="0" max="30" step="1"><span id="cBlurVal">14px</span></div></div>
<div class="field"><label>Background intensity</label><div class="range-row"><input id="cIntensity" type="range" min="0.2" max="2" step="0.1"><span id="cIntensityVal">1.0x</span></div></div>
<label style="display:flex;gap:8px;align-items:center;margin-top:12px"><input id="cCompact" type="checkbox"> Compact miner cards</label>
<div class="field"><label>Bitcoin block API base URL</label><input id="cBlockApi" value="https://mempool.space/api"><div class="sub">Default uses mempool.space. Later this can point at a compatible local Mempool/Bitcoin service on Umbrel.</div></div>
<div class="row"><div class="field"><label>BTC public wallet address (optional)</label><input id="cBtcWallet" placeholder="bc1…"><div class="sub">Watch-only balance. Never enter a seed phrase or private key.</div></div><div class="field"><label>BCH public wallet address (optional)</label><input id="cBchWallet" placeholder="bitcoincash:q…"><div class="sub">Watch-only balance. Never enter a seed phrase or private key.</div></div></div>
<div class="row"><div class="field"><label>BTC SoloPool mining address (optional)</label><input id="cBtcSoloPool" placeholder="bc1…"><div class="sub">Loads public BTC SoloPool miner status.</div></div><div class="field"><label>BCH SoloPool mining address (optional)</label><input id="cBchSoloPool" placeholder="q…"><div class="sub">Loads public status and enables pool-side block detection.</div></div></div>
<div class="row"><div class="field"><label>BTC solo-mining hashrate</label><div style="display:grid;grid-template-columns:1fr 90px;gap:7px"><input id="cBtcSoloHash" type="number" min="0" step="any" placeholder="476"><select id="cBtcSoloUnit"><option value="TH">TH/s</option><option value="PH">PH/s</option></select></div><div class="sub">Used only for probability calculations.</div></div><div class="field"><label>BCH solo-mining hashrate</label><div style="display:grid;grid-template-columns:1fr 90px;gap:7px"><input id="cBchSoloHash" type="number" min="0" step="any" placeholder="476"><select id="cBchSoloUnit"><option value="TH">TH/s</option><option value="PH">PH/s</option></select></div><div class="sub">Enter the hashrate actually pointed at BCH.</div></div></div>
<div class="actions"><button class="btn" onclick="resetDashboardLayout()">Reset Box Layout</button><button class="btn" onclick="resetCustomization()">Reset Theme</button><button class="btn primary" onclick="saveCustomization()">Save Customization</button></div>
</div></div>

<div class="modal block-party" id="blockPartyModal"><div class="dialog card block-party-panel"><div class="block-party-title">BLOCK FOUND!</div><div class="block-party-miner" id="blockPartyMiner">Your miner found a block</div><div class="sub" id="blockPartyDetails" style="margin-top:8px"></div><div class="actions"><button class="btn" onclick="closeBlockParty()">Close</button><button class="btn primary" onclick="replayLastBlock()">Replay Party</button></div></div></div>

<div class="modal" id="diagModal"><div class="dialog card" style="width:min(900px,100%);max-height:85vh;overflow:auto">
<h2>Miner Diagnostics</h2>
<div style="color:#8da0b8;font-size:13px;margin-bottom:10px">Read-only API discovery. Copy this output back into ChatGPT so we can build the exact adapter.</div>
<pre id="diagOut" style="white-space:pre-wrap;word-break:break-word;background:#050b14;border:1px solid #20334a;border-radius:10px;padding:12px;max-height:55vh;overflow:auto;color:#cfe4f7"></pre>
<div class="actions"><button class="btn" onclick="copyDiag()">Copy</button><button class="btn primary" onclick="closeDiag()">Close</button></div>
</div></div>


<div class="modal" id="detailModal"><div class="dialog card" style="width:min(1100px,100%);max-height:92vh;overflow:auto"><div style="display:flex;justify-content:space-between;align-items:center"><div><div class="sub">Miner Detail</div><h2 id="detailTitle" style="margin:2px 0 0"></h2></div><button class="btn" onclick="closeDetail()">Close</button></div><div class="tabs" style="margin-top:12px"><button class="pill" onclick="loadDetailHistory(900)">15m</button><button class="pill" onclick="loadDetailHistory(3600)">1h</button><button class="pill" onclick="loadDetailHistory(86400)">24h</button><button class="pill" onclick="loadDetailHistory(604800)">7d</button></div><div class="detail-grid" id="detailStats" style="margin-top:12px"></div><div class="detail-tabs"><button class="pill detail-tab active" onclick="showDetailSection('charts',this)">Charts</button><button class="pill detail-tab" onclick="showDetailSection('hardware',this)">Hardware</button><button class="pill detail-tab" onclick="showDetailSection('events',this)">Events</button></div>
<div id="detailHardware" style="display:none"></div><div id="detailCharts"><div class="chart-wrap"><b>Hashrate</b><canvas id="hashChart" class="chart-canvas"></canvas></div><div class="chart-wrap"><b>Temperature</b><canvas id="tempChart" class="chart-canvas"></canvas></div><div class="chart-wrap"><b>Power</b><canvas id="powerChart" class="chart-canvas"></canvas></div></div><div id="detailEventsWrap" style="display:none"><div class="section-title">Recent Events</div><div id="detailEvents"></div></div><div class="section-title">Troubleshooting</div><div class="tabs"><button class="btn" onclick="probe(selectedMinerId)">Probe API</button><button class="btn" onclick="diagnose(selectedMinerId)">Run Diagnostics</button><button class="btn" id="detailIceRaw" onclick="iceRaw(selectedMinerId)">IceRiver Raw</button></div></div></div>
<div class="modal" id="logsModal"><div class="dialog card" style="width:min(1100px,100%);max-height:90vh;overflow:auto">
<div style="display:flex;justify-content:space-between;align-items:center"><div><h2 style="margin:0">Event Logs</h2><div class="sub">Shares, best shares, miner state, alerts and new blocks</div></div><button class="btn" onclick="closeLogs()">Close</button></div>
<div class="log-toolbar"><span class="log-badge" id="logCount">0 events</span><button class="btn" onclick="loadLogs()">Refresh</button></div><div id="fleetEvents" style="margin-top:8px"></div></div></div>

<div class="modal" id="historyModal"><div class="dialog card" style="width:min(1150px,100%);max-height:92vh;overflow:auto">
<div style="display:flex;justify-content:space-between;align-items:center"><div><h2 style="margin:0">Fleet History</h2><div class="sub">Real samples stored by RigPulse</div></div><button class="btn" onclick="closeHistory()">Close</button></div>
<div class="history-toolbar"><button class="pill" onclick="loadFleetHistory(3600)">1h</button><button class="pill" onclick="loadFleetHistory(21600)">6h</button><button class="pill" onclick="loadFleetHistory(86400)">24h</button><button class="pill" onclick="loadFleetHistory(604800)">7d</button><span class="sub" id="historyRange"></span></div>
<div id="fleetHistory" class="history-miners"></div></div></div>

<div class="modal" id="poolsModal"><div class="dialog card" style="width:min(1050px,100%);max-height:90vh;overflow:auto">
<div style="display:flex;justify-content:space-between;align-items:center"><div><h2 style="margin:0">Mining Pools</h2><div class="sub">Pool status reported directly by each miner</div></div><button class="btn" onclick="closePools()">Close</button></div>
<div id="poolSummary" class="sub" style="margin-top:10px"></div><div id="poolCards" class="pool-grid"></div></div></div>

<div class="modal" id="networkModal"><div class="dialog card" style="width:min(1100px,100%);max-height:92vh;overflow:auto">
<div style="display:flex;justify-content:space-between;align-items:center"><div><h2 style="margin:0">Bitcoin Network</h2><div class="sub">Live network context plus your fleet's best-share progress</div></div><button class="btn" onclick="closeNetwork()">Close</button></div>
<div id="networkCards" class="network-grid"></div>
<div class="section-title">Best Share Leaderboard</div>
<div id="bestLeaderboard"></div>
</div></div>

<div class="modal" id="healthModal"><div class="dialog card" style="width:min(900px,100%);max-height:90vh;overflow:auto">
<div style="display:flex;justify-content:space-between;align-items:center"><h2 style="margin:0">Fleet Health & Alerts</h2><div><button class="btn" onclick="openAlertSettings()">Thresholds</button> <button class="btn" onclick="$('healthModal').classList.remove('show')">Close</button></div></div>
<div id="healthDetails" class="health-list"></div>
</div></div>

<div class="modal" id="alertModal"><div class="dialog card" style="width:min(720px,100%)">
<div style="display:flex;justify-content:space-between;align-items:center"><h2 style="margin:0">Alert Settings</h2><button class="btn" onclick="$('alertModal').classList.remove('show')">Close</button></div>
<div class="alert-grid" style="margin-top:14px">
 <div class="field"><label>Temperature warning (°C)</label><input id="aTemp" type="number" min="1" max="150"></div>
 <div class="field"><label>Hashrate drop (%)</label><input id="aHash" type="number" min="1" max="99"></div>
 <div class="field"><label>Reject-rate warning (%)</label><input id="aReject" type="number" min="0" max="100" step="0.1"></div>
 <div class="field"><label>Offline delay (seconds)</label><input id="aOffline" type="number" min="10" max="3600"></div>
</div>
<label style="display:flex;gap:8px;align-items:center;margin-top:14px"><input id="aPool" type="checkbox"> Alert when pool reports disconnected</label>
<div style="display:flex;justify-content:space-between;align-items:center;margin-top:16px">
 <button class="btn" onclick="downloadConfig()">Export Miner Config</button>
 <button class="btn primary" onclick="saveAlertSettings()">Save Settings</button>
</div>
</div></div>
<script>
let miners=[], settings={share_emoji:"🎉",share_emoji_sha256:"🎉",share_emoji_blake3:"🎉",share_emoji_default:"🎉",animation_density:7}, customization={theme:"midnight",card_opacity:.82,blur_px:14,background_intensity:1,compact_cards:false,block_api_base:"https://mempool.space/api",btc_wallet_address:"",bch_wallet_address:"",btc_solopool_address:"",bch_solopool_address:"",btc_solo_hashrate:0,btc_solo_hashrate_unit:"TH",bch_solo_hashrate:0,bch_solo_hashrate_unit:"TH"}, filter='all', editingMinerId=null, sortBy='name', selectedMinerId=null, activeEmojiField='shareEmojiDefault', fleetBestMinerId=null, sparkCache=new Map(), lastBlockFound=null;
const LAYOUT_KEY='rigpulse-dashboard-layout-v1',SIDEBAR_KEY='rigpulse-sidebar-hidden';let layoutEditing=false,draggedLayoutBox=null,defaultLayout={};
function toggleSidebar(force){const hidden=force??!document.body.classList.contains('sidebar-hidden');document.body.classList.toggle('sidebar-hidden',hidden);localStorage.setItem(SIDEBAR_KEY,hidden?'1':'0');$('menuToggleBtn').textContent=hidden?'☰ Show Menu':'☰ Hide Menu'}
function layoutContainers(){return ['summaryMetrics','chainStrips','dashboardPanels'].map($).filter(Boolean)}
function saveDashboardLayout(){const state={order:{},sizes:{}};for(const c of layoutContainers()){state.order[c.id]=[...c.children].map(x=>x.id);for(const box of c.children)state.sizes[box.id]=box.classList.contains('layout-full')?'full':box.classList.contains('layout-wide')?'wide':'normal'}localStorage.setItem(LAYOUT_KEY,JSON.stringify(state))}
function cycleBoxSize(box){const next=box.classList.contains('layout-wide')?'full':box.classList.contains('layout-full')?'normal':'wide';box.classList.toggle('layout-wide',next==='wide');box.classList.toggle('layout-full',next==='full');saveDashboardLayout();toast(`Box size: ${next}`)}
function setupDashboardLayout(){for(const c of layoutContainers()){defaultLayout[c.id]=[...c.children].map(x=>x.id);for(const box of c.children){box.classList.add('layout-box');box.draggable=false;if(c.id!=='dashboardPanels'&&!box.querySelector('.layout-size-btn')){const b=document.createElement('button');b.className='layout-size-btn';b.type='button';b.title='Cycle box width: normal, wide, full';b.textContent='↔ Resize';b.onclick=e=>{e.stopPropagation();cycleBoxSize(box)};box.appendChild(b)}box.addEventListener('dragstart',()=>{if(!layoutEditing)return;draggedLayoutBox=box;box.classList.add('dragging')});box.addEventListener('dragend',()=>{box.classList.remove('dragging');draggedLayoutBox=null;saveDashboardLayout()})}c.addEventListener('dragover',e=>{if(!layoutEditing||!draggedLayoutBox||draggedLayoutBox.parentElement!==c)return;e.preventDefault();const target=e.target.closest('.layout-box');if(!target||target===draggedLayoutBox||target.parentElement!==c)return;const r=target.getBoundingClientRect(),before=e.clientY<r.top+r.height/2&&(Math.abs(e.clientY-(r.top+r.height/2))>r.height*.2||e.clientX<r.left+r.width/2);c.insertBefore(draggedLayoutBox,before?target:target.nextSibling)})}try{const state=JSON.parse(localStorage.getItem(LAYOUT_KEY)||'{}');for(const c of layoutContainers()){for(const id of state.order?.[c.id]||[]){const box=$(id);if(box&&box.parentElement===c)c.appendChild(box)}for(const box of c.children){const size=state.sizes?.[box.id];box.classList.toggle('layout-wide',size==='wide');box.classList.toggle('layout-full',size==='full')}}}catch(e){}}
function toggleLayoutMode(){layoutEditing=!layoutEditing;document.body.classList.toggle('layout-edit',layoutEditing);for(const c of layoutContainers())for(const box of c.children)box.draggable=layoutEditing;$('layoutToggleBtn').textContent=layoutEditing?'✓ Finish Layout':'↔ Arrange';toast(layoutEditing?'Drag boxes to rearrange; use Resize to change width':'Dashboard layout saved')}
function resetDashboardLayout(){localStorage.removeItem(LAYOUT_KEY);for(const c of layoutContainers()){for(const id of defaultLayout[c.id]||[]){const box=$(id);if(box)c.appendChild(box)}for(const box of c.children)box.classList.remove('layout-wide','layout-full')}toast('Dashboard box layout reset')}
const $=id=>document.getElementById(id);
function fmt(v,d=1){return v==null?'--':Number(v).toFixed(d)}

function shareNumber(v){if(v==null||v==='')return null;let s=String(v).trim().replace(/,/g,''),m=s.match(/^(-?\d+(?:\.\d+)?)\s*([kKmMgGtTpPeE]?)$/);if(!m){let n=Number(s);return Number.isFinite(n)?n:null}let mult={'':1,K:1e3,M:1e6,G:1e9,T:1e12,P:1e15,E:1e18}[m[2].toUpperCase()]||1;return Number(m[1])*mult}
function fmtShare(v){let n=shareNumber(v);if(n==null)return'--';let a=Math.abs(n),units=[['E',1e18],['P',1e15],['T',1e12],['G',1e9],['M',1e6],['K',1e3]];for(const [u,m] of units)if(a>=m){let x=n/m;return x.toFixed(2).replace(/0$/,'')+u}return n.toFixed(1)}

function canonicalHash(t,algorithm){
 if(!t||t.hashrate==null)return {value:null,unit:''};
 let v=Number(t.hashrate),u=String(t.hashrate_unit||'').toUpperCase();
 if(algorithm==='SHA-256'){
  if(u.startsWith('MH'))v/=1000000; else if(u.startsWith('GH'))v/=1000; else if(u.startsWith('PH'))v*=1000;
  return {value:v,unit:'TH/s'};
 }
 if(algorithm==='Blake3'){
  if(u.startsWith('MH'))v/=1000; else if(u.startsWith('TH'))v*=1000;
  return {value:v,unit:'GH/s'};
 }
 if(algorithm==='Scrypt'){
  if(u.startsWith('GH'))v*=1000; else if(u.startsWith('TH'))v*=1000000;
  return {value:v,unit:'MH/s'};
 }
 return {value:v,unit:t.hashrate_unit||''};
}
function hashText(t,algorithm){let h=canonicalHash(t,algorithm);return h.value!=null?fmt(h.value)+' '+h.unit:'--'}
function rawv(t,k){return t?.raw?.full?.[k] ?? t?.raw?.[k] ?? null}
function uptime(s){if(!s)return'--';let d=Math.floor(s/86400),h=Math.floor((s%86400)/3600);return d?`${d}d ${h}h`:`${h}h`}
function drawLineChart(id,rows,field){const c=$(id);if(!c)return;const dpr=window.devicePixelRatio||1,rect=c.getBoundingClientRect();c.width=Math.max(300,rect.width*dpr);c.height=Math.max(160,rect.height*dpr);const x=c.getContext('2d');x.scale(dpr,dpr);const w=rect.width,h=rect.height;x.clearRect(0,0,w,h);const vals=rows.map(r=>r[field]).filter(v=>v!=null&&Number.isFinite(Number(v))).map(Number);if(vals.length<2){x.fillStyle='#8192a7';x.font='13px system-ui';x.fillText('Not enough data yet',12,24);return}let mn=Math.min(...vals),mx=Math.max(...vals);if(mx===mn)mx=mn+1;const p=24;x.strokeStyle='#20334a';for(let i=0;i<4;i++){let y=p+(h-p*2)*(i/3);x.beginPath();x.moveTo(p,y);x.lineTo(w-p,y);x.stroke()}x.strokeStyle='#38b6ff';x.lineWidth=2;x.beginPath();let z=0;rows.forEach((r,i)=>{if(r[field]==null)return;let xx=p+(w-p*2)*(i/Math.max(1,rows.length-1)),yy=h-p-(Number(r[field])-mn)/(mx-mn)*(h-p*2);if(z++===0)x.moveTo(xx,yy);else x.lineTo(xx,yy)});x.stroke();x.fillStyle='#8192a7';x.font='11px system-ui';x.fillText(mx.toFixed(1),2,14);x.fillText(mn.toFixed(1),2,h-4)}
function spark(){return `<svg class="spark" viewBox="0 0 195 45" preserveAspectRatio="none"><polyline fill="none" stroke="#38b6ff" stroke-width="1.5" points="0,30 195,30"/></svg>`}
async function load(){
 [miners,settings,customization]=await Promise.all([
   fetch('/api/miners').then(r=>r.json()),
   fetch('/api/settings').then(r=>r.json()),
   fetch('/api/customization').then(r=>r.json())
 ]);
 $('emojiBtn').textContent=settings.share_emoji+' Emoji';
 applyCustomization(customization);
 render();
 loadBlockStatus();
}
async function renderPoolMiners(){const grid=$('miners');if(!grid.querySelector('.pool-miner-card')&&!grid.querySelector('.pool-empty'))grid.innerHTML='<div class="sub pool-loading">Loading SoloPool workers…</div>';try{const d=await fetch('/api/chain-status').then(r=>r.json()),sp=d.solopool||{},workers=[];if(filter!=='pool')return;for(const coin of ['btc','bch'])for(const w of(sp[coin]?.workers||[]))workers.push({...w,coin});workers.sort((a,b)=>String(a.name).localeCompare(String(b.name)));grid.querySelector('.pool-loading')?.remove();const existing=new Map([...grid.querySelectorAll('.pool-miner-card')].map(card=>[card.dataset.workerKey,card])),active=new Set();for(const w of workers){const key=`${w.coin}:${w.name}`,on=!w.offline&&!w.dead;active.add(key);let card=existing.get(key);if(!card){card=document.createElement('article');card.className='card miner pool-miner-card';card.innerHTML='<div class="miner-head"><div><h3 class="pool-worker-name"></h3><div class="sub pool-worker-sub"></div></div><div class="status"></div></div><div class="hash"></div><div class="sub pool-worker-detail"></div><div class="stats"><div><span>Valid Shares</span><b data-field="valid"></b></div><div><span>Invalid</span><b data-field="invalid"></b></div><div><span>Stale</span><b data-field="stale"></b></div><div><span>Port</span><b data-field="port"></b></div><div><span>Region</span><b data-field="region"></b></div><div><span>Coin</span><b data-field="coin"></b></div></div>';card.dataset.workerKey=key}card.className=`card miner pool-miner-card ${w.coin} ${on?'online':'offline'}`;card.querySelector('.pool-worker-name').textContent=w.name;card.querySelector('.pool-worker-sub').textContent=`${w.coin.toUpperCase()} SoloPool · ${w.geo||'Unknown region'}`;const status=card.querySelector('.status');status.className=`status ${on?'green':'red'}`;status.textContent=`● ${on?'Online':'Offline'}`;card.querySelector('.hash').textContent=hashFromHs(w.hr);card.querySelector('.pool-worker-detail').textContent=`Current hashrate · Average: ${hashFromHs(w.avgHr)} · Last share: ${blockAge(w.lastShare)}`;card.querySelector('[data-field="valid"]').textContent=Number(w.validShares||0).toLocaleString();card.querySelector('[data-field="invalid"]').textContent=Number(w.invalidShares||0).toLocaleString();card.querySelector('[data-field="stale"]').textContent=Number(w.staleShares||0).toLocaleString();card.querySelector('[data-field="port"]').textContent=w.port??'--';card.querySelector('[data-field="region"]').textContent=w.geo||'--';card.querySelector('[data-field="coin"]').textContent=w.coin.toUpperCase();grid.appendChild(card)}for(const [key,card] of existing)if(!active.has(key))card.remove();grid.querySelector('.pool-empty')?.remove();if(!workers.length){const empty=document.createElement('div');empty.className='sub pool-empty';empty.textContent='No SoloPool workers found. Add a BTC or BCH SoloPool address in Customization.';grid.appendChild(empty)}$('onlineCount').textContent=`${workers.filter(w=>!w.offline&&!w.dead).length} / ${workers.length} pool miners online`;loadFleetSummary();refreshHealth()}catch(e){if(filter==='pool'&&!grid.querySelector('.pool-miner-card'))grid.innerHTML=`<div class="sub pool-empty">Could not load SoloPool workers: ${esc(String(e))}</div>`}}
function render(){if(filter==='pool'){renderPoolMiners();return}let shown=miners.filter(m=>filter==='all'||m.algorithm===filter);const ss={name:(a,b)=>String(a.name).localeCompare(String(b.name)),hashrate:(a,b)=>(b.telemetry?.hashrate??-1)-(a.telemetry?.hashrate??-1),temp:(a,b)=>(b.telemetry?.temp_c??-1)-(a.telemetry?.temp_c??-1),power:(a,b)=>(b.telemetry?.power_w??-1)-(a.telemetry?.power_w??-1),efficiency:(a,b)=>(a.telemetry?.efficiency??999999)-(b.telemetry?.efficiency??999999)};shown.sort(ss[sortBy]||ss.name);let out='',online=miners.filter(m=>m.telemetry?.online).length;for(const m of shown){let t=m.telemetry||{},on=!!t.online;out+=`<article class="card miner ${on?'online':'offline'} clickable" onclick="openDetail(${m.id})"><div class="miner-head"><div><h3>${esc(m.name)}</h3><div class="sub">${esc(m.model||m.family)} · ${esc(m.algorithm)}</div></div><div class="status ${on?'green':t.reachable?'orange':'red'}">● ${on?'Online':t.reachable?'Reachable / No Telemetry':'Offline'}</div></div><div class="hash">${hashText(t,m.algorithm)}</div>${t.avg_hashrate!=null?`<div class="sub">Avg: ${fmt(t.avg_hashrate)} ${t.avg_hashrate_unit||t.hashrate_unit||''}</div>`:''}<div class="sub">Today: ${m.shares_today??'--'} · Session: ${m.shares_session??'--'}</div><div class="spark" id="spark-${m.id}"></div><div class="stats"><div><span>Max Temp</span><b>${t.temp_c!=null?fmt(t.temp_c,0)+'°C':'--'}</b></div><div><span>Power</span><b>${t.power_w!=null?fmt(t.power_w,0)+' W':'--'}</b></div><div><span>Lifetime</span><b>${m.shares_lifetime??'--'}</b></div><div><span>Best</span><b>${fmtShare(t.best_share)}</b></div><div><span>Efficiency</span><b>${t.efficiency!=null?fmt(t.efficiency,1)+' '+(t.efficiency_unit||''):'--'}</b></div><div><span>Reject %</span><b>${m.reject_pct!=null?fmt(m.reject_pct,3)+'%':'--'}</b></div><div><span>Uptime</span><b>${t.uptime_s!=null?uptime(t.uptime_s):'--'}</b></div><div><span>Pool</span><b class="${t.pool_alive===true?'green':t.pool_alive===false?'red':''}">${t.pool_alive===true?'Alive':t.pool_alive===false?'Down':'--'}</b></div></div>${m.family==='iceriver'&&t.raw?`<div class="sub" style="margin-top:10px">Board sensors: ${Array.isArray(t.raw.inlet_temps)&&t.raw.inlet_temps.length?t.raw.inlet_temps.join('/')+'°C':'--'} / ${Array.isArray(t.raw.outlet_temps)&&t.raw.outlet_temps.length?t.raw.outlet_temps.join('/')+'°C':'--'} · HW errors: ${t.raw.hardware_errors??'--'}</div>`:''}<div style="display:flex;justify-content:space-between;margin-top:12px" onclick="event.stopPropagation()"><small style="color:#7f91a7">${esc(m.ip)}</small><div><button class="btn" style="padding:5px 8px;font-size:11px" onclick="probe(${m.id})">Probe</button> <button class="btn" style="padding:5px 8px;font-size:11px" onclick="diagnose(${m.id})">Diagnose</button> ${m.family==='iceriver'?`<button class="btn" style="padding:5px 8px;font-size:11px" onclick="iceRaw(${m.id})">IceRiver Raw</button>`:''} <button class="btn" style="padding:5px 8px;font-size:11px" onclick="editMiner(${m.id})">Edit</button> <button class="btn danger" style="padding:5px 8px;font-size:11px" onclick="deleteMiner(${m.id})">Delete</button></div></div></article>`}$('miners').innerHTML=out||'<div class="sub">No miners yet.</div>';$('onlineCount').textContent=`${online} / ${miners.length} online`;loadFleetSummary(); refreshHealth();shown.forEach(m=>loadMiniSpark(m.id))}
function esc(s){return String(s??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]))}
function openAdd(){
 editingMinerId=null;
 $('minerModalTitle').textContent='Add Miner'; $('saveMinerBtn').textContent='Add Miner';
 $('mName').value='';$('mIp').value='';$('mFamily').value='auto';$('mAlgo').value='SHA-256';$('mModel').value='';
 $('addModal').classList.add('show')
}
function closeAdd(){$('addModal').classList.remove('show')}
function editMiner(id){
 let m=miners.find(x=>x.id===id); if(!m)return;
 editingMinerId=id;
 $('minerModalTitle').textContent='Edit Miner'; $('saveMinerBtn').textContent='Save Changes';
 $('mName').value=m.name||'';$('mIp').value=m.ip||'';$('mFamily').value=m.family||'auto';$('mAlgo').value=m.algorithm||'SHA-256';$('mModel').value=m.model||'';
 $('addModal').classList.add('show')
}
async function deleteMiner(id){
 let m=miners.find(x=>x.id===id); if(!m)return;
 if(!confirm(`Delete ${m.name}? This also removes its stored history.`))return;
 let r=await fetch(`/api/miners/${id}`,{method:'DELETE'});
 if(!r.ok)return toast(await r.text());
 await load(); toast('Miner deleted')
}
async function saveMiner(){
 let body={name:$('mName').value.trim(),ip:$('mIp').value.trim(),family:$('mFamily').value,algorithm:$('mAlgo').value,model:$('mModel').value.trim()};
 if(!body.name||!body.ip)return toast('Name and IP are required');
 let url=editingMinerId?`/api/miners/${editingMinerId}`:'/api/miners';
 let method=editingMinerId?'PUT':'POST';
 let r=await fetch(url,{method,headers:{'content-type':'application/json'},body:JSON.stringify(body)});
 if(!r.ok)return toast(await r.text());
 let msg=editingMinerId?'Miner updated':'Miner added';
 closeAdd(); editingMinerId=null; await load(); toast(msg);
}
async function probe(id){let r=await fetch(`/api/miners/${id}/probe`,{method:'POST'});let x=await r.json();toast(x.online?'Miner API responded':'No supported API response yet');await load()}
async function responseData(r){const text=await r.text();if(!r.ok)throw new Error(text||`Server returned HTTP ${r.status}`);try{return JSON.parse(text)}catch(e){throw new Error(text||'Server returned an invalid response')}}
async function iceRaw(id){
 $('diagOut').textContent='POSTing /user/userpanel with post=4…';
 $('diagModal').classList.add('show');
 try{
   let r=await fetch(`/api/miners/${id}/iceriver-post4-diagnostic`);
   let x=await responseData(r);
   $('diagOut').textContent=JSON.stringify(x,null,2);
 }catch(e){$('diagOut').textContent='IceRiver diagnostic failed: '+e}
}
async function diagnose(id){
 $('diagOut').textContent='Scanning read-only local API endpoints…\nThis normally takes a few seconds.';
 $('diagModal').classList.add('show');
 let timer=setTimeout(()=>{$('diagOut').textContent+='\\nStill scanning — the miner may be ignoring some connection attempts.'},5000);
 try{
   let r=await fetch(`/api/miners/${id}/diagnostics`);
   clearTimeout(timer);
   let x=await responseData(r);
   $('diagOut').textContent=JSON.stringify({miner:x.miner,useful_count:x.useful_count,useful:x.useful,discovered_endpoints:x.discovered_endpoints||[],endpoint_results:x.endpoint_results||[],js_hints:x.js_hints||[]},null,2);
 }catch(e){clearTimeout(timer);$('diagOut').textContent='Diagnostics failed: '+e}
}
function closeDiag(){$('diagModal').classList.remove('show')}
document.addEventListener('click',e=>{
 const modal=e.target;
 if(!modal.classList?.contains('modal')||!modal.classList.contains('show'))return;
 modal.classList.remove('show');
 if(modal.id==='detailModal')selectedMinerId=null;
 if(modal.id==='addModal')editingMinerId=null;
});
async function copyDiag(){
 try{await navigator.clipboard.writeText($('diagOut').textContent);toast('Diagnostics copied')}
 catch(e){toast('Copy failed — select the text manually')}
}

function pickEmoji(e){$(activeEmojiField).value=e}
['shareEmojiSha','shareEmojiBlake','shareEmojiDefault'].forEach(id=>document.addEventListener('focusin',e=>{if(e.target?.id===id)activeEmojiField=id}));
function openSettings(){$('settingsModal').classList.add('show');$('shareEmojiSha').value=settings.share_emoji_sha256||settings.share_emoji;$('shareEmojiBlake').value=settings.share_emoji_blake3||settings.share_emoji;$('shareEmojiDefault').value=settings.share_emoji_default||settings.share_emoji;$('density').value=settings.animation_density}
function closeSettings(){$('settingsModal').classList.remove('show')}
async function saveSettings(){
 let fallback=$('shareEmojiDefault').value||'🎉';let body={share_emoji:fallback,share_emoji_sha256:$('shareEmojiSha').value||fallback,share_emoji_blake3:$('shareEmojiBlake').value||fallback,share_emoji_default:fallback,animation_density:Number($('density').value)||7,celebrate_rejected:false};
 settings=await fetch('/api/settings',{method:'PUT',headers:{'content-type':'application/json'},body:JSON.stringify(body)}).then(r=>r.json());closeSettings();$('emojiBtn').textContent=settings.share_emoji+' Emoji';toast('Celebration saved')
}
function toast(s){$('toast').textContent=s;$('toast').style.display='block';setTimeout(()=>$('toast').style.display='none',2200)}
function emojiForAlgorithm(algorithm){if(algorithm==='SHA-256')return settings.share_emoji_sha256||settings.share_emoji;if(algorithm==='Blake3')return settings.share_emoji_blake3||settings.share_emoji;return settings.share_emoji_default||settings.share_emoji||'🎉'}
function celebrate(name,count=1,type='share',algorithm=''){
 algorithm=algorithm||miners.find(m=>m.name===name)?.algorithm||'';let chosen=emojiForAlgorithm(algorithm);let n=Math.min(30,Math.max(settings.animation_density,count)); for(let i=0;i<n;i++){setTimeout(()=>{let e=document.createElement('div');e.className='celebrate';e.textContent=type==='best_share'?'🏆':chosen;
 e.style.left=(5+Math.random()*90)+'vw';e.style.top=(-10-Math.random()*20)+'px';document.body.appendChild(e);setTimeout(()=>e.remove(),2400)},i*75)}
 toast(type==='best_share'?`🏆 New best share — ${name}`:`${chosen} ${name} submitted ${count>1?count+' shares':'a share'}`);
 let row=$('stream'); if(row.children.length===1&&row.textContent.includes('Waiting'))row.innerHTML='';
 let item=document.createElement('div');item.className='stream-item';item.innerHTML=`${type==='best_share'?'🏆':chosen}<small>${esc(name)}</small>`;row.prepend(item);while(row.children.length>12)row.lastChild.remove();
}
async function loadFleetSummary(){try{let s=await fetch('/api/fleet-summary').then(r=>r.json()),sha=s.algorithms?.['SHA-256'],bl=s.algorithms?.['Blake3'];$('shaHash').textContent=sha&&sha.hashrate?fmt(sha.hashrate)+' '+(sha.unit||''):'--';$('shaCount').textContent=sha?`${sha.online}/${sha.count} online`:'0 miners';$('blakeHash').textContent=bl&&bl.hashrate?fmt(bl.hashrate)+' '+(bl.unit||''):'--';$('blakeCount').textContent=bl?`${bl.online}/${bl.count} online`:'0 miners';$('sharesToday').textContent=s.shares_today==null?'--':Number(s.shares_today).toLocaleString();$('sharesSession').textContent=s.shares_session==null?'':`Session: ${Number(s.shares_session).toLocaleString()} · Lifetime: ${Number(s.shares_lifetime||0).toLocaleString()}`;$('temp').textContent=s.avg_temp_c==null?'--°C':fmt(s.avg_temp_c,0)+'°C';$('fleetOnline').textContent=`${s.online}/${s.total} online`;$('power').textContent=s.known_power_w?fmt(s.known_power_w/1000,2)+' kW':'--';$('unknownPower').textContent=s.unknown_power_miners?`${s.unknown_power_miners} miner(s) don't report watts`:'All miners report power';$('fleetBestShare').textContent=s.fleet_best?fmtShare(s.fleet_best.value):'--';$('fleetBestMiner').textContent=s.fleet_best?`Miner: ${s.fleet_best.miner_name}`:'No reported best share'}catch(e){}}
async function loadMiniSpark(id){try{let rows=await fetch(`/api/miners/${id}/history?seconds=3600`).then(r=>r.json()),h=$(`spark-${id}`);if(!h)return;let v=rows.map(r=>r.hashrate).filter(x=>x!=null).map(Number);if(v.length<2){h.innerHTML='<div class="sub" style="padding:10px 0">Collecting history…</div>';return}let mn=Math.min(...v),mx=Math.max(...v);if(mx===mn)mx=mn+1;let pts=[];rows.forEach((r,i)=>{if(r.hashrate==null)return;let x=i/Math.max(1,rows.length-1)*195,y=40-(Number(r.hashrate)-mn)/(mx-mn)*34;pts.push(`${x.toFixed(1)},${y.toFixed(1)}`)});h.innerHTML=`<svg viewBox="0 0 195 45" preserveAspectRatio="none" style="width:100%;height:42px"><polyline fill="none" stroke="#38b6ff" stroke-width="1.5" points="${pts.join(' ')}"/></svg>`}catch(e){}}
async function openDetail(id){selectedMinerId=id;$('detailModal').classList.add('show');let m=await fetch(`/api/miners/${id}/detail`).then(r=>r.json()),t=m.telemetry||{};$('detailTitle').textContent=m.name;$('detailStats').innerHTML=`<div class="detail-stat"><small>Realtime Hashrate</small><b>${hashText(t,m.algorithm)}</b></div><div class="detail-stat"><small>Average Hashrate</small><b>${t.avg_hashrate!=null?fmt(t.avg_hashrate)+' '+(t.avg_hashrate_unit||''):'--'}</b></div><div class="detail-stat"><small>Temperature</small><b>${t.temp_c!=null?fmt(t.temp_c,0)+'°C':'--'}</b></div><div class="detail-stat"><small>Power</small><b>${t.power_w!=null?fmt(t.power_w,0)+' W':'--'}</b></div><div class="detail-stat"><small>Shares Today</small><b>${m.shares_today??'--'}</b></div><div class="detail-stat"><small>Session Shares</small><b>${m.shares_session??'--'}</b></div><div class="detail-stat"><small>Lifetime Shares</small><b>${m.shares_lifetime??'--'}</b></div><div class="detail-stat"><small>Reject %</small><b>${m.reject_pct!=null?fmt(m.reject_pct,4)+'%':'--'}</b></div><div class="detail-stat"><small>Best Share</small><b>${fmtShare(t.best_share)}</b></div><div class="detail-stat"><small>Uptime</small><b>${t.uptime_s?uptime(t.uptime_s):'--'}</b></div><div class="detail-stat"><small>Pool</small><b>${t.pool_alive===true?'Alive':t.pool_alive===false?'Down':'--'}</b></div><div class="detail-stat"><small>IP</small><b style="font-size:14px">${esc(m.ip)}</b></div>`;await loadDetailHistory(3600);await loadDetailEvents(id);await loadHardware(id)}
function closeDetail(){$('detailModal').classList.remove('show');selectedMinerId=null}
async function loadDetailHistory(seconds){if(!selectedMinerId)return;let rows=await fetch(`/api/miners/${selectedMinerId}/history?seconds=${seconds}`).then(r=>r.json());requestAnimationFrame(()=>{drawLineChart('hashChart',rows,'hashrate');drawLineChart('tempChart',rows,'temp_c');drawLineChart('powerChart',rows,'power_w')})}
function eventLabel(e){return({accepted_share:'Accepted share',rejected_share:'Rejected share',best_share:'New best share',block_found:'BLOCK FOUND',miner_offline:'Miner offline',miner_recovered:'Miner recovered',miner_seen_online:'Miner online',temperature_warning:'Temperature warning',hashrate_drop:'Hashrate drop',new_block:'New Bitcoin block'})[e.event_type]||e.event_type}
function eventIcon(e){return({accepted_share:'🎉',rejected_share:'❌',best_share:'🏆',block_found:'🎊',miner_offline:'🔴',miner_recovered:'🟢',miner_seen_online:'🟢',temperature_warning:'🌡️',hashrate_drop:'⚠️',new_block:'₿'})[e.event_type]||'•'}
function eventValue(e){if(e.event_type==='accepted_share'&&e.value_num!=null)return `+${e.value_num}`;if(e.event_type==='rejected_share'&&e.value_num!=null)return `+${e.value_num}`;if(e.event_type==='new_block')return `#${e.value_text||''}`;return e.value_text??e.value_num??''}
function eventTable(rows){if(!rows.length)return'<div class="health-item">No event rows are stored yet. Leave RigPulse running while miners submit shares and events will appear here.</div>';return `<table class="event-table"><thead><tr><th></th><th>Time</th><th>Miner</th><th>Event</th><th>Value</th></tr></thead><tbody>${rows.map(e=>`<tr><td>${eventIcon(e)}</td><td>${new Date(e.ts*1000).toLocaleString()}</td><td>${esc(e.miner_name||'Network')}</td><td>${esc(eventLabel(e))}</td><td>${esc(eventValue(e))}</td></tr>`).join('')}</tbody></table>`}
async function loadDetailEvents(id){try{let rows=await fetch(`/api/events?miner_id=${id}&limit=50`).then(r=>r.json());$('detailEvents').innerHTML=eventTable(rows)}catch(e){$('detailEvents').innerHTML='<div class="sub">Could not load events.</div>'}}
async function loadLogs(){$('fleetEvents').innerHTML='<div class="sub">Loading events…</div>';try{let rows=await fetch('/api/events?limit=300').then(async r=>{if(!r.ok)throw new Error(await r.text());return r.json()});$('logCount').textContent=`${rows.length} event${rows.length===1?'':'s'}`;$('fleetEvents').innerHTML=eventTable(rows)}catch(e){$('fleetEvents').innerHTML=`<div class="health-item">❌ Could not load logs: ${esc(String(e))}</div>`}}
async function openLogs(){$('logsModal').classList.add('show');await loadLogs()}
function closeLogs(){$('logsModal').classList.remove('show')}

function historySpark(samples,field='hashrate'){const vals=samples.map(x=>x[field]).filter(v=>v!=null&&Number.isFinite(Number(v))).map(Number);if(vals.length<2)return'<div class="sub" style="padding:18px 0">Not enough samples yet.</div>';let min=Math.min(...vals),max=Math.max(...vals);if(max===min)max=min+1;let pts=[];samples.forEach((s,i)=>{if(s[field]==null)return;let x=i/Math.max(1,samples.length-1)*600,y=62-(Number(s[field])-min)/(max-min)*54;pts.push(`${x.toFixed(1)},${y.toFixed(1)}`)});return `<svg viewBox="0 0 600 70" preserveAspectRatio="none" style="width:100%;height:70px"><line x1="0" y1="62" x2="600" y2="62" stroke="#1d344c"/><polyline fill="none" stroke="#38b6ff" stroke-width="2" points="${pts.join(' ')}"/></svg>`}
async function loadFleetHistory(seconds=86400){$('fleetHistory').innerHTML='<div class="sub">Loading history…</div>';try{const d=await fetch(`/api/fleet-history?seconds=${seconds}`).then(async r=>{if(!r.ok)throw new Error(await r.text());return r.json()});$('historyRange').textContent=`${seconds>=86400?Math.round(seconds/86400)+' day':Math.round(seconds/3600)+' hour'} view`;if(!d.miners.length){$('fleetHistory').innerHTML='<div class="health-item">No miners configured.</div>';return}$('fleetHistory').innerHTML=d.miners.map(x=>{const s=x.samples||[],l=x.latest||{},first=s[0]||{};let shareDelta=(l.accepted!=null&&first.accepted!=null)?Math.max(0,l.accepted-first.accepted):null;return `<div class="history-row clickable" onclick="openDetail(${x.miner.id})"><div class="history-row-head"><div><b>${esc(x.miner.name)}</b><div class="sub">${esc(x.miner.algorithm||'')} · ${s.length} samples</div></div><div class="status ${l.online?'green':'red'}">● ${l.online?'Online':'Offline'}</div></div><div class="history-spark">${historySpark(s)}</div><div class="history-stats"><span>Latest <b>${l.hashrate!=null?fmt(l.hashrate):'--'}</b></span><span>Temp <b>${l.temp_c!=null?fmt(l.temp_c,0)+'°C':'--'}</b></span><span>Power <b>${l.power_w!=null?fmt(l.power_w,0)+' W':'--'}</b></span><span>Accepted Δ <b>${shareDelta??'--'}</b></span></div></div>`}).join('')}catch(e){$('fleetHistory').innerHTML=`<div class="health-item">❌ Could not load history: ${esc(String(e))}</div>`}}
async function openHistory(){$('historyModal').classList.add('show');await loadFleetHistory(86400)}
function closeHistory(){$('historyModal').classList.remove('show')}

function poolStatus(p){return p.alive===true?['green','Alive']:p.alive===false?['red','Down']:['orange','Unknown']}
async function openPools(){$('poolsModal').classList.add('show');$('poolCards').innerHTML='<div class="sub">Loading pool telemetry…</div>';try{const d=await fetch('/api/pools').then(async r=>{if(!r.ok)throw new Error(await r.text());return r.json()});$('poolSummary').textContent=`${d.alive} alive · ${d.down} down · ${d.unknown} unknown`;$('poolCards').innerHTML=d.miners.map(p=>{const st=poolStatus(p);return `<div class="pool-card"><div style="display:flex;justify-content:space-between;gap:10px"><div><b>${esc(p.miner_name)}</b><div class="sub">${esc(p.algorithm||'')}</div></div><span class="status ${st[0]}">● ${st[1]}</span></div><div class="pool-url" title="${esc(p.url||'Not reported')}">${esc(p.url||'Pool URL not reported by firmware')}</div><div class="sub">${p.user?`Worker: ${esc(p.user)}`:'Worker not reported'}</div><div class="pool-stats"><div><small>Accepted</small><b>${p.accepted??'--'}</b></div><div><small>Rejected</small><b>${p.rejected??'--'}</b></div><div><small>Difficulty</small><b>${esc(p.difficulty??'--')}</b></div></div></div>`}).join('')||'<div class="health-item">No miners configured.</div>'}catch(e){$('poolCards').innerHTML=`<div class="health-item">❌ Could not load pools: ${esc(String(e))}</div>`}}
function closePools(){$('poolsModal').classList.remove('show')}

async function refreshHealth(){
 try{
  const h=await fetch('/api/health').then(r=>r.json());
  const b=$('healthBanner');
  const cls=h.status==='healthy'?'ok':h.status==='critical'?'bad':'warn';
  b.className='health-banner '+cls;
  const icon=h.status==='healthy'?'🟢':h.status==='critical'?'🔴':'🟠';
  const title=h.status==='healthy'?'Fleet Healthy':h.status==='critical'?'Fleet Needs Attention':'Fleet Warning';
  b.innerHTML=`<div class="health-dot">${icon}</div><div><div class="health-title">${title}</div><div class="health-detail">${h.healthy}/${h.total} healthy · ${h.issues_count} miner(s) with alerts</div></div>`;
  b.onclick=openHealth;
 }catch(e){}
}
async function openHealth(){
 $('healthModal').classList.add('show');
 const h=await fetch('/api/health').then(r=>r.json());
 if(!h.issues.length){
   $('healthDetails').innerHTML='<div class="health-item">🟢 No active fleet issues.</div>'; return;
 }
 $('healthDetails').innerHTML=h.issues.map(x=>`<div class="health-item"><b>${esc(x.miner_name)}</b><div class="sub">${esc(x.algorithm||'')} · ${esc(x.ip||'')}</div>${x.issues.map(i=>`<div style="margin-top:7px">${i.severity==='critical'?'🔴':'🟠'} ${esc(i.message)}</div>`).join('')}</div>`).join('');
}
async function openAlertSettings(){
 const a=await fetch('/api/alerts/settings').then(r=>r.json());
 $('aTemp').value=a.temperature_c;
 $('aHash').value=a.hashrate_drop_pct;
 $('aReject').value=a.reject_pct;
 $('aOffline').value=a.offline_seconds;
 $('aPool').checked=!!a.pool_disconnect;
 $('alertModal').classList.add('show');
}
async function saveAlertSettings(){
 const payload={
  temperature_c:Number($('aTemp').value),
  hashrate_drop_pct:Number($('aHash').value),
  reject_pct:Number($('aReject').value),
  offline_seconds:Number($('aOffline').value),
  pool_disconnect:$('aPool').checked
 };
 const r=await fetch('/api/alerts/settings',{method:'PUT',headers:{'Content-Type':'application/json'},body:JSON.stringify(payload)});
 if(r.ok){toast('Alert settings saved');$('alertModal').classList.remove('show');refreshHealth()}
}
function downloadConfig(){
 fetch('/api/export/config').then(r=>r.json()).then(data=>{
   const blob=new Blob([JSON.stringify(data,null,2)],{type:'application/json'});
   const a=document.createElement('a');a.href=URL.createObjectURL(blob);
   a.download=`rigpulse-config-${new Date().toISOString().slice(0,10)}.json`;
   a.click();setTimeout(()=>URL.revokeObjectURL(a.href),1000);
 });
}

function applyCustomization(c){
 customization=c||customization;
 const root=document.documentElement;
 root.style.setProperty('--card-opacity',Number(customization.card_opacity||.82));
 root.style.setProperty('--glass-blur',`${Number(customization.blur_px||0)}px`);
 root.style.setProperty('--bg-intensity',Number(customization.background_intensity||1));
 document.body.classList.remove('theme-aurora','theme-bitcoin','theme-matrix','theme-ocean','theme-mining-room','theme-liquid-lab','theme-nerd-console','compact');
 if(customization.theme && customization.theme!=='midnight')document.body.classList.add(`theme-${customization.theme}`);
 if(customization.compact_cards)document.body.classList.add('compact');
 applyNerdConsoleLayout(customization.theme==='nerd-console');
}
function applyNerdConsoleLayout(enabled){const main=document.querySelector('.main'),top=main.querySelector('.top'),health=$('healthBanner'),metrics=$('summaryMetrics'),chains=$('chainStrips'),panels=$('dashboardPanels'),wallet=$('walletStrip'),solo=$('solopoolStrip'),stream=$('shareStream'),toolbar=main.querySelector('.toolbar'),minerGrid=$('miners');if(!main||!panels)return;if(enabled){panels.append(stream);main.append(top,solo,wallet,health,toolbar,minerGrid,metrics,chains,panels)}else{panels.append(wallet,solo,stream);main.append(top,health,metrics,chains,panels,toolbar,minerGrid)}}
function previewCustomization(){
 const c={
  theme:$('cTheme').value,card_opacity:Number($('cOpacity').value),blur_px:Number($('cBlur').value),
  background_intensity:Number($('cIntensity').value),compact_cards:$('cCompact').checked,
  block_api_base:$('cBlockApi').value||'https://mempool.space/api',btc_wallet_address:$('cBtcWallet').value.trim(),bch_wallet_address:$('cBchWallet').value.trim(),btc_solopool_address:$('cBtcSoloPool').value.trim(),bch_solopool_address:$('cBchSoloPool').value.trim(),btc_solo_hashrate:Number($('cBtcSoloHash').value||0),btc_solo_hashrate_unit:$('cBtcSoloUnit').value,bch_solo_hashrate:Number($('cBchSoloHash').value||0),bch_solo_hashrate_unit:$('cBchSoloUnit').value
 };
 $('cOpacityVal').textContent=Math.round(c.card_opacity*100)+'%';
 $('cBlurVal').textContent=c.blur_px+'px'; $('cIntensityVal').textContent=c.background_intensity.toFixed(1)+'x';
 syncBackgroundChoices(c.theme);
 applyCustomization(c);
}
function syncBackgroundChoices(theme){document.querySelectorAll('.background-choice').forEach(b=>b.classList.toggle('active',b.dataset.theme===theme||(b.dataset.theme==='midnight'&&!['mining-room','liquid-lab','nerd-console'].includes(theme))))}
function chooseBackground(theme){$('cTheme').value=theme;previewCustomization()}
async function openCustomization(){
 customization=await fetch('/api/customization').then(r=>r.json());
 $('cTheme').value=customization.theme;$('cOpacity').value=customization.card_opacity;$('cBlur').value=customization.blur_px;
 $('cIntensity').value=customization.background_intensity;$('cCompact').checked=!!customization.compact_cards;$('cBlockApi').value=customization.block_api_base;$('cBtcWallet').value=customization.btc_wallet_address||'';$('cBchWallet').value=customization.bch_wallet_address||'';$('cBtcSoloPool').value=customization.btc_solopool_address||'';$('cBchSoloPool').value=customization.bch_solopool_address||'';$('cBtcSoloHash').value=customization.btc_solo_hashrate||'';$('cBtcSoloUnit').value=customization.btc_solo_hashrate_unit||'TH';$('cBchSoloHash').value=customization.bch_solo_hashrate||'';$('cBchSoloUnit').value=customization.bch_solo_hashrate_unit||'TH';
 previewCustomization();$('customModal').classList.add('show');
}
function closeCustomization(){$('customModal').classList.remove('show');applyCustomization(customization)}
function resetCustomization(){
 $('cTheme').value='midnight';$('cOpacity').value=.82;$('cBlur').value=14;$('cIntensity').value=1;$('cCompact').checked=false;$('cBlockApi').value='https://mempool.space/api';$('cBtcWallet').value='';$('cBchWallet').value='';$('cBtcSoloPool').value='';$('cBchSoloPool').value='';$('cBtcSoloHash').value='';$('cBchSoloHash').value='';$('cBtcSoloUnit').value='TH';$('cBchSoloUnit').value='TH';previewCustomization();
}
async function saveCustomization(){
 const body={theme:$('cTheme').value,card_opacity:Number($('cOpacity').value),blur_px:Number($('cBlur').value),background_intensity:Number($('cIntensity').value),compact_cards:$('cCompact').checked,block_api_base:$('cBlockApi').value||'https://mempool.space/api',btc_wallet_address:$('cBtcWallet').value.trim(),bch_wallet_address:$('cBchWallet').value.trim(),btc_solopool_address:$('cBtcSoloPool').value.trim(),bch_solopool_address:$('cBchSoloPool').value.trim(),btc_solo_hashrate:Number($('cBtcSoloHash').value||0),btc_solo_hashrate_unit:$('cBtcSoloUnit').value,bch_solo_hashrate:Number($('cBchSoloHash').value||0),bch_solo_hashrate_unit:$('cBchSoloUnit').value};
 const r=await fetch('/api/customization',{method:'PUT',headers:{'content-type':'application/json'},body:JSON.stringify(body)});
 if(!r.ok){toast('Could not save customization');return}
 customization=await r.json();applyCustomization(customization);$('customModal').classList.remove('show');$('btcBalance').textContent=customization.btc_wallet_address?'Updating…':'Not configured';$('bchBalance').textContent=customization.bch_wallet_address?'Updating…':'Not configured';toast('Customization saved');try{await fetch('/api/wallets/refresh',{method:'POST'});await loadChainExtras()}catch(e){toast('Saved — wallet service is temporarily unavailable')}setTimeout(loadBlockStatus,500);
}
['cTheme','cOpacity','cBlur','cIntensity','cCompact'].forEach(id=>document.addEventListener('input',e=>{if(e.target&&e.target.id===id)previewCustomization()}));

function shortHash(h){return h?`${h.slice(0,12)}…${h.slice(-8)}`:'—'}
function humanBytes(n){if(n==null)return'—';if(n>=1e6)return(n/1e6).toFixed(2)+' MB';if(n>=1e3)return(n/1e3).toFixed(1)+' KB';return n+' B'}
function blockAge(ts){if(!ts)return'—';let s=Math.max(0,Math.floor(Date.now()/1000-ts));if(s<60)return s+'s';if(s<3600)return Math.floor(s/60)+'m';return Math.floor(s/3600)+'h '+Math.floor((s%3600)/60)+'m'}
async function loadBlockStatus(){
 try{
  const b=await fetch('/api/block-status').then(r=>r.json());
  if(!b.available){$('blockHeight').textContent='Bitcoin block unavailable';$('blockHash').textContent=b.error||'waiting for network…';$('blockState').textContent='● WAITING';$('blockState').className='status orange';return}
  $('blockHeight').textContent=`Block ${Number(b.height).toLocaleString()}`;$('blockHash').textContent=shortHash(b.hash);
  $('blockAge').textContent=blockAge(b.timestamp);$('blockTx').textContent=b.tx_count==null?'—':Number(b.tx_count).toLocaleString();$('blockSize').textContent=humanBytes(b.size);$('blockDifficulty').textContent=fmtDifficulty(b.difficulty);
  $('blockSource').textContent=(b.source||'').replace(/^https?:\/\//,'').replace(/\/api\/?$/,'');$('blockState').textContent='● LIVE';$('blockState').className='status green';
 }catch(e){}
}
function newBlockCelebrate(b){
 const strip=$('blockStrip');strip.classList.remove('block-pulse');void strip.offsetWidth;strip.classList.add('block-pulse');
 toast(`₿ New Bitcoin block ${Number(b.height).toLocaleString()} · ${b.tx_count??'—'} tx`);
 for(let i=0;i<10;i++){setTimeout(()=>{let e=document.createElement('div');e.className='celebrate';e.textContent='₿';e.style.left=(8+Math.random()*84)+'vw';e.style.top=(-10-Math.random()*20)+'px';document.body.appendChild(e);setTimeout(()=>e.remove(),2400)},i*90)}
 loadBlockStatus();
}

function chancePct(v){if(v==null)return'—';let n=Number(v);if(n>=1)return n.toFixed(2)+'%';if(n>=.01)return n.toFixed(3)+'%';if(n>=.0001)return n.toFixed(5)+'%';return n.toExponential(2)+'%'}
function chanceOneIn(v){let n=Number(v);if(!Number.isFinite(n)||n<=0)return'1 in ∞';let x=100/n;if(x<10)return`1 in ${x.toFixed(2)}`;if(x<1000)return`1 in ${x.toFixed(1)}`;return`1 in ${Math.round(x).toLocaleString()}`}
function expectedTime(seconds){if(seconds==null||!Number.isFinite(Number(seconds)))return'—';let d=Number(seconds)/86400;if(d<1)return Math.max(1,Math.round(Number(seconds)/3600))+' hours';if(d<365)return d.toFixed(d<10?1:0)+' days';let y=d/365;return y<1000?y.toFixed(y<10?1:0)+' years':y.toExponential(2)+' years'}
function renderSoloChance(coin,c){const odds=$(coin+'SoloOdds'),expected=$(coin+'SoloExpected');if(!c){odds.textContent='Configure hashrate';expected.textContent='—';return}odds.textContent=`24h ${chanceOneIn(c.chance_24h_pct)} chance (${chancePct(c.chance_24h_pct)}) · 7d ${chanceOneIn(c.chance_7d_pct)} chance (${chancePct(c.chance_7d_pct)}) · 30d ${chanceOneIn(c.chance_30d_pct)} chance (${chancePct(c.chance_30d_pct)})`;expected.textContent=expectedTime(c.expected_seconds)}
function updateSoloPoolLayout(){const strip=$('solopoolStrip'),items=[...strip.querySelectorAll('.solopool-item')],active=items.filter(item=>!item.classList.contains('unconfigured')).length;strip.classList.toggle('single-pool',active===1)}
function renderSoloPool(coin,p,configured){const el=$(coin+'SoloPoolStatus'),item=el.closest('.solopool-item');item.classList.toggle('unconfigured',!p&&!configured);if(!p){el.innerHTML=`<div class="solopool-hash">—</div><div class="solopool-stats"><span>${configured?'Waiting for API…':'Not configured'}</span></div>`;updateSoloPoolLayout();return}item.classList.remove('unconfigured');const online=Number(p.online_workers||0),total=Number(p.total_workers||0),pct=total?Math.max(0,Math.min(100,online/total*100)):0;el.innerHTML=`<div class="solopool-console-main"><div class="pool-gauge" style="--pool-pct:${pct}%"><span>${online}/${total}<small>online</small></span></div><div><div class="solopool-hash">${hashFromHs(p.hashrate)}</div><div class="solopool-stats"><span>Current hashrate</span><span>Average <b>${hashFromHs(p.average_hashrate)}</b></span><span>Last share <b>${blockAge(p.last_share)}</b></span></div></div></div><div class="solopool-highlights"><div class="solopool-highlight"><span>Best Share</span><b>${fmtShare(p.best_share)}</b></div><div class="solopool-highlight"><span>Blocks Found</span><b>${p.total_blocks??0}</b></div></div>`;updateSoloPoolLayout()}
function hashFromHs(v){if(v==null)return'—';let n=Number(v);if(n>=1e15)return(n/1e15).toFixed(2)+' PH/s';if(n>=1e12)return(n/1e12).toFixed(2)+' TH/s';if(n>=1e9)return(n/1e9).toFixed(2)+' GH/s';return n.toLocaleString()+' H/s'}
function renderPrice(symbol,p){const value=$(symbol+'Price'),change=$(symbol+'PriceChange');if(!p||p.usd==null){value.textContent='Unavailable';change.textContent='';change.className='price-change';if(symbol==='alph'){$('alphPriceState').textContent='● WAITING';$('alphPriceState').className='status orange'}return}const n=Number(p.usd),digits=n>=1000?0:n>=1?2:n>=.01?3:5;value.textContent=n.toLocaleString(undefined,{style:'currency',currency:'USD',minimumFractionDigits:digits,maximumFractionDigits:digits});const c=Number(p.change_24h);if(Number.isFinite(c)){change.textContent=`${c>=0?'+':''}${c.toFixed(2)}% 24h`;change.className=`price-change ${c>=0?'up':'down'}`}else{change.textContent='';change.className='price-change'}if(symbol==='alph'){$('alphPriceState').textContent='● LIVE';$('alphPriceState').className='status green'}}
function walletText(entry,price,symbol){if(entry?.balance==null)return null;const amount=Number(entry.balance),usd=Number(price?.usd),fiat=Number.isFinite(usd)?` · ${(amount*usd).toLocaleString(undefined,{style:'currency',currency:'USD',minimumFractionDigits:2,maximumFractionDigits:2})}`:'';return `${amount.toFixed(8)} ${symbol}${fiat}`}

async function loadChainExtras(){
 try{
  const d=await fetch('/api/chain-status').then(r=>r.json()),b=d.bch||{},w=d.wallets||{},sp=d.solopool||{},prices=d.prices||{};
  if(b.available){$('bchBlockHeight').textContent=`BCH Block ${Number(b.height).toLocaleString()}`;$('bchBlockHash').textContent=shortHash(b.hash);$('bchBlockAge').textContent='Live';$('bchBlockTx').textContent=b.transactions_24h==null?'—':Number(b.transactions_24h).toLocaleString();$('bchMempool').textContent=b.mempool_transactions==null?'—':Number(b.mempool_transactions).toLocaleString();$('bchDifficulty').textContent=fmtDifficulty(b.difficulty);$('bchBlockState').textContent='● LIVE';$('bchBlockState').className='status green'}
  else{$('bchBlockHeight').textContent='BCH unavailable';$('bchBlockState').textContent='● WAITING';$('bchBlockState').className='status orange'}
  $('btcBalance').textContent=walletText(w.btc,prices.btc,'BTC')??(customization.btc_wallet_address?(w.errors?.btc?'Address/API error':'Updating…'):'Not configured');$('bchBalance').textContent=walletText(w.bch,prices.bch,'BCH')??(customization.bch_wallet_address?(w.errors?.bch?'Address/API error':'Updating…'):'Not configured');
  renderPrice('btc',prices.btc);renderPrice('bch',prices.bch);renderPrice('alph',prices.alph);
  renderSoloChance('btc',d.solo_chances?.btc);renderSoloChance('bch',d.solo_chances?.bch);renderSoloPool('btc',sp.btc,!!customization.btc_solopool_address);renderSoloPool('bch',sp.bch,!!customization.bch_solopool_address);
 }catch(e){}
}
function closeBlockParty(){$('blockPartyModal').classList.remove('show')}
function cardBlockConfetti(minerId){const card=[...document.querySelectorAll('.miner')].find(c=>(c.getAttribute('onclick')||'').includes(`openDetail(${minerId})`));if(!card)return;const end=Date.now()+10000,timer=setInterval(()=>{if(Date.now()>end){clearInterval(timer);return}for(let i=0;i<3;i++){let p=document.createElement('span');p.className='card-confetti';p.textContent=['🎊','✨','₿','🟨'][Math.floor(Math.random()*4)];p.style.left=(5+Math.random()*90)+'%';p.style.top='-15px';card.appendChild(p);setTimeout(()=>p.remove(),1900)}},180)}
function blockFoundCelebrate(b){lastBlockFound=b;$('replayBlockBtn').style.display='inline-block';$('blockPartyMiner').textContent=`${b.miner_name||'Your miner'} found a block!`;$('blockPartyDetails').textContent=`${b.algorithm||b.details?.algorithm||''} · ${new Date((b.ts||Date.now()/1000)*1000).toLocaleString()}`;$('blockPartyModal').classList.add('show');for(let i=0;i<100;i++){setTimeout(()=>{let e=document.createElement('div');e.className='celebrate';e.textContent=['🎊','🎉','✨','₿','🏆'][Math.floor(Math.random()*5)];e.style.left=(3+Math.random()*94)+'vw';e.style.top=(-10-Math.random()*30)+'px';document.body.appendChild(e);setTimeout(()=>e.remove(),2400)},i*95)}cardBlockConfetti(Number(b.miner_id));load()}
function replayLastBlock(){if(lastBlockFound)blockFoundCelebrate(lastBlockFound);else toast('No miner block event has been recorded yet')}
async function loadLastBlockEvent(){try{const b=await fetch('/api/block-found/latest').then(r=>r.json());if(b.available){lastBlockFound=b;$('replayBlockBtn').style.display='inline-block';const ageSeconds=Math.max(0,Date.now()/1000-Number(b.ts||0));if(b.active&&ageSeconds<12*60*60)blockFoundCelebrate(b)}}catch(e){}}

function showDetailSection(section,btn){
 document.querySelectorAll('.detail-tab').forEach(x=>x.classList.remove('active')); if(btn)btn.classList.add('active');
 $('detailCharts').style.display=section==='charts'?'block':'none';
 $('detailHardware').style.display=section==='hardware'?'block':'none';
 $('detailEventsWrap').style.display=section==='events'?'block':'none';
}
function simpleVal(v){return v==null||v===''?'--':String(v)}
function boardTable(rows){
 if(!Array.isArray(rows)||!rows.length)return'<div class="sub">No board-level data reported by this firmware.</div>';
 const keys=[...new Set(rows.flatMap(r=>r&&typeof r==='object'?Object.keys(r):[]))].slice(0,8);
 if(!keys.length)return'<div class="sub">No board-level data reported.</div>';
 return `<table class="board-table"><thead><tr>${keys.map(k=>`<th>${esc(k)}</th>`).join('')}</tr></thead><tbody>${rows.map(r=>`<tr>${keys.map(k=>`<td>${esc(simpleVal(r?.[k]))}</td>`).join('')}</tr>`).join('')}</tbody></table>`;
}
async function loadHardware(id){
 try{
  const h=await fetch(`/api/miners/${id}/hardware`).then(r=>r.json());
  const fans=(h.fans||[]).map(f=>`<div class="hardware-card"><small>${esc(f.name||'Fan')}</small><b>${esc(simpleVal(f.rpm))}${f.rpm!=null?' RPM':''}</b></div>`).join('');
  const temps=(h.temperatures||[]).map(t=>`<div class="hardware-card"><small>${esc(t.name||'Temp')}</small><b>${esc(simpleVal(t.c))}${t.c!=null?'°C':''}</b></div>`).join('');
  $('detailHardware').innerHTML=`<div class="hardware-grid">
   <div class="hardware-card"><small>Firmware</small><b>${esc(simpleVal(h.firmware))}</b></div>
   <div class="hardware-card"><small>Work Mode</small><b>${esc(simpleVal(h.work_mode))}</b></div>
   <div class="hardware-card"><small>HW Errors</small><b>${esc(simpleVal(h.hardware_errors))}</b></div>
   <div class="hardware-card"><small>Power Source</small><b>${esc(simpleVal(h.power_source))}</b></div>
   ${fans}${temps}
  </div><div class="section-title">Boards / ASIC Data</div>${boardTable(h.boards)}`;
 }catch(e){$('detailHardware').innerHTML='<div class="sub">Hardware detail unavailable.</div>'}
}
function fmtDifficulty(v){
 if(v==null)return'--';let n=Number(v);if(!Number.isFinite(n))return String(v);
 const units=[['E',1e18],['P',1e15],['T',1e12],['G',1e9],['M',1e6],['K',1e3]];
 for(const [u,m] of units)if(n>=m)return (n/m).toFixed(2)+u;
 return n.toFixed(0);
}
function fmtOneIn(v){if(v==null||!Number.isFinite(Number(v)))return'--';let n=Number(v);if(n<1)return'better than target';if(n<10)return`1 in ${n.toFixed(2)}`;return`1 in ${Math.round(n).toLocaleString()}`}
async function openNetwork(){
 $('networkModal').classList.add('show');$('networkCards').innerHTML='<div class="sub">Loading network…</div>';
 try{
  const d=await fetch('/api/network-status').then(r=>r.json()),b=d.block||{},n=d.network||{};
  $('networkCards').innerHTML=`
   <div class="card network-card"><small>Block Height</small><b>${b.height!=null?Number(b.height).toLocaleString():'--'}</b></div>
   <div class="card network-card"><small>Difficulty</small><b>${fmtDifficulty(d.difficulty)}</b></div>
   <div class="card network-card"><small>Mempool</small><b>${n.mempool_count!=null?Number(n.mempool_count).toLocaleString():'--'} tx</b></div>
   <div class="card network-card"><small>Fast Fee</small><b>${n.fastest_fee!=null?n.fastest_fee+' sat/vB':'--'}</b></div>
   <div class="card network-card"><small>1 Hour Fee</small><b>${n.hour_fee!=null?n.hour_fee+' sat/vB':'--'}</b></div>`;
  if(!d.leaderboard?.length){$('bestLeaderboard').innerHTML='<div class="sub">No miners currently report a best-share value.</div>';return}
  $('bestLeaderboard').innerHTML=`<table class="leaderboard"><thead><tr><th>#</th><th>Miner</th><th>Best Share</th><th>Difficulty Progress</th><th>Comparison</th></tr></thead><tbody>${d.leaderboard.map((x,i)=>`<tr><td>${i+1}</td><td>${esc(x.miner_name)}</td><td>${fmtShare(x.best_share)}</td><td>${x.difficulty_progress_pct!=null?x.difficulty_progress_pct.toFixed(6)+'%':'--'}</td><td>${fmtOneIn(x.one_in)}</td></tr>`).join('')}</tbody></table>`;
 }catch(e){$('networkCards').innerHTML=`<div class="health-item">❌ Could not load network status: ${esc(String(e))}</div>`}
}
function closeNetwork(){$('networkModal').classList.remove('show')}
function applyFleetBestRing(){document.querySelectorAll('.miner').forEach(card=>{const id=Number((card.getAttribute('onclick')||'').match(/openDetail\((\d+)\)/)?.[1]);card.classList.toggle('fleet-best',id===fleetBestMinerId)})}
async function refreshFleetBestRing(){try{const s=await fetch('/api/fleet-summary').then(r=>r.json());fleetBestMinerId=s.fleet_best?.miner_id??null;applyFleetBestRing()}catch(e){}}
function stabilizeMinerCards(){applyFleetBestRing();document.querySelectorAll('.miner').forEach(card=>{if(card.classList.contains('pool-miner-card'))return;const id=Number((card.getAttribute('onclick')||'').match(/openDetail\((\d+)\)/)?.[1]),m=miners.find(x=>x.id===id),spark=$(`spark-${id}`);if(spark&&sparkCache.has(id))spark.innerHTML=sparkCache.get(id);card.classList.toggle('block-winner',!!m?.block_found);if(m?.block_found&&!card.querySelector('.block-found-badge'))card.insertAdjacentHTML('afterbegin','<div class="block-found-badge">⛏ BLOCK FOUND</div>');if(!m?.block_found)card.querySelector('.block-found-badge')?.remove();const stats=card.querySelector('.stats');if(stats&&!stats.querySelector('.current-share-stat'))stats.children[3]?.insertAdjacentHTML('afterend',`<div class="current-share-stat"><span>Current Share Difficulty</span><b>${fmtShare(m?.telemetry?.current_share)}</b></div>`);if(stats&&!stats.querySelector('.fan-stat'))stats.insertAdjacentHTML('beforeend',`<div class="fan-stat"><span>Fan Speed</span><b>${m?.telemetry?.fan_rpm!=null?fmt(m.telemetry.fan_rpm,0)+' RPM':'--'}</b></div>`);if(stats)stats.querySelectorAll(':scope>div').forEach(cell=>{if(cell.querySelector('.console-stat-icon'))return;const label=(cell.querySelector('span')?.textContent||'').toLowerCase();let icon=label.includes('temp')?'°':label.includes('power')?'ϟ':label.includes('best')?'◆':label.includes('current')?'◎':label.includes('fan')?'✣':label.includes('uptime')?'◷':label.includes('pool')?'●':label.includes('reject')?'×':label.includes('efficiency')?'η':'Σ';cell.insertAdjacentHTML('afterbegin',`<i class="console-stat-icon">${icon}</i>`)})})}
new MutationObserver(()=>requestAnimationFrame(stabilizeMinerCards)).observe($('miners'),{childList:true});
function connectWS(){
 let proto=location.protocol==='https:'?'wss':'ws',ws=new WebSocket(`${proto}://${location.host}/ws`);
 ws.onopen=()=>{$('wsState').textContent='LIVE';$('wsState').style.color='#31da7a';ws.send('hello')};
 ws.onmessage=e=>{let x=JSON.parse(e.data);if(x.type==='share')celebrate(x.miner_name,x.count);if(x.type==='best_share')celebrate(x.miner_name,1,'best_share');if(x.type==='block_found')blockFoundCelebrate(x);if(x.type==='rejected_share')toast(`❌ ${x.miner_name}: ${x.count} rejected`);if(x.type==='miner_offline')toast(`🔴 ${x.miner_name} went offline`);if(x.type==='miner_recovered')toast(`🟢 ${x.miner_name} recovered`);if(x.type==='temperature_warning')toast(`🌡️ ${x.miner_name}: ${x.value}°C`);if(x.type==='hashrate_drop')toast(`⚠️ ${x.miner_name}: hashrate drop`);if(x.type==='new_block')newBlockCelebrate(x)};
 ws.onclose=()=>{$('wsState').textContent='reconnecting…';setTimeout(connectWS,2000)}
}
setupDashboardLayout();toggleSidebar(localStorage.getItem(SIDEBAR_KEY)==='1');load().then(()=>{loadLastBlockEvent();loadChainExtras()});connectWS();refreshFleetBestRing();setInterval(load,10000);setInterval(refreshFleetBestRing,10000);setInterval(loadBlockStatus,20000);setInterval(loadChainExtras,30000);
</script>
</body></html>"""
