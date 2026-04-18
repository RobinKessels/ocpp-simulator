import asyncio
import logging
import os
import websockets
import random
import sqlite3
from datetime import datetime, timezone
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from uvicorn import Config, Server

from ocpp.routing import on
from ocpp.v21 import ChargePoint as cp
from ocpp.v21 import call, call_result
from ocpp.v21.enums import Action

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("ocpp_server_api")

active_chargers = {}
SITE_MAX_AMPS = int(os.getenv("SITE_MAX_AMPS", "64"))
PER_EVSE_MAX_AMPS = int(os.getenv("PER_EVSE_MAX_AMPS", "16"))
DB_PATH = os.getenv("CSMS_DB_PATH", "/app/data/csms.db")
EVSE_IDS = [1, 2]


def ensure_db():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS stations (
                cp_id TEXT PRIMARY KEY,
                status TEXT NOT NULL,
                last_seen TEXT
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS sessions (
                transaction_id TEXT PRIMARY KEY,
                cp_id TEXT NOT NULL,
                evse_id INTEGER NOT NULL,
                started_at TEXT,
                ended_at TEXT,
                end_reason TEXT,
                energy_wh REAL,
                final_soc REAL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS site_config (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                site_max_amps INTEGER NOT NULL,
                per_evse_max_amps INTEGER NOT NULL
            )
            """
        )
        conn.execute(
            """
            INSERT OR IGNORE INTO site_config (id, site_max_amps, per_evse_max_amps)
            VALUES (1, ?, ?)
            """,
            (SITE_MAX_AMPS, PER_EVSE_MAX_AMPS),
        )
        conn.commit()


def now_iso():
    return datetime.now(timezone.utc).isoformat()


def upsert_station(cp_id: str, status: str):
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            """
            INSERT INTO stations (cp_id, status, last_seen)
            VALUES (?, ?, ?)
            ON CONFLICT(cp_id)
            DO UPDATE SET status=excluded.status, last_seen=excluded.last_seen
            """,
            (cp_id, status, now_iso()),
        )
        conn.commit()


def upsert_session_start(transaction_id: str, cp_id: str, evse_id: int, started_at: str):
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            """
            INSERT INTO sessions (transaction_id, cp_id, evse_id, started_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(transaction_id)
            DO UPDATE SET cp_id=excluded.cp_id, evse_id=excluded.evse_id, started_at=excluded.started_at
            """,
            (transaction_id, cp_id, evse_id, started_at),
        )
        conn.commit()


def update_session_progress(transaction_id: str, energy_wh, soc_percent):
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            """
            UPDATE sessions
            SET energy_wh = COALESCE(?, energy_wh),
                final_soc = COALESCE(?, final_soc)
            WHERE transaction_id = ?
            """,
            (energy_wh, soc_percent, transaction_id),
        )
        conn.commit()


def close_session(transaction_id: str, ended_at: str, end_reason: str, energy_wh, soc_percent):
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            """
            UPDATE sessions
            SET ended_at = ?,
                end_reason = ?,
                energy_wh = COALESCE(?, energy_wh),
                final_soc = COALESCE(?, final_soc)
            WHERE transaction_id = ?
            """,
            (ended_at, end_reason, energy_wh, soc_percent, transaction_id),
        )
        conn.commit()


def get_active_session_count() -> int:
    return sum(len(charger.active_transactions) for charger in active_chargers.values())


def default_evse_state(evse_id: int):
    return {
        "evse_id": evse_id,
        "status": "Connected",
        "transaction_id": None,
        "soc_percent": None,
        "energy_wh": None,
        "assigned_amps": 0,
        "last_event_type": "None",
        "last_update": None,
    }


def recompute_load_limits():
    active_sessions = get_active_session_count()
    assigned_amps = (
        min(PER_EVSE_MAX_AMPS, SITE_MAX_AMPS // active_sessions) if active_sessions > 0 else 0
    )
    for charger in active_chargers.values():
        for evse_id, state in charger.station_state.items():
            state["assigned_amps"] = (
                assigned_amps if evse_id in charger.active_transactions else 0
            )


def get_assigned_amps(cp_id: str, evse_id: int) -> int:
    charger = active_chargers.get(cp_id)
    if not charger:
        return 0
    state = charger.station_state.get(evse_id, {})
    return int(state.get("assigned_amps", 0) or 0)


def generate_id():
    # Returns a random integer between 1 and 2 billion
    return random.randint(1, 2147483647)


class ChargePoint(cp):
    @on(Action.boot_notification)
    async def on_boot_notification(self, charging_station, reason, **kwargs):
        logger.info(
            f"[{self.id}] Boot: {charging_station.get('vendor_name')} | Reason: {reason}"
        )
        upsert_station(self.id, "Booted")
        return call_result.BootNotification(
            current_time=datetime.now(timezone.utc).isoformat(),
            interval=10,
            status="Accepted",
        )

    @on(Action.heartbeat)
    async def on_heartbeat(self):
        return call_result.Heartbeat(
            current_time=datetime.now(timezone.utc).isoformat()
        )

    @on(Action.transaction_event)
    async def on_transaction_event(
        self, event_type, timestamp, trigger_reason, seq_no, transaction_info, **kwargs
    ):
        tx_id = transaction_info.get("transaction_id") or transaction_info.get(
            "transactionId"
        )
        if not tx_id:
            logger.warning(
                f"[{self.id}] TransactionEvent missing transaction id: {transaction_info}"
            )
            return call_result.TransactionEvent()
        evse = kwargs.get("evse", {})
        evse_id = evse.get("id", 1)
        meter_value = kwargs.get("meter_value", [])
        soc_percent = None
        energy_wh = None

        for reading in meter_value:
            sampled_values = reading.get("sampled_value") or reading.get(
                "sampledValue", []
            )
            for sampled in sampled_values:
                measurand = sampled.get("measurand")
                value = sampled.get("value")
                if measurand == "SoC":
                    soc_percent = value
                elif measurand == "Energy.Active.Import.Register":
                    energy_wh = value

        if event_type == "Started":
            logger.info(f"[{self.id}] Transaction STARTED. ID: {tx_id}")
            self.active_transactions[evse_id] = tx_id
            self.station_state[evse_id] = {
                "evse_id": evse_id,
                "status": "Charging",
                "transaction_id": tx_id,
                "soc_percent": soc_percent,
                "energy_wh": energy_wh,
                "assigned_amps": 0,
                "last_event_type": event_type,
                "last_update": timestamp,
            }
            upsert_session_start(tx_id, self.id, evse_id, timestamp)
        elif event_type == "Updated":
            prev = self.station_state.get(evse_id, {})
            self.station_state[evse_id] = {
                "evse_id": evse_id,
                "status": "Charging",
                "transaction_id": tx_id,
                "soc_percent": soc_percent if soc_percent is not None else prev.get("soc_percent"),
                "energy_wh": energy_wh if energy_wh is not None else prev.get("energy_wh"),
                "assigned_amps": prev.get("assigned_amps", 0),
                "last_event_type": event_type,
                "last_update": timestamp,
            }
            update_session_progress(tx_id, energy_wh, soc_percent)
        elif event_type == "Ended":
            logger.info(f"[{self.id}] Transaction ENDED. ID: {tx_id}")
            if evse_id in self.active_transactions:
                del self.active_transactions[evse_id]
            prev = self.station_state.get(evse_id, {})
            self.station_state[evse_id] = {
                "evse_id": evse_id,
                "status": "Idle",
                "transaction_id": None,
                "soc_percent": soc_percent if soc_percent is not None else prev.get("soc_percent"),
                "energy_wh": energy_wh if energy_wh is not None else prev.get("energy_wh"),
                "assigned_amps": 0,
                "last_event_type": event_type,
                "last_update": timestamp,
            }
            close_session(tx_id, timestamp, trigger_reason, energy_wh, soc_percent)
        recompute_load_limits()
        return call_result.TransactionEvent()

    def __init__(self, id, connection):
        super().__init__(id, connection)
        self.active_transactions = {}  # Mapping of evse_id: transaction_id
        self.station_state = {}

app = FastAPI(title="CSMS API")
ensure_db()
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:8080",
        "http://127.0.0.1:8080",
        "http://localhost:8081",
        "http://127.0.0.1:8081",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/chargers")
async def list_chargers():
    """Returns a list of all currently connected charger IDs."""
    return {"connected_chargers": list(active_chargers.keys())}

@app.get("/stations")
async def list_stations():
    stations = []
    for cp_id, charger in active_chargers.items():
        evses = []
        for evse_id in EVSE_IDS:
            evses.append(charger.station_state.get(evse_id, default_evse_state(evse_id)))
        stations.append({"cp_id": cp_id, "evses": evses})
    return {"stations": stations}


@app.get("/site-state")
async def site_state():
    active_sessions = get_active_session_count()
    assigned_amps = (
        min(PER_EVSE_MAX_AMPS, SITE_MAX_AMPS // active_sessions) if active_sessions > 0 else 0
    )
    return {
        "site_max_amps": SITE_MAX_AMPS,
        "per_evse_max_amps": PER_EVSE_MAX_AMPS,
        "active_sessions": active_sessions,
        "assigned_amps_per_active_evse": assigned_amps,
        "total_allocated_amps": active_sessions * assigned_amps,
    }


@app.get("/allocation/{cp_id}/{evse_id}")
async def station_allocation(cp_id: str, evse_id: int):
    if cp_id not in active_chargers:
        raise HTTPException(status_code=404, detail="Charger not connected")
    return {"cp_id": cp_id, "evse_id": evse_id, "assigned_amps": get_assigned_amps(cp_id, evse_id)}


@app.get("/sessions")
async def list_sessions(limit: int = 50):
    safe_limit = max(1, min(limit, 500))
    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            """
            SELECT transaction_id, cp_id, evse_id, started_at, ended_at, end_reason, energy_wh, final_soc
            FROM sessions
            ORDER BY COALESCE(ended_at, started_at) DESC
            LIMIT ?
            """,
            (safe_limit,),
        ).fetchall()
    return {"sessions": [dict(row) for row in rows]}

@app.post("/remote-start/{cp_id}/{evse_id}")
async def remote_start(cp_id: str, evse_id: int):
    """Triggers a RequestStartTransaction command to a specific charger."""
    if cp_id not in active_chargers:
        raise HTTPException(
            status_code=404, detail=f"Charger {cp_id} not found/connected."
        )

    charger = active_chargers[cp_id]

    request = call.RequestStartTransaction(
        remote_start_id=generate_id(),
        id_token={"idToken": "SIM-DRIVER", "type": "Central"},
        evse_id=evse_id,  # <--- This tells the box which cable to unlock
    )

    try:
        response = await charger.call(request)
        return {"status": response.status, "charger_id": cp_id}
    except Exception as e:
        logger.error(f"Failed to send command to {cp_id}: {e}")
        raise HTTPException(status_code=500, detail="OCPP Command Timeout")


@app.post("/remote-stop/{cp_id}/{evse_id}")
async def remote_stop(cp_id: str, evse_id: int):
    """Tells a charger to stop a specific active transaction."""
    if cp_id not in active_chargers:
        raise HTTPException(status_code=404, detail="Charger not connected")

    charger = active_chargers[cp_id]

    tx_id = charger.active_transactions.get(evse_id)

    if not tx_id:
        raise HTTPException(status_code=400, detail="No active transaction found on this EVSE")

    request = call.RequestStopTransaction(transaction_id=tx_id)
    response = await charger.call(request)
    return {"status": response.status, "stopped_transaction_id": tx_id}


# WebSocket Handler
async def on_connect(websocket):
    cp_id = websocket.request.path.strip("/")
    charger = ChargePoint(cp_id, websocket)

    # Register the charger in our global dict
    active_chargers[cp_id] = charger
    upsert_station(cp_id, "Connected")
    logger.info(f"[{cp_id}] Connected and registered in API state.")

    try:
        await charger.start()
    finally:
        # Unregister when the charger disconnects
        if cp_id in active_chargers:
            del active_chargers[cp_id]
        upsert_station(cp_id, "Disconnected")
        recompute_load_limits()
        logger.warning(f"[{cp_id}] Disconnected and removed from API state.")


async def main():
    # WebSocket Server (Port 9000)
    ws_server = websockets.serve(on_connect, "0.0.0.0", 9000, subprotocols=["ocpp2.1"])

    # FastAPI Server (Port 8000)
    config = Config(app=app, host="0.0.0.0", port=8000, loop="asyncio")
    api_server = Server(config)

    logger.info("Starting Multi-Protocol Server (OCPP: 9000, API: 8000)")

    await asyncio.gather(ws_server, api_server.serve())


if __name__ == "__main__":
    asyncio.run(main())
