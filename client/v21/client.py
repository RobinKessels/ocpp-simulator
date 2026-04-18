import asyncio
import logging
import websockets
import os
import uuid
import random
import urllib.request
import urllib.error
import json
from datetime import datetime, timezone

from ocpp.v21 import ChargePoint as cp
from ocpp.v21 import call, call_result
from ocpp.v21.datatypes import ChargingStationType
from ocpp.v21.enums import Action
from ocpp.routing import on

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("charge_point")


class ChargePoint(cp):
    def __init__(self, id, connection):
        super().__init__(id, connection)
        self.active_sessions = {}
        self.api_base_url = os.getenv("CSMS_API_URL", "http://ocpp-server:8000").rstrip("/")

    async def send_heartbeat(self, interval):
        """background task to send heartbeats at the interval defined by the csms."""
        logger.info(f"[{self.id}] starting heartbeat loop. interval: {interval}s")
        try:
            while True:
                request = call.Heartbeat()
                await self.call(request)
                logger.debug(f"[{self.id}] heartbeat sent.")
                await asyncio.sleep(interval)
        except Exception as e:
            logger.error(f"[{self.id}] heartbeat loop interrupted: {e}")

    async def send_boot_notification(self):
        logger.info(f"[{self.id}] Sending BootNotification...")

        request = call.BootNotification(
            charging_station=ChargingStationType(
                model="Wallbox XYZ", vendor_name="acme"
            ),
            reason="PowerUp",
        )

        try:
            response = await self.call(request)
            if response.status == "Accepted":
                logger.info(f"[{self.id}] Registration ACCEPTED by Central System.")
                # Start the heartbeat loop using the interval provided by the server
                await self.send_heartbeat(response.interval)
            else:
                logger.warning(
                    f"[{self.id}] Registration {response.status}. Closing connection."
                )
        except Exception as e:
            logger.error(f"[{self.id}] Error during BootNotification: {e}")

    def _now(self):
        return datetime.now(timezone.utc).isoformat()

    async def _send_transaction_event(
        self,
        event_type,
        trigger_reason,
        seq_no,
        transaction_id,
        evse_id,
        meter_wh,
        soc_percent,
    ):
        request = call.TransactionEvent(
            event_type=event_type,
            timestamp=self._now(),
            trigger_reason=trigger_reason,
            seq_no=seq_no,
            transaction_info={"transactionId": transaction_id},
            evse={"id": evse_id},
            meter_value=[
                {
                    "timestamp": self._now(),
                    "sampledValue": [
                        {"value": meter_wh, "measurand": "Energy.Active.Import.Register"},
                        {"value": soc_percent, "measurand": "SoC"},
                    ],
                }
            ],
        )
        await self.call(request)

    async def _fetch_assigned_amps(self, evse_id):
        endpoint = f"{self.api_base_url}/allocation/{self.id}/{evse_id}"

        def _fetch():
            req = urllib.request.Request(endpoint, method="GET")
            with urllib.request.urlopen(req, timeout=2) as response:
                payload = json.loads(response.read().decode("utf-8"))
                return int(payload.get("assigned_amps", 0))

        try:
            return await asyncio.to_thread(_fetch)
        except (urllib.error.URLError, TimeoutError, ValueError, json.JSONDecodeError):
            # Keep simulation resilient even if API polling fails briefly.
            return 0

    async def _charging_loop(self, transaction_id):
        session = self.active_sessions[transaction_id]
        evse_id = session["evse_id"]
        meter_wh = random.randint(5000, 9000)
        soc_percent = random.randint(20, 60)

        await self._send_transaction_event(
            event_type="Started",
            trigger_reason="RemoteStart",
            seq_no=session["seq_no"],
            transaction_id=transaction_id,
            evse_id=evse_id,
            meter_wh=meter_wh,
            soc_percent=soc_percent,
        )
        session["seq_no"] += 1

        while not session["stop_event"].is_set():
            await asyncio.sleep(5)
            assigned_amps = await self._fetch_assigned_amps(evse_id)
            amp_ratio = max(assigned_amps, 0) / 16
            energy_step = int(random.randint(80, 240) * amp_ratio)
            soc_step = int(round(random.randint(2, 6) * amp_ratio))
            meter_wh += max(0, energy_step)
            soc_percent = min(100, soc_percent + max(0, soc_step))
            await self._send_transaction_event(
                event_type="Updated",
                trigger_reason="MeterValuePeriodic",
                seq_no=session["seq_no"],
                transaction_id=transaction_id,
                evse_id=evse_id,
                meter_wh=meter_wh,
                soc_percent=soc_percent,
            )
            logger.info(
                f"[{self.id}] Charging transaction {transaction_id} on EVSE {evse_id}: "
                f"SoC={soc_percent}% assigned={assigned_amps}A"
            )
            session["seq_no"] += 1
            if soc_percent >= 100:
                logger.info(
                    f"[{self.id}] Auto-stopping transaction {transaction_id}: battery reached 100%."
                )
                session["stop_event"].set()

        end_reason = "RemoteStop"
        if soc_percent >= 100:
            end_reason = "ChargingStateChanged"
        await self._send_transaction_event(
            event_type="Ended",
            trigger_reason=end_reason,
            seq_no=session["seq_no"],
            transaction_id=transaction_id,
            evse_id=evse_id,
            meter_wh=meter_wh,
            soc_percent=soc_percent,
        )
        logger.info(f"[{self.id}] Session ended for transaction {transaction_id}.")
        self.active_sessions.pop(transaction_id, None)

    @on(Action.request_start_transaction)
    async def on_request_start_transaction(self, remote_start_id, evse_id=None, **kwargs):
        if evse_id is None:
            evse_id = 1

        transaction_id = f"TX-{uuid.uuid4().hex[:8]}"
        stop_event = asyncio.Event()
        self.active_sessions[transaction_id] = {
            "remote_start_id": remote_start_id,
            "evse_id": evse_id,
            "seq_no": 0,
            "stop_event": stop_event,
        }
        asyncio.create_task(self._charging_loop(transaction_id))
        logger.info(
            f"[{self.id}] Accepted remote start {remote_start_id}, transaction {transaction_id} on EVSE {evse_id}."
        )
        return call_result.RequestStartTransaction(status="Accepted")

    @on(Action.request_stop_transaction)
    async def on_request_stop_transaction(self, transaction_id, **kwargs):
        session = self.active_sessions.get(transaction_id)
        if not session:
            logger.warning(
                f"[{self.id}] Stop requested for unknown transaction {transaction_id}."
            )
            return call_result.RequestStopTransaction(status="Rejected")

        session["stop_event"].set()
        logger.info(f"[{self.id}] Accepted remote stop for transaction {transaction_id}.")
        return call_result.RequestStopTransaction(status="Accepted")


async def main():
    unique_id = f"CP_{uuid.uuid4().hex[:6]}"
    base_url = os.getenv("CSMS_URL", "ws://localhost:9000").rstrip("/")
    url = f"{base_url}/{unique_id}"
    retry_seconds = int(os.getenv("RETRY_SECONDS", "3"))
    logger.info(f"Charge point {unique_id} targeting Central System: {url}")

    while True:
        try:
            logger.info(f"[{unique_id}] Attempting WebSocket connection...")
            async with websockets.connect(url, subprotocols=["ocpp2.1"]) as ws:
                logger.info(f"[{unique_id}] WebSocket connected to {url}")

                # Create the CP instance with the unique ID from the URL path
                cp_id = url.split("/")[-1]
                charge_point = ChargePoint(cp_id, ws)

                await asyncio.gather(
                    charge_point.start(), charge_point.send_boot_notification()
                )
        except ConnectionRefusedError:
            logger.warning(
                f"[{unique_id}] CSMS not ready (connection refused). Retrying in {retry_seconds}s."
            )
        except OSError as e:
            logger.warning(
                f"[{unique_id}] Network/socket error ({e}). Retrying in {retry_seconds}s."
            )
        except Exception as e:
            logger.error(
                f"[{unique_id}] Unexpected error: {e}. Retrying in {retry_seconds}s."
            )

        await asyncio.sleep(retry_seconds)


if __name__ == "__main__":
    asyncio.run(main())
