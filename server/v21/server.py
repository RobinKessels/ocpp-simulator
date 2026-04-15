import asyncio
import logging
import websockets
from datetime import datetime, timezone

from ocpp.routing import on
from ocpp.v21 import ChargePoint as cp
from ocpp.v21 import call_result
from ocpp.v21.enums import Action

# 1. Configure logging for the entire application
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("ocpp_server")


class ChargePoint(cp):
    @on(Action.boot_notification)
    async def on_boot_notification(self, charging_station, reason, **kwargs):
        # Extracting info from the v2.1 nested charging_station object
        vendor = charging_station.get("vendor_name", "Unknown")
        model = charging_station.get("model", "Unknown")

        logger.info(
            f"[{self.id}] Received BootNotification from {vendor} ({model}) | Reason: {reason}"
        )

        return call_result.BootNotification(
            current_time=datetime.now(timezone.utc).isoformat(),
            interval=10,
            status="Accepted",
        )

    @on(Action.heartbeat)
    async def on_heartbeat(self):
        logger.info(f"[{self.id}] Heartbeat received.")
        return call_result.Heartbeat(
            current_time=datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S") + "Z"
        )


async def on_connect(websocket):
    """
    Handles new WebSocket connections and negotiates the subprotocol.
    """
    try:
        requested_protocols = websocket.request.headers.get(
            "Sec-WebSocket-Protocol", "None"
        )
    except Exception:
        requested_protocols = "None"

    if websocket.subprotocol:
        logger.info(
            f"Connection established using subprotocol: {websocket.subprotocol}"
        )
    else:
        logger.warning(
            f"Protocol Mismatch! Client requested: {requested_protocols} | "
            f"Server supports: {websocket.available_subprotocols}. Closing connection."
        )
        return await websocket.close()

    # Extract ID from the URL path (e.g., /CP_1 -> CP_1)
    charge_point_id = websocket.request.path.strip("/")
    charge_point = ChargePoint(charge_point_id, websocket)

    try:
        logger.info(f"[{charge_point_id}] Starting OCPP session...")
        await charge_point.start()
    except websockets.exceptions.ConnectionClosed:
        logger.info(f"[{charge_point_id}] Client disconnected.")
    except Exception as e:
        logger.error(f"[{charge_point_id}] Error during session: {e}")


async def main():
    server = await websockets.serve(
        on_connect, "0.0.0.0", 9000, subprotocols=["ocpp2.1"]
    )

    logger.info("OCPP 2.1 Central System started on ws://0.0.0.0:9000")
    await server.wait_closed()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Server shut down manually.")
