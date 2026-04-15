import asyncio
import logging
import websockets
from datetime import datetime, timezone

from ocpp.routing import on
from ocpp.v16 import ChargePoint as cp
from ocpp.v16 import call_result
from ocpp.v16.enums import Action, RegistrationStatus

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("ocpp_server")


class MyChargePoint(cp):
    @on(Action.boot_notification)
    async def on_boot_notification(
        self, charge_point_vendor, charge_point_model, **kwargs
    ):
        logger.info(
            f"[{self.id}] Received BootNotification from {charge_point_vendor} ({charge_point_model})"
        )

        return call_result.BootNotification(
            current_time=datetime.now(tz=timezone.utc).isoformat(),
            interval=10,
            status=RegistrationStatus.accepted,
        )


async def on_connect(connection: websockets.ServerConnection):
    """
    Triggers whenever a new WebSocket connection is established.
    """
    path = connection.request.path
    charge_point_id = path.split("/")[-1]

    logger.info(
        f"New connection detected on path: {path} | Assigned ID: {charge_point_id}"
    )

    charge_point = MyChargePoint(charge_point_id, connection)

    try:
        await charge_point.start()
    except websockets.exceptions.ConnectionClosed:
        logger.warning(f"[{charge_point_id}] Connection closed by client.")
    except Exception as e:
        logger.error(f"[{charge_point_id}] An unexpected error occurred: {e}")


async def main():
    logger.info("Starting OCPP Server on 0.0.0.0:9000...")

    async with websockets.serve(
        on_connect,
        "0.0.0.0",
        9000,
        subprotocols=["ocpp1.6", "ocpp2.0.1"],
    ) as server:
        logger.info("Server is live and listening for chargers.")
        await server.wait_closed()


if __name__ == "__main__":
    asyncio.run(main())
