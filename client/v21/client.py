import asyncio
import logging
import websockets
import os

from ocpp.v21 import ChargePoint as cp
from ocpp.v21 import call
from ocpp.v21.datatypes import ChargingStationType

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("charge_point")


class ChargePoint(cp):
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


async def main():
    unique_id = os.getenv("HOSTNAME") or socket.gethostname() or "unknown_cp"
    base_url = os.getenv("CSMS_URL", "ws://localhost:9000/CP_1")
    url = f"{base_url}/{unique_id}"
    logger.info(f"Attempting connection to Central System: {url}")

    try:
        async with websockets.connect(url, subprotocols=["ocpp2.1"]) as ws:
            logger.info(f"WebSocket connected to {url}")

            # Create the CP instance with the unique ID from the URL path
            cp_id = url.split("/")[-1]
            charge_point = ChargePoint(cp_id, ws)

            await asyncio.gather(
                charge_point.start(), charge_point.send_boot_notification()
            )
    except ConnectionRefusedError:
        logger.error(f"Connection refused. Ensure the CSMS is running at {url}")
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")


if __name__ == "__main__":
    asyncio.run(main())
