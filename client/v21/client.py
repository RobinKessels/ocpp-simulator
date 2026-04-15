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
        request = call.Heartbeat()
        while True:
            await self.call(request)
            await asyncio.sleep(interval)

    async def send_boot_notification(self):
        print("Sending boot notification..")
        request = call.BootNotification(
            charging_station=ChargingStationType(
                model="Wallbox XYZ", vendor_name="acme"
            ),
            reason="PowerUp",
        )
        response = await self.call(request)

        if response.status == "Accepted":
            print("Connected to central system.")
            await self.send_heartbeat(response.interval)


async def main():
    url = os.getenv("CSMS_URL", "ws://localhost:9000/CP_1")
    logger.info(f"Connecting to Central System at: {url}")
    try:
        async with websockets.connect(url, subprotocols=["ocpp2.1"]) as ws:
            logger.info("WebSocket connection established.")
            charge_point = ChargePoint("CP_1", ws)
            await asyncio.gather(
                charge_point.start(), charge_point.send_boot_notification()
            )
    except ConnectionRefusedError:
        logger.error(f"Connection refused. Is the server running at {url}?")
    except Exception as e:
        logger.error(f"Connection failed: {e}")


if __name__ == "__main__":
    asyncio.run(main())
