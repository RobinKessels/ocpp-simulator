import asyncio
import os
import logging
import websockets

from ocpp.v16 import ChargePoint as cp
from ocpp.v16 import call, call_result
from ocpp.v16.enums import RegistrationStatus

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("charge_point")


class ChargePoint(cp):
    async def send_boot_notification(self):
        logger.info(f"[{self.id}] Sending BootNotification to Central System...")

        request = call.BootNotification(
            charge_point_model="Wallbox XYZ",
            charge_point_vendor="acme",
        )

        try:
            response: call_result.BootNotification = await self.call(request)

            if response.status == RegistrationStatus.accepted:
                logger.info(
                    f"[{self.id}] BootNotification ACCEPTED. Central System time: {response.current_time}"
                )
                print("Connected to central system.")
            else:
                logger.warning(
                    f"[{self.id}] BootNotification REJECTED. Status: {response.status}"
                )
        except Exception as e:
            logger.error(f"[{self.id}] Error during BootNotification: {e}")


async def main():
    url = os.getenv("CSMS_URL", "ws://localhost:9000/CP_1")
    logger.info(f"Connecting to Central System at: {url}")

    try:
        async with websockets.connect(url, subprotocols=["ocpp1.6"]) as ws:
            logger.info("WebSocket connection established.")
            charge_point = ChargePoint("CP_1", ws)

            await asyncio.gather(
                charge_point.start(),
                charge_point.send_boot_notification(),
            )
    except ConnectionRefusedError:
        logger.error(f"Connection refused. Is the server running at {url}?")
    except Exception as e:
        logger.error(f"Connection failed: {e}")


if __name__ == "__main__":
    asyncio.run(main())
