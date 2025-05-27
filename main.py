import asyncio
import logging
import time
from datetime import datetime
import pandas_market_calendars as mcal
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

from src.ws_client import ws_connect  # noqa: E402


def main():
    nyse = mcal.get_calendar("NYSE")
    while True:
        now = datetime.now()
        if nyse.is_open_at_time(now):
            logger.info("Market is open, starting WebSocket connection.")
            break
        else:
            logger.info("Market is closed, waiting for 5 minutes before retrying.")
            time.sleep(300)

    asyncio.run(ws_connect())


if __name__ == "__main__":
    main()
