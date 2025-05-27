import asyncio
import logging
import time
from datetime import datetime
import pandas_market_calendars as mcal
from dotenv import load_dotenv
import pandas as pd
load_dotenv()

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

from src.ws_client import ws_connect  # noqa: E402


def main():
    nyse = mcal.get_calendar("NYSE")
    
    while True:
        now_ts = pd.Timestamp.now(tz="US/Eastern")
        today = now_ts.date()

        sched = nyse.schedule(
            start_date=today,
            end_date=today
        )

        is_open = False
        if not sched.empty:
            is_open = nyse.open_at_time(sched, now_ts)

        if is_open:
            logger.info("Market is open, starting WebSocket connection.")
            break
        else:
            logger.info("Market is closed, retrying in 5 minutes…")
            time.sleep(300)

    asyncio.run(ws_connect())


if __name__ == "__main__":
    main()
