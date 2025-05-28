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
        now_et = pd.Timestamp.now(tz="US/Eastern")
        today = now_et.date().strftime("%Y-%m-%d")

        sched = nyse.schedule(start_date=today, end_date=today)

        if sched.empty:
            logging.info("Market closed today. Retrying in  5 min…")
            time.sleep(300)
            continue

        sched = sched.tz_localize("UTC")
        sched = sched.tz_convert("US/Eastern")

        mo_et = sched.iloc[0]["market_open"]
        mc_et = sched.iloc[0]["market_close"]

        if mo_et <= now_et <= mc_et:
            logging.info("Market is open. Starting WebSocket…")
            break
        else:
            logging.info("Outside market hours. Retrying in 5 min…")
            time.sleep(300)

    asyncio.run(ws_connect())


if __name__ == "__main__":
    main()
