import asyncio
import logging

from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

from src.ws_client import ws_connect  # noqa: E402


def main():
    asyncio.run(ws_connect())


if __name__ == "__main__":
    main()
