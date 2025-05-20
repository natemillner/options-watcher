import logging
import os

import requests

logger = logging.getLogger(__name__)


def get_session_id() -> str:
    """
    Get the session ID from the Tradier API using the provided token.

    Returns:
        str: The session ID.
    """

    headers = {
        "Authorization": f"Bearer {os.getenv('TRADIER_API_KEY')}",
        "Accept": "application/json",
    }
    logger.info("Getting session ID from Tradier API")
    response = requests.post(
        "https://api.tradier.com/v1/markets/events/session", headers=headers, data={}
    )

    if response.status_code == 200:
        return response.json()["stream"]["sessionid"]
    else:
        raise Exception(
            f"Failed to get session ID: {response.status_code} - {response.text}"
        )
