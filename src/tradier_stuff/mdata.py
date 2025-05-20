import logging
import os
from datetime import date

import requests

from .helpers import parse_occ_symbol

logger = logging.getLogger(__name__)


def get_options_symbols(root: str) -> list[str]:
    """
    Get the list of options symbols from the Tradier API.

    Args:
        root (str): The root URL for the Tradier API.

    Returns:
        list[str]: A list of options symbols.
    """
    headers = {
        "Authorization": f"Bearer {os.getenv('TRADIER_API_KEY')}",
        "Accept": "application/json",
    }
    params = {
        "underlying": root,
    }
    logger.info(f"Getting options symbols for {root} from Tradier API")
    response = requests.get(
        "https://api.tradier.com/v1/markets/options/lookup",
        headers=headers,
        params=params,
    )
    if response.status_code == 200:
        response = response.json().get("symbols", [])
        if response:
            return response[0].get("options", [])
        else:
            return []
    else:
        logger.error(
            f"Failed to get options symbols: {response.status_code} - {response.text}"
        )
        raise Exception(
            f"Failed to get options symbols: {response.status_code} - {response.text}"
        )


def filter_options_symbols(
    symbols: list[str], max_expiration: date, strike_range: int, target_strike: float
) -> list[str]:
    """
    Filter the options symbols based on the provided filter string.

    Args:
        symbols (list[str]): The list of options symbols.
        filter_str (str): The filter string.

    Returns:
        list[str]: A list of filtered options symbols.
    """
    logger.info(
        f"Filtering options symbols with max expiration {max_expiration} and strike range {strike_range}"
    )
    filtered_symbols = []
    for symbol in symbols:
        parsed_symbol = parse_occ_symbol(symbol)
        if (
            parsed_symbol["expiration"] <= max_expiration
            and abs(parsed_symbol["strike"] - target_strike) <= strike_range
        ):
            filtered_symbols.append(symbol)
    return filtered_symbols


def get_price(symbol: str) -> float:
    """
    Get the price of the underlying asset.

    Args:
        symbol (str): The symbol of the underlying asset.

    Returns:
        float: The price of the underlying asset.
    """
    headers = {
        "Authorization": f"Bearer {os.getenv('TRADIER_API_KEY')}",
        "Accept": "application/json",
    }
    params = {
        "symbols": symbol,
        "greeks": "false",
    }
    logger.info(f"Getting price for {symbol} from Tradier API")
    response = requests.get(
        "https://api.tradier.com/v1/markets/quotes", headers=headers, params=params
    )
    if response.status_code == 200:
        response = response.json().get("quotes", {}).get("quote", {})
        if response:
            return response.get("last")
        else:
            return None
    else:
        raise Exception(
            f"Failed to get price: {response.status_code} - {response.text}"
        )
