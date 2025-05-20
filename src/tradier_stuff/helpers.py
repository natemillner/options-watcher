import re
from datetime import datetime


def parse_occ_symbol(symbol: str) -> dict:
    """
    Parse a full OCC-formatted option symbol using regex.
    Works with variable-length underlying tickers.

    Example:
    'RIOT250425C00007500' -> {
        'underlying': 'RIOT',
        'expiration': datetime.date(2025, 4, 25),
        'type': 'call',
        'strike': 7.5
    }
    """
    pattern = r"^(?P<underlying>[A-Z]+)(?P<date>\d{6})(?P<type>[CP])(?P<strike>\d{8})$"
    match = re.match(pattern, symbol)

    if not match:
        raise ValueError(f"Invalid OCC symbol format: {symbol}")

    groups = match.groupdict()
    expiration = datetime.strptime(groups["date"], "%y%m%d").date()
    option_type = "call" if groups["type"] == "C" else "put"
    strike = int(groups["strike"]) / 1000

    return {
        "symbol": symbol,
        "root": groups["underlying"],
        "expiration": expiration,
        "call_putt": option_type,
        "strike": strike,
    }
