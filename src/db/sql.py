INSERT_SYMBOL_SQL = """
INSERT INTO symbols (symbol, root, expiration, call_putt, strike)
VALUES ($1, $2, $3, $4, $5)
ON CONFLICT (symbol) DO NOTHING
"""

# Insert a trade
INSERT_TRADE_SQL = """
INSERT INTO trades (symbol, time, bid, ask, trade_price, trade_size, trade_condition)
VALUES ($1, $2, $3, $4, $5, $6, $7)
"""
