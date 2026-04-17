INSERT_SYMBOL_SQL = """
INSERT INTO symbols (symbol, root, expiration, call_putt, strike)
VALUES ($1, $2, $3, $4, $5)
ON CONFLICT (symbol) DO NOTHING
"""

# Insert a trade and return its database id
INSERT_TRADE_SQL = """
INSERT INTO trades (symbol, time, bid, ask, trade_price, trade_size, trade_condition)
VALUES ($1, $2, $3, $4, $5, $6, $7)
RETURNING trade_id
"""

INSERT_ANOMALY_SCORE_SQL = """
INSERT INTO anomaly_scores (
	trade_id,
	symbol,
	root,
	anomaly_label,
	anomaly_score,
	model_version,
	inferred_at
)
VALUES ($1, $2, $3, $4, $5, $6, $7)
"""

INSERT_TRADE_EVENT_SQL = """
INSERT INTO trade_events (
	symbol,
	root,
	expiration,
	call_putt,
	strike,
	event_start,
	event_end,
	duration_seconds,
	n_trades,
	total_size,
	mean_anomaly_score,
	worst_anomaly_score,
	dominant_trade_condition,
	model_version,
	finalized_at
)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
"""

INSERT_OPTION_CHAIN_SNAPSHOT_SQL = """
INSERT INTO option_chain_snapshots (
	symbol,
	root,
	expiration,
	call_putt,
	strike,
	snapshot_minute,
	underlying_price,
	bid,
	ask,
	last_price,
	mark_price,
	bid_size,
	ask_size,
	volume,
	open_interest,
	implied_volatility,
	delta,
	gamma,
	theta,
	vega,
	rho,
	intrinsic_value,
	extrinsic_value,
	updated_at
)
VALUES (
	$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12,
	$13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24
)
ON CONFLICT (symbol, snapshot_minute) DO UPDATE SET
	root = EXCLUDED.root,
	expiration = EXCLUDED.expiration,
	call_putt = EXCLUDED.call_putt,
	strike = EXCLUDED.strike,
	underlying_price = EXCLUDED.underlying_price,
	bid = EXCLUDED.bid,
	ask = EXCLUDED.ask,
	last_price = EXCLUDED.last_price,
	mark_price = EXCLUDED.mark_price,
	bid_size = EXCLUDED.bid_size,
	ask_size = EXCLUDED.ask_size,
	volume = EXCLUDED.volume,
	open_interest = EXCLUDED.open_interest,
	implied_volatility = EXCLUDED.implied_volatility,
	delta = EXCLUDED.delta,
	gamma = EXCLUDED.gamma,
	theta = EXCLUDED.theta,
	vega = EXCLUDED.vega,
	rho = EXCLUDED.rho,
	intrinsic_value = EXCLUDED.intrinsic_value,
	extrinsic_value = EXCLUDED.extrinsic_value,
	updated_at = EXCLUDED.updated_at
"""
