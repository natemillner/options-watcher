import logging
import os

from sqlalchemy import (
    Boolean,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    String,
    UniqueConstraint,
    create_engine,
    BigInteger,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy.dialects.postgresql import TIMESTAMP

logger = logging.getLogger(__name__)


class Base(DeclarativeBase):
    pass


def create_tables() -> None:
    """
    Create the database tables using sqlalchemy
    """
    engine = create_engine(
        f"postgresql://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}@{os.getenv('POSTGRES_HOST')}:{os.getenv('POSTGRES_PORT')}/{os.getenv('POSTGRES_NAME')}"
    )
    Base.metadata.create_all(engine)
    engine.dispose()


class Symbols(Base):
    __tablename__ = "symbols"
    symbol: Mapped[str] = mapped_column(String, primary_key=True)
    root: Mapped[str] = mapped_column(String)
    expiration: Mapped[DateTime] = mapped_column(DateTime)
    call_putt: Mapped[str] = mapped_column(String)
    strike: Mapped[float] = mapped_column(Float)


class Trades(Base):
    __tablename__ = "trades"
    trade_id: Mapped[int] = mapped_column(
        BigInteger, primary_key=True, autoincrement=True
    )
    symbol: Mapped[str] = mapped_column(
        ForeignKey("symbols.symbol"), nullable=False, index=True
    )
    time: Mapped[TIMESTAMP] = mapped_column(
        TIMESTAMP(precision=6), nullable=False, index=True
    )
    bid: Mapped[float] = mapped_column(Float)
    ask: Mapped[float] = mapped_column(Float)
    trade_price: Mapped[float] = mapped_column(Float)
    trade_size: Mapped[int] = mapped_column(Integer)
    trade_condition: Mapped[str] = mapped_column(String)


class AnomalyScores(Base):
    __tablename__ = "anomaly_scores"
    anomaly_id: Mapped[int] = mapped_column(
        BigInteger, primary_key=True, autoincrement=True
    )
    trade_id: Mapped[int] = mapped_column(
        ForeignKey("trades.trade_id"), nullable=False, index=True
    )
    symbol: Mapped[str] = mapped_column(
        ForeignKey("symbols.symbol"), nullable=False, index=True
    )
    root: Mapped[str] = mapped_column(String, nullable=False, index=True)
    anomaly_label: Mapped[bool] = mapped_column(Boolean, nullable=False)
    anomaly_score: Mapped[float] = mapped_column(Float, nullable=False, index=True)
    model_version: Mapped[str] = mapped_column(String, nullable=False)
    inferred_at: Mapped[TIMESTAMP] = mapped_column(
        TIMESTAMP(precision=6), nullable=False, index=True
    )


class TradeEvents(Base):
    __tablename__ = "trade_events"
    event_id: Mapped[int] = mapped_column(
        BigInteger, primary_key=True, autoincrement=True
    )
    symbol: Mapped[str] = mapped_column(
        ForeignKey("symbols.symbol"), nullable=False, index=True
    )
    root: Mapped[str] = mapped_column(String, nullable=False, index=True)
    expiration: Mapped[DateTime] = mapped_column(DateTime, nullable=False)
    call_putt: Mapped[str] = mapped_column(String, nullable=False)
    strike: Mapped[float] = mapped_column(Float, nullable=False)
    event_start: Mapped[TIMESTAMP] = mapped_column(
        TIMESTAMP(precision=6), nullable=False, index=True
    )
    event_end: Mapped[TIMESTAMP] = mapped_column(
        TIMESTAMP(precision=6), nullable=False, index=True
    )
    duration_seconds: Mapped[int] = mapped_column(Integer, nullable=False)
    n_trades: Mapped[int] = mapped_column(Integer, nullable=False)
    total_size: Mapped[int] = mapped_column(Integer, nullable=False)
    mean_anomaly_score: Mapped[float] = mapped_column(Float, nullable=False)
    worst_anomaly_score: Mapped[float] = mapped_column(Float, nullable=False)
    dominant_trade_condition: Mapped[str] = mapped_column(String, nullable=False)
    model_version: Mapped[str] = mapped_column(String, nullable=False)
    finalized_at: Mapped[TIMESTAMP] = mapped_column(
        TIMESTAMP(precision=6), nullable=False, index=True
    )


class OptionChainSnapshot(Base):
    __tablename__ = "option_chain_snapshots"
    __table_args__ = (
        UniqueConstraint("symbol", "snapshot_minute", name="uq_option_chain_symbol_minute"),
    )

    snapshot_id: Mapped[int] = mapped_column(
        BigInteger, primary_key=True, autoincrement=True
    )
    symbol: Mapped[str] = mapped_column(
        ForeignKey("symbols.symbol"), nullable=False, index=True
    )
    root: Mapped[str] = mapped_column(String, nullable=False, index=True)
    expiration: Mapped[DateTime] = mapped_column(DateTime, nullable=False)
    call_putt: Mapped[str] = mapped_column(String, nullable=False)
    strike: Mapped[float] = mapped_column(Float, nullable=False)
    snapshot_minute: Mapped[TIMESTAMP] = mapped_column(
        TIMESTAMP(precision=6), nullable=False, index=True
    )
    underlying_price: Mapped[float | None] = mapped_column(Float, nullable=True)
    bid: Mapped[float | None] = mapped_column(Float, nullable=True)
    ask: Mapped[float | None] = mapped_column(Float, nullable=True)
    last_price: Mapped[float | None] = mapped_column(Float, nullable=True)
    mark_price: Mapped[float | None] = mapped_column(Float, nullable=True)
    bid_size: Mapped[int | None] = mapped_column(Integer, nullable=True)
    ask_size: Mapped[int | None] = mapped_column(Integer, nullable=True)
    volume: Mapped[int | None] = mapped_column(Integer, nullable=True)
    open_interest: Mapped[int | None] = mapped_column(Integer, nullable=True)
    implied_volatility: Mapped[float | None] = mapped_column(Float, nullable=True)
    delta: Mapped[float | None] = mapped_column(Float, nullable=True)
    gamma: Mapped[float | None] = mapped_column(Float, nullable=True)
    theta: Mapped[float | None] = mapped_column(Float, nullable=True)
    vega: Mapped[float | None] = mapped_column(Float, nullable=True)
    rho: Mapped[float | None] = mapped_column(Float, nullable=True)
    intrinsic_value: Mapped[float | None] = mapped_column(Float, nullable=True)
    extrinsic_value: Mapped[float | None] = mapped_column(Float, nullable=True)
    updated_at: Mapped[TIMESTAMP] = mapped_column(
        TIMESTAMP(precision=6), nullable=False, index=True
    )


class Summary(Base):
    __tablename__ = "summary"
    symbol: Mapped[str] = mapped_column(ForeignKey("symbols.symbol"), primary_key=True)
    time: Mapped[DateTime] = mapped_column(DateTime, primary_key=True)
    open_price: Mapped[float] = mapped_column(Float)
    high_price: Mapped[float] = mapped_column(Float)
    low_price: Mapped[float] = mapped_column(Float)
    close_price: Mapped[float] = mapped_column(Float)


logger.info("Creating tables")
create_tables()
