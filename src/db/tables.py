import logging
import os

from sqlalchemy import DateTime, Float, ForeignKey, Integer, String, create_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

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
    symbol: Mapped[str] = mapped_column(ForeignKey("symbols.symbol"), primary_key=True)
    sequence: Mapped[int] = mapped_column(Integer, primary_key=True)
    time: Mapped[DateTime] = mapped_column(DateTime)
    bid: Mapped[float] = mapped_column(Float)
    ask: Mapped[float] = mapped_column(Float)
    trade_price: Mapped[float] = mapped_column(Float)
    trade_size: Mapped[int] = mapped_column(Integer)
    trade_condition: Mapped[str] = mapped_column(String)


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
