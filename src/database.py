import duckdb
import pandas as pd


class StockDatabase:

  def __init__(self, db_path: str = "db/market_data.duckdb"):
    self.db_path = db_path
    self.conn = duckdb.connect(self.db_path)
    self._init_schema()

  def _init_schema(self) -> None:
    """Create target table and primary key for upserts."""
    self.conn.execute("""
            CREATE TABLE IF NOT EXISTS daily_prices (
                date DATE,
                ticker VARCHAR,
                open DOUBLE,
                high DOUBLE,
                low DOUBLE,
                close DOUBLE,
                volume BIGINT,
                PRIMARY KEY (ticker, date)
            )
        """)

  def upsert_prices(self, df: pd.DataFrame) -> int:
    """Insert or update price records idempotently."""
    if df.empty:
      return 0

    self.conn.register("df_view", df)
    query = """
            INSERT INTO daily_prices
            SELECT date, ticker, open, high, low, close, volume FROM df_view
            ON CONFLICT (ticker, date) DO UPDATE SET
                open = EXCLUDED.open,
                high = EXCLUDED.high,
                low = EXCLUDED.low,
                close = EXCLUDED.close,
                volume = EXCLUDED.volume
        """
    self.conn.execute(query)
    self.conn.unregister("df_view")
    return len(df)

  def close(self) -> None:
    self.conn.close()
