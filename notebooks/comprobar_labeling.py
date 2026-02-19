import polars as pl

dataframe = pl.read_parquet("data/procesados/BTCUSDT_5m_processed.parquet")
print(dataframe.select(["timestamp", "close", "target"]).tail(15))
