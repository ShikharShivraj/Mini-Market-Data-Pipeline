"""Goal Create a static tick dataset by capturing public Binance market-data streams for
 BTCUSDT (best bid/ask quotes + aggregated trades) and writing them to a Parquet file.

Deliverables src/capture.py that records for N seconds and writes Parquet; includes reconnect and clean shutdown.


{"stream":"btcusdt@aggTrade","data":{"e":"aggTrade","E":1769224657584,"s":"BTCUSDT","a":3821253461,"p":"89879.78000000","q":"0.00068000","f":5808851364,"l":5808851364,"T":1769224657583,"m":true,"M":true}}
{"stream":"btcusdt@bookTicker","data":{"u":85592068563,"s":"BTCUSDT","b":"89879.78000000","B":"4.93986000","a":"89879.79000000","A":"2.11027000"}}
{"stream":"btcusdt@aggTrade","data":{"e":"aggTrade","E":1769224658079,"s":"BTCUSDT","a":3821253462,"p":"89879.79000000","q":"0.00222000","f":5808851365,"l":5808851365,"T":1769224658079,"m":false,"M":true}}
"""

import argparse
from pathlib import Path
import time
from websockets.sync.client import connect
import logging
import json
from typing import TypedDict, Optional
from dataclasses import dataclass
import sys
import pyarrow as pa
import pyarrow.parquet as pq


BASE_URL = "wss://stream.binance.com:9443"
EVENT_TYPE_DICT = {"bookTicker": "quote", "aggTrade": "trade"}
BATCH_ROWS = 1000

RAW_TICK_SCHEMA = pa.schema(
    [
        ("ts_event_ms", pa.int64()),
        ("ts_recv_ms", pa.int64()),
        ("symbol", pa.string()),
        ("event_type", pa.string()),
        ("bid", pa.float64()),
        ("bid_sz", pa.float64()),
        ("ask", pa.float64()),
        ("ask_sz", pa.float64()),
        ("update_id", pa.int64()),
        ("trade_px", pa.float64()),
        ("trade_sz", pa.float64()),
        ("trade_id", pa.int64()),
    ]
)


@dataclass(frozen=True)
class CaptureConfig:
    symbol: str
    output_path: Path
    duration: int
    base_url: str = BASE_URL
    batch_rows: int = BATCH_ROWS

    @property
    def stream_book_ticker(self) -> str:
        return f"{self.symbol}@bookTicker"

    @property
    def stream_agg_trade(self) -> str:
        return f"{self.symbol}@aggTrade"

    @property
    def combined_stream_url(self) -> str:
        return f"{self.base_url}/stream?streams={self.stream_book_ticker}/{self.stream_agg_trade}"


class RowSchema(TypedDict):
    ts_recv_ms: int
    ts_event_ms: int
    symbol: str
    event_type: str
    bid: float | None
    bid_sz: float | None
    ask: float | None
    ask_sz: float | None
    update_id: int | None
    trade_px: float | None
    trade_sz: float | None
    trade_id: int | None


class CaptureTickData:

    def __init__(self, config: CaptureConfig, logger: logging.Logger) -> None:
        self.buffer: list[RowSchema] = []
        self.logger = logger
        self.cfg = config
        self._writer: pq.ParquetWriter | None = None
        self._tmp_out: Path = self.cfg.output_path.with_name(
            self.cfg.output_path.name + ".tmp"
        )

    def connnector(self) -> None:
        start_time = time.time()
        end_time = start_time + self.cfg.duration
        now = start_time
        backoff_sec = 0.5
        max_backoff_sec = 8.0
        attempt = 0

        try:
            while True:
                now = time.time()
                if now >= end_time:
                    self.logger.info("Capture duration reached; stopping.")
                    break

                try:
                    with connect(self.cfg.combined_stream_url) as ws:
                        self.logger.info("Connected to WebSocket.")
                        # Reset backoff after a successful connect
                        backoff_sec = 0.5
                        attempt = 0

                        while True:
                            now = time.time()
                            if now >= end_time:
                                self.logger.info(
                                    "Capture duration reached; closing connection."
                                )
                                break

                            # Blocking receive; Binance is high-frequency so this should return often.
                            response = ws.recv()
                            self.validator(
                                response,
                                self.cfg.stream_book_ticker,
                                self.cfg.stream_agg_trade,
                            )

                except KeyboardInterrupt:
                    self.logger.info("KeyboardInterrupt received; shutting down.")
                    break

                except Exception as e:
                    attempt += 1
                    remaining = end_time - time.time()
                    if remaining <= 0:
                        self.logger.warning(
                            "Time is up during reconnect attempts; stopping."
                        )
                        break

                    sleep_for = min(backoff_sec, max_backoff_sec, remaining)
                    self.logger.warning(
                        "WebSocket error (attempt %d): %s. Reconnecting in %.2fs",
                        attempt,
                        e,
                        sleep_for,
                    )
                    time.sleep(sleep_for)
                    backoff_sec = min(backoff_sec * 2.0, max_backoff_sec)

        finally:
            # Best-effort final flush (don’t lose the last partial batch)
            try:
                if self.buffer:
                    self.logger.info(
                        "Final flush of %d buffered rows.", len(self.buffer)
                    )
                    self.flush_batch_to_parquet()
            except Exception as e:
                self.logger.exception("Final flush failed: %s", e)
            try:
                if self._writer is not None:
                    self._writer.close()
                    self._writer = None

                    # Atomic rename: tmp -> final output path
                    self._tmp_out.replace(self.cfg.output_path)

                    self.logger.info("Wrote parquet to %s", self.cfg.output_path)
            except Exception as e:
                self.logger.exception("Failed to finalize parquet output: %s", e)

    def handler_book_ticker(self, row: dict[str, dict[str, str]]) -> RowSchema | None:
        if "data" not in row:
            return None

        payload = row["data"]
        required_keys = ["b", "B", "a", "A", "u"]

        for req_key in required_keys:
            if req_key not in payload:
                return None

        try:
            now = int(time.time() * 1000)
            bid = float(payload["b"])
            bid_sz = float(payload["B"])
            ask = float(payload["a"])
            ask_sz = float(payload["A"])
            update_id = int(payload["u"])

            res_dict: RowSchema = {
                "ts_recv_ms": now,
                "ts_event_ms": now,
                "symbol": self.cfg.symbol,
                "event_type": EVENT_TYPE_DICT["bookTicker"],
                "bid": bid,
                "bid_sz": bid_sz,
                "ask": ask,
                "ask_sz": ask_sz,
                "update_id": update_id,
                "trade_px": None,
                "trade_id": None,
                "trade_sz": None,
            }

            return res_dict

        except Exception as e:
            self.logger.exception("Type conversion failed %s", e)
            return None

    def handler_agg_trade(self, row: dict[str, dict[str, str]]) -> RowSchema | None:
        if "data" not in row:
            return None

        payload = row["data"]

        required_keys = ["E", "p", "q", "a"]
        for req_key in required_keys:
            if req_key not in payload:
                return None

        try:
            now = int(time.time() * 1000)
            event_ts = int(payload["E"])
            trade_px = float(payload["p"])
            trade_sz = float(payload["q"])
            trade_id = int(payload["a"])

            res_dict: RowSchema = {
                "ts_recv_ms": now,
                "ts_event_ms": event_ts,
                "symbol": self.cfg.symbol,
                "event_type": EVENT_TYPE_DICT["aggTrade"],
                "bid": None,
                "bid_sz": None,
                "ask": None,
                "ask_sz": None,
                "update_id": None,
                "trade_px": trade_px,
                "trade_id": trade_id,
                "trade_sz": trade_sz,
            }

            return res_dict

        except Exception as e:
            self.logger.exception("Type conversion failed %s", e)
            return None

    def validator(
        self, response: str | bytes, stream_book_ticker: str, stream_agg_trade: str
    ) -> None:
        if response:
            row = json.loads(response)
            if row and row["stream"] == stream_book_ticker:
                res = self.handler_book_ticker(row)
                if res:
                    self.buffer.append(res)

            elif row and row["stream"] == stream_agg_trade:
                res = self.handler_agg_trade(row)
                if res:
                    self.buffer.append(res)

            if len(self.buffer) >= self.cfg.batch_rows:
                self.flush_batch_to_parquet()

    def flush_batch_to_parquet(self) -> None:
        if not self.buffer:
            return

        table = pa.Table.from_pylist(self.buffer, schema=RAW_TICK_SCHEMA)
        if self._writer is None:
            self.cfg.output_path.parent.mkdir(parents=True, exist_ok=True)

            if self._tmp_out.exists():
                self._tmp_out.unlink()

            self._writer = pq.ParquetWriter(
                where=str(self._tmp_out),
                schema=RAW_TICK_SCHEMA,
                compression="snappy",
                use_dictionary=True,
            )

        self._writer.write_table(table)
        self.buffer.clear()
        return


def setup_logger(
    name: str, level: int = logging.INFO, log_file: Optional[Path] = None
) -> logging.Logger:
    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.propagate = False

    if logger.handlers:
        logger.handlers.clear()

    fmt = logging.Formatter(
        fmt="%(asctime)s.%(msecs)03d | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    sh = logging.StreamHandler(sys.stdout)
    sh.setLevel(level)
    sh.setFormatter(fmt)
    logger.addHandler(sh)

    if log_file is not None:
        log_file.parent.mkdir(parents=True, exist_ok=True)
        fh = logging.FileHandler(log_file, encoding="utf-8")
        fh.setLevel(level)
        fh.setFormatter(fmt)
        logger.addHandler(fh)

    return logger


def main():

    parser = argparse.ArgumentParser()
    parser.add_argument("--symbol", help="Trade symbol", required=True)
    parser.add_argument(
        "--duration_sec",
        help="Time for which to stream in seconds",
        type=int,
        required=True,
    )
    parser.add_argument(
        "--out",
        help="Output file path for parquet file",
        type=Path,
        required=True,
    )

    args = parser.parse_args()

    symbol = args.symbol.lower()
    duration = args.duration_sec
    output_path = args.out

    config = CaptureConfig(symbol=symbol, output_path=output_path, duration=duration)

    logger = setup_logger(name="CapturetickLogger")
    logger.info("Start of logging")

    obj = CaptureTickData(
        config,
        logger=logger,
    )
    obj.connnector()


if __name__ == "__main__":
    main()
