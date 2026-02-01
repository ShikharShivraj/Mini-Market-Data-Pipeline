CANON_COLS = [
    "ts_recv_ms",
    "ts_event_ms",
    "symbol",
    "event_type",
    "bid",
    "bid_sz",
    "ask",
    "ask_sz",
    "update_id",
    "trade_px",
    "trade_sz",
    "trade_id",
]

DTYPES: dict[str, str] = {
    "ts_recv_ms": "Int64",
    "ts_event_ms": "Int64",
    "symbol": "string",
    "event_type": "string",
    "bid": "float64",
    "bid_sz": "float64",
    "ask": "float64",
    "ask_sz": "float64",
    "update_id": "Int64",
    "trade_px": "float64",
    "trade_sz": "float64",
    "trade_id": "Int64",
}

EVENT_TYPES = ["quote", "trade"]

EVENT_TYPE_RANK = {"quote": 0, "trade": 1}
