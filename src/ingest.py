import argparse
from pathlib import Path
import pandas as pd
from .schema import CANON_COLS, DTYPES, EVENT_TYPES, EVENT_TYPE_RANK


def reader(filepath: Path) -> pd.DataFrame:
    dataset = pd.read_parquet(filepath)
    return dataset


def normalize(df: pd.DataFrame) -> pd.DataFrame:
    res = df.reindex(columns=CANON_COLS).astype(DTYPES)

    res["event_type"] = res["event_type"].str.lower()
    res = res.loc[res["event_type"].isin(EVENT_TYPES)].copy()

    is_quote = res["event_type"].eq("quote")
    is_trade = res["event_type"].eq("trade")

    trade_cols = ["trade_px", "trade_sz", "trade_id"]
    quote_cols = ["bid", "ask", "bid_sz", "ask_sz", "update_id"]

    res.loc[is_quote, trade_cols] = pd.NA
    res.loc[is_trade, quote_cols] = pd.NA

    res.loc[is_quote, "ts_event_ms"] = res.loc[is_quote, "ts_event_ms"].fillna(
        res.loc[is_quote, "ts_recv_ms"]
    )

    res["event_type_rank"] = res["event_type"].map(EVENT_TYPE_RANK).astype("int64")
    res["id_key"] = res["update_id"].where(is_quote, res["trade_id"])

    res = res.sort_values(
        by=["symbol", "ts_event_ms", "event_type_rank", "id_key"],
        kind="mergesort",
    )

    res = res.drop(columns=["event_type_rank", "id_key"])

    return res


def writer(df: pd.DataFrame, filepath: Path) -> None:
    filepath.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(filepath, index=False)
    return


def main():
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="cmd", required=True)

    normalize_parser = subparsers.add_parser(
        "normalize", help="CLI subcommand to enforce canonical data organization"
    )
    normalize_parser.add_argument(
        "--in", help="Input parquet path", dest="in_path", type=Path, required=True
    )
    normalize_parser.add_argument(
        "--out", help="Output parquet path", dest="out_path", type=Path, required=True
    )

    args = parser.parse_args()
    if args.cmd == "normalize":
        raw_data = reader(args.in_path)
        normalized_data = normalize(raw_data)
        writer(normalized_data, args.out_path)


if __name__ == "__main__":
    main()
