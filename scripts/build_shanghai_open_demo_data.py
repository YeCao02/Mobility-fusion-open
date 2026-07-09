from __future__ import annotations

import argparse
import json
import shutil
from datetime import datetime, timedelta, timezone
from pathlib import Path

import polars as pl

import build_open_demo_data as open_demo


TZ = timezone(timedelta(hours=8))
DEFAULT_PRODUCTION_ROOT = Path(r"S:\GEO BIG data\Shanghai-2026\sample_mobility_fusion_v0_4_0")
DEFAULT_OUT = Path("demo/shanghai-validation/data")
DEFAULT_DATES = [f"2026-05-{day:02d}" for day in range(1, 15)]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build Shanghai validation demo shards from original mobility-fusion outputs.")
    parser.add_argument("--production-root", type=Path, default=DEFAULT_PRODUCTION_ROOT)
    parser.add_argument("--out", type=Path, default=DEFAULT_OUT)
    parser.add_argument("--dates", nargs="*", default=DEFAULT_DATES)
    return parser.parse_args()


def read_date(root: Path, date: str) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    nodes_path = root / "stage" / "full_od_nodes" / f"date={date}" / "*.parquet"
    segments_path = root / "stage" / "full_od_segments" / f"date={date}" / "*.parquet"
    events_path = root / "stage" / "standardized_event_od_ranges" / f"date={date}" / "*" / "*.parquet"
    nodes = pl.scan_parquet(str(nodes_path)).collect(engine="streaming")
    segments = pl.scan_parquet(str(segments_path)).collect(engine="streaming")
    events = (
        pl.scan_parquet(str(events_path))
        .select(
            [
                "uuid",
                "date",
                "point_source",
                "source_norm",
                "lon",
                "lat",
                "p_name",
                "start_time",
                "end_time",
                "negative_duration_flag",
                "raw_strategy_status",
                "h3_10",
            ]
        )
        .collect(engine="streaming")
    )
    return nodes, segments, events


def write_json(path: Path, payload: dict) -> float:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")
    return path.stat().st_size / 1024 / 1024


def clean_output(out: Path) -> None:
    for folder_name in ["processed", "raw"]:
        folder = out / folder_name
        if folder.exists():
            shutil.rmtree(folder)
        folder.mkdir(parents=True, exist_ok=True)
    for filename in ["manifest.json", "validation.json", "case_summary.csv"]:
        path = out / filename
        if path.exists():
            path.unlink()


def main() -> None:
    args = parse_args()
    open_demo.CITY_NAME_BY_ABBR["sh"] = "Shanghai"
    clean_output(args.out)
    manifest = {
        "generated_at": datetime.now(TZ).isoformat(),
        "description": (
            "Shanghai residence sample replayed through the original mobility-fusion FULL_OD pipeline. "
            "The browser uses the same processed/raw shard contract as the GBA demo."
        ),
        "h3_res": 10,
        "method": {
            "production_root": str(args.production_root),
            "chain_mode": "FULL_OD",
            "city": "Shanghai",
            "sample": "300 UUIDs per date, ranks 1-300, 601-900, ... from WifiStable first-appearance order.",
        },
        "cities": {"sh": {"name": "Shanghai", "files": []}},
    }

    for date in args.dates:
        print(f"[read] {date}", flush=True)
        nodes, segments, events = read_date(args.production_root, date)
        print(f"       nodes={nodes.height:,}, od={segments.height:,}, raw_events={events.height:,}", flush=True)
        processed = open_demo.build_processed_payload("sh", date, nodes, segments)
        raw = open_demo.build_raw_payload(date, events, nodes)
        processed_name = f"processed_sh_{date}.json"
        raw_name = f"raw_sh_{date}.json"
        processed_mb = write_json(args.out / "processed" / processed_name, processed)
        raw_mb = write_json(args.out / "raw" / raw_name, raw)
        manifest["cities"]["sh"]["files"].append(
            {
                "date": date,
                "processed": f"data/processed/{processed_name}",
                "raw": f"data/raw/{raw_name}",
                "uuid_count": processed["stats"].get("uuid_count", 0),
                "raw_event_count": events.height,
                "processed_size_mb": round(processed_mb, 2),
                "raw_size_mb": round(raw_mb, 2),
                "stats": processed.get("stats", {}),
            }
        )
        print(f"[write] {date}: processed={processed_mb:.2f} MB raw={raw_mb:.2f} MB", flush=True)

    write_json(args.out / "manifest.json", manifest)
    print(f"[done] manifest={args.out / 'manifest.json'}", flush=True)


if __name__ == "__main__":
    main()
