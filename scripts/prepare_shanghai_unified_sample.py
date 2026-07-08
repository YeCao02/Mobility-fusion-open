from __future__ import annotations

import argparse
import gzip
import json
import os
import re
import time
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

import polars as pl


TZ = timezone(timedelta(hours=8))
TIME_FORMAT = "%Y-%m-%d %H:%M:%S"
DEFAULT_RAW_ROOT = Path(r"S:\GEO BIG data\Shanghai-2026")
DEFAULT_OUT_ROOT = Path(r"S:\GEO BIG data\Shanghai-2026\sample_unified_points")
SOURCE_ORDER = ["Timing", "WifiConnect", "WifiStable", "SceneReco"]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Extract Shanghai residence UUID-day samples and rewrite them as "
            "mobility-fusion unified_points raw input."
        )
    )
    parser.add_argument("--raw-root", type=Path, default=DEFAULT_RAW_ROOT)
    parser.add_argument("--out-root", type=Path, default=DEFAULT_OUT_ROOT)
    parser.add_argument(
        "--poi-info",
        type=Path,
        default=None,
        help="Optional Shanghai poi_info csv.gz used to map SceneReco p_id to p_name. Defaults to *poi_info*.csv.gz under raw-root.",
    )
    parser.add_argument("--dates", nargs="*", default=[f"2026-05-{day:02d}" for day in range(1, 15)])
    parser.add_argument("--sample-source", default="WifiStable")
    parser.add_argument("--sample-size", type=int, default=100)
    parser.add_argument("--sample-stride", type=int, default=200)
    parser.add_argument("--threads", type=int, default=24)
    parser.add_argument("--batch-size", type=int, default=50_000)
    parser.add_argument("--batches-per-read", type=int, default=4)
    parser.add_argument("--cutoff", default="2026-05-15 00:00:00")
    parser.add_argument("--no-assume-sorted", action="store_true")
    parser.add_argument("--overwrite", action="store_true")
    return parser.parse_args()


def now_text() -> str:
    return datetime.now(TZ).strftime("%Y-%m-%d %H:%M:%S UTC+08:00")


def configure_threads(threads: int) -> None:
    os.environ.setdefault("POLARS_MAX_THREADS", str(threads))
    os.environ.setdefault("RAYON_NUM_THREADS", str(threads))


def discover_residence_files(raw_root: Path) -> tuple[dict[str, dict[str, Path]], list[dict[str, str]]]:
    pattern = re.compile(r"residence_([A-Za-z]+)-(\d{4}-\d{2}-\d{2}).*\.csv(?:_2)?\.gz$", re.I)
    grouped: dict[str, dict[str, list[Path]]] = defaultdict(lambda: defaultdict(list))
    for path in raw_root.iterdir():
        if not path.is_file():
            continue
        match = pattern.search(path.name)
        if match:
            source, date = match.group(1), match.group(2)
            grouped[date][source].append(path)

    selected: dict[str, dict[str, Path]] = {}
    duplicates: list[dict[str, str]] = []
    for date, by_source in grouped.items():
        selected[date] = {}
        for source, paths in by_source.items():
            paths = sorted(paths, key=lambda p: ("_2" in p.name, p.name))
            selected[date][source] = paths[0]
            for duplicate in paths[1:]:
                duplicates.append(
                    {
                        "date": date,
                        "source": source,
                        "kept": str(paths[0]),
                        "ignored_duplicate": str(duplicate),
                    }
                )
    return selected, duplicates


def batched_reader(path: Path, columns: list[str], batch_size: int, threads: int):
    return pl.read_csv_batched(
        path,
        has_header=True,
        columns=columns,
        schema_overrides={column: pl.Utf8 for column in columns},
        ignore_errors=True,
        infer_schema_length=0,
        n_threads=threads,
        batch_size=batch_size,
        low_memory=False,
        rechunk=False,
    )


def resolve_poi_info(raw_root: Path, explicit: Path | None) -> Path | None:
    if explicit:
        return explicit if explicit.exists() else None
    candidates = sorted(raw_root.glob("*poi_info*.csv.gz"))
    return candidates[0] if candidates else None


def load_poi_names(path: Path | None) -> pl.DataFrame:
    if path is None:
        print("[poi] no poi_info csv.gz found; SceneReco p_name will be empty", flush=True)
        return pl.DataFrame(schema={"p_id": pl.Utf8, "p_name": pl.Utf8})
    df = (
        pl.scan_csv(
            path,
            schema_overrides={"p_id": pl.Utf8, "poi_name": pl.Utf8},
            infer_schema_length=0,
        )
        .select(
            pl.col("p_id").cast(pl.Utf8).str.strip_chars(),
            pl.col("poi_name").cast(pl.Utf8).str.strip_chars().alias("p_name"),
        )
        .filter(pl.col("p_id").is_not_null() & (pl.col("p_id") != ""))
        .unique(subset=["p_id"], keep="first")
        .collect()
    )
    named = df.filter(pl.col("p_name").is_not_null() & (pl.col("p_name") != "")).height
    print(f"[poi] loaded {df.height:,} p_id names ({named:,} non-empty) from {path}", flush=True)
    return df


def collect_ranked_uuids(path: Path, end_rank: int, batch_size: int, batches_per_read: int, threads: int) -> list[str]:
    seen: set[str] = set()
    ordered: list[str] = []
    reader = batched_reader(path, ["uuid"], batch_size, threads)
    while len(ordered) < end_rank:
        batches = reader.next_batches(batches_per_read)
        if not batches:
            break
        for batch in batches:
            for uuid in batch["uuid"].drop_nulls().to_list():
                if uuid not in seen:
                    seen.add(uuid)
                    ordered.append(uuid)
                    if len(ordered) >= end_rank:
                        break
            if len(ordered) >= end_rank:
                break
    return ordered


def sample_uuids_for_dates(
    files: dict[str, dict[str, Path]],
    dates: list[str],
    sample_source: str,
    sample_size: int,
    sample_stride: int,
    batch_size: int,
    batches_per_read: int,
    threads: int,
) -> dict[str, list[str]]:
    samples: dict[str, list[str]] = {}
    for day_idx, date in enumerate(dates):
        by_source = files.get(date, {})
        source = sample_source if sample_source in by_source else sorted(by_source)[0]
        start_rank = day_idx * sample_stride
        end_rank = start_rank + sample_size
        ordered = collect_ranked_uuids(by_source[source], end_rank, batch_size, batches_per_read, threads)
        samples[date] = ordered[start_rank:end_rank]
        print(f"[sample] {date} source={source} ranks={start_rank + 1}-{end_rank} uuids={len(samples[date])}", flush=True)
    return samples


def normalize_batch(batch: pl.DataFrame, source: str, date: str, poi_names: pl.DataFrame, cutoff: datetime | None) -> pl.DataFrame:
    out = (
        batch.lazy()
        .with_columns(
            point_source=pl.lit(source),
            date=pl.lit(date),
            lon=pl.col("longitude").cast(pl.Float64, strict=False),
            lat=pl.col("latitude").cast(pl.Float64, strict=False),
            p_id=pl.col("p_id").cast(pl.Utf8).str.strip_chars(),
            start_dt=pl.col("start_time").str.strptime(pl.Datetime, format=TIME_FORMAT, strict=False),
            end_dt=pl.col("end_time").str.strptime(pl.Datetime, format=TIME_FORMAT, strict=False),
        )
        .join(poi_names.lazy(), on="p_id", how="left")
        .with_columns(pl.col("p_name").fill_null(""))
        .filter(pl.col("uuid").is_not_null() & pl.col("start_dt").is_not_null() & pl.col("end_dt").is_not_null())
        .select(["uuid", "date", "point_source", "lon", "lat", "p_id", "p_name", "start_time", "end_time"])
        .collect()
    )
    if cutoff is not None and not out.is_empty():
        out = (
            out.lazy()
            .with_columns(pl.col("start_time").str.strptime(pl.Datetime, format=TIME_FORMAT, strict=False).alias("_start_dt"))
            .filter(pl.col("_start_dt") < cutoff.replace(tzinfo=None))
            .drop("_start_dt")
            .collect()
        )
    return out


def read_source_events(
    path: Path,
    source: str,
    date: str,
    uuids: set[str],
    batch_size: int,
    batches_per_read: int,
    threads: int,
    assume_sorted: bool,
    poi_names: pl.DataFrame,
    cutoff: datetime | None,
) -> pl.DataFrame:
    columns = ["uuid", "longitude", "latitude", "p_id", "fix_ap_ratio", "start_time", "end_time"]
    reader = batched_reader(path, columns, batch_size, threads)
    uuid_series = pl.Series("uuid", sorted(uuids))
    max_uuid = max(uuids)
    parts: list[pl.DataFrame] = []
    rows_seen = 0
    while True:
        batches = reader.next_batches(batches_per_read)
        if not batches:
            break
        stop_after_batch = False
        for batch in batches:
            if batch.is_empty():
                continue
            rows_seen += batch.height
            filtered = batch.filter(pl.col("uuid").is_in(uuid_series.implode()))
            if filtered.height:
                parts.append(normalize_batch(filtered, source, date, poi_names, cutoff))
            if assume_sorted:
                last_uuid = batch["uuid"].drop_nulls().tail(1)
                if len(last_uuid) and str(last_uuid.item()) > max_uuid:
                    stop_after_batch = True
        if stop_after_batch:
            break
    out = pl.concat([p for p in parts if not p.is_empty()], how="vertical_relaxed") if any(not p.is_empty() for p in parts) else empty_unified()
    print(f"[read] {date} {source}: rows_seen={rows_seen:,} matched={out.height:,}", flush=True)
    return out


def empty_unified() -> pl.DataFrame:
    return pl.DataFrame(
        schema={
            "uuid": pl.Utf8,
            "date": pl.Utf8,
            "point_source": pl.Utf8,
            "lon": pl.Float64,
            "lat": pl.Float64,
            "p_id": pl.Utf8,
            "p_name": pl.Utf8,
            "start_time": pl.Utf8,
            "end_time": pl.Utf8,
        }
    )


def write_gzip_csv(df: pl.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    ordered = df.sort(["uuid", "start_time", "end_time", "point_source"]).select(
        ["uuid", "date", "point_source", "lon", "lat", "p_id", "p_name", "start_time", "end_time"]
    )
    with gzip.open(path, "wt", encoding="utf-8", newline="") as fh:
        ordered.write_csv(fh)


def main() -> None:
    args = parse_args()
    configure_threads(args.threads)
    started = time.time()
    cutoff = datetime.strptime(args.cutoff, TIME_FORMAT) if args.cutoff else None
    files, duplicates = discover_residence_files(args.raw_root)
    missing = [date for date in args.dates if date not in files]
    if missing:
        raise FileNotFoundError(f"Missing residence files for dates: {missing}")
    args.out_root.mkdir(parents=True, exist_ok=True)
    poi_names = load_poi_names(resolve_poi_info(args.raw_root, args.poi_info))

    samples = sample_uuids_for_dates(
        files,
        args.dates,
        args.sample_source,
        args.sample_size,
        args.sample_stride,
        args.batch_size,
        args.batches_per_read,
        args.threads,
    )

    manifest = {
        "generated_at": now_text(),
        "raw_root": str(args.raw_root),
        "out_root": str(args.out_root),
        "dates": args.dates,
        "sample_source": args.sample_source,
        "sample_size": args.sample_size,
        "sample_stride": args.sample_stride,
        "cutoff": args.cutoff,
        "duplicates_ignored": duplicates,
        "files": [],
    }

    for date in args.dates:
        out_file = args.out_root / date / f"shanghai_residence_sample_{date}.csv.gz"
        if out_file.exists() and not args.overwrite:
            print(f"[skip] {date}: {out_file}", flush=True)
            continue

        uuids = set(samples[date])
        date_parts = []
        for source in SOURCE_ORDER:
            path = files[date].get(source)
            if path is None:
                continue
            date_parts.append(
                read_source_events(
                    path,
                    source,
                    date,
                    uuids,
                    args.batch_size,
                    args.batches_per_read,
                    args.threads,
                    not args.no_assume_sorted,
                    poi_names,
                    cutoff,
                )
            )
        date_df = pl.concat(date_parts, how="vertical_relaxed") if date_parts else empty_unified()
        write_gzip_csv(date_df, out_file)
        manifest["files"].append(
            {
                "date": date,
                "file": str(out_file),
                "sample_uuid_count": len(samples[date]),
                "row_count": date_df.height,
                "source_counts": date_df.group_by("point_source").len().to_dicts() if not date_df.is_empty() else [],
                "scenereco_named_rows": (
                    date_df.filter((pl.col("point_source") == "SceneReco") & (pl.col("p_name").str.strip_chars() != "")).height
                    if not date_df.is_empty()
                    else 0
                ),
            }
        )
        print(f"[write] {date}: rows={date_df.height:,} file={out_file}", flush=True)

    manifest["runtime_seconds"] = round(time.time() - started, 2)
    (args.out_root / "sample_manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[done] manifest={args.out_root / 'sample_manifest.json'} runtime={manifest['runtime_seconds']}s", flush=True)


if __name__ == "__main__":
    main()
