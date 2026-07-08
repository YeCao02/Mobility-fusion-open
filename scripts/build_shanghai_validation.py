from __future__ import annotations

import argparse
import csv
import json
import math
import os
import re
import time
from collections import Counter, defaultdict
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any

import polars as pl


TZ = timezone(timedelta(hours=8))
DEFAULT_RAW_ROOT = Path(r"S:\GEO BIG data\Shanghai-2026")
DEFAULT_OUT = Path("demo/shanghai-validation/data/validation.json")
TIME_FORMAT = "%Y-%m-%d %H:%M:%S"
SOURCE_ORDER = ["Timing", "WifiConnect", "WifiStable", "SceneReco"]
SOURCE_CODE = {"Timing": "T", "WifiConnect": "C", "WifiStable": "W", "SceneReco": "S"}
SHANGHAI_BBOX = {
    "min_lon": 120.85,
    "max_lon": 122.20,
    "min_lat": 30.65,
    "max_lat": 31.95,
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--raw-root", type=Path, default=DEFAULT_RAW_ROOT)
    parser.add_argument("--out", type=Path, default=DEFAULT_OUT)
    parser.add_argument("--dates", nargs="*", default=[f"2026-05-{day:02d}" for day in range(1, 15)])
    parser.add_argument("--sample-size", type=int, default=100)
    parser.add_argument("--sample-stride", type=int, default=200)
    parser.add_argument("--sample-source", default="WifiStable")
    parser.add_argument("--threads", type=int, default=24)
    parser.add_argument("--batch-size", type=int, default=50_000)
    parser.add_argument("--batches-per-read", type=int, default=4)
    parser.add_argument("--max-events-per-case", type=int, default=1800)
    parser.add_argument("--no-assume-sorted", action="store_true")
    return parser.parse_args()


def configure_threads(threads: int) -> None:
    os.environ.setdefault("POLARS_MAX_THREADS", str(threads))


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


def collect_ranked_uuids(
    path: Path,
    end_rank: int,
    batch_size: int,
    batches_per_read: int,
    threads: int,
) -> list[str]:
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
        print(
            f"[sample] {date} source={source} ranks={start_rank + 1}-{end_rank} uuids={len(samples[date])}",
            flush=True,
        )
    return samples


def normalize_batch(batch: pl.DataFrame, source: str, date: str) -> pl.DataFrame:
    return (
        batch.lazy()
        .with_columns(
            source=pl.lit(source),
            source_code=pl.lit(SOURCE_CODE.get(source, source[:1])),
            date=pl.lit(date),
            lon=pl.col("longitude").cast(pl.Float64, strict=False),
            lat=pl.col("latitude").cast(pl.Float64, strict=False),
            start_dt=pl.col("start_time").str.strptime(pl.Datetime, format=TIME_FORMAT, strict=False),
            end_dt=pl.col("end_time").str.strptime(pl.Datetime, format=TIME_FORMAT, strict=False),
            fix_ap_ratio_num=pl.col("fix_ap_ratio").cast(pl.Float64, strict=False),
        )
        .select(
            [
                "date",
                "uuid",
                "source",
                "source_code",
                "lon",
                "lat",
                "p_id",
                "fix_ap_ratio_num",
                "start_time",
                "end_time",
                "start_dt",
                "end_dt",
            ]
        )
        .collect()
    )


def read_source_events(
    path: Path,
    source: str,
    date: str,
    uuids: set[str],
    batch_size: int,
    batches_per_read: int,
    threads: int,
    assume_sorted: bool,
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
                parts.append(normalize_batch(filtered, source, date))
            if assume_sorted:
                last_uuid = batch["uuid"].drop_nulls().tail(1)
                if len(last_uuid) and str(last_uuid.item()) > max_uuid:
                    stop_after_batch = True
        if stop_after_batch:
            break
    if not parts:
        return pl.DataFrame(
            schema={
                "date": pl.Utf8,
                "uuid": pl.Utf8,
                "source": pl.Utf8,
                "source_code": pl.Utf8,
                "lon": pl.Float64,
                "lat": pl.Float64,
                "p_id": pl.Utf8,
                "fix_ap_ratio_num": pl.Float64,
                "start_time": pl.Utf8,
                "end_time": pl.Utf8,
                "start_dt": pl.Datetime,
                "end_dt": pl.Datetime,
            }
        )
    out = pl.concat(parts, how="vertical")
    print(f"[read] {date} {source}: rows_seen={rows_seen:,} matched={out.height:,}", flush=True)
    return out


def haversine_m(lon1: float, lat1: float, lon2: float, lat2: float) -> float | None:
    if not all(math.isfinite(x) for x in [lon1, lat1, lon2, lat2]):
        return None
    radius = 6_371_008.8
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    return 2 * radius * math.asin(math.sqrt(min(1.0, a)))


def dt_to_ms(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return int(value.replace(tzinfo=TZ).timestamp() * 1000)
    return None


def local_time(value: Any, date_label: str) -> str:
    if value is None:
        return ""
    if not isinstance(value, datetime):
        return str(value)
    base = datetime.strptime(date_label, "%Y-%m-%d").date()
    offset = (value.date() - base).days
    clock = value.strftime("%H:%M:%S")
    return clock if offset == 0 else f"{offset:+d}d {clock}"


def valid_bbox(lon: float | None, lat: float | None) -> bool:
    if lon is None or lat is None or not math.isfinite(lon) or not math.isfinite(lat):
        return False
    return (
        SHANGHAI_BBOX["min_lon"] <= lon <= SHANGHAI_BBOX["max_lon"]
        and SHANGHAI_BBOX["min_lat"] <= lat <= SHANGHAI_BBOX["max_lat"]
    )


def event_duration_min(row: dict[str, Any]) -> float | None:
    start, end = row.get("start_dt"), row.get("end_dt")
    if not isinstance(start, datetime) or not isinstance(end, datetime):
        return None
    return (end - start).total_seconds() / 60


def build_case(date: str, uuid: str, rows: list[dict[str, Any]], max_events: int) -> dict[str, Any]:
    rows = sorted(rows, key=lambda r: (r.get("start_dt") or datetime.max, r.get("end_dt") or datetime.max, r.get("source") or ""))
    source_counts = Counter(str(row.get("source")) for row in rows)
    events: list[list[Any]] = []
    drift_count = 0
    out_bbox = 0
    negative_duration = 0
    long_duration = 0
    invalid_time = 0
    max_speed = 0.0
    max_step = 0.0
    overlap_conflict = 0
    prev = None

    for order, row in enumerate(rows, start=1):
        lon = row.get("lon")
        lat = row.get("lat")
        duration = event_duration_min(row)
        status = "K"
        if not valid_bbox(lon, lat):
            out_bbox += 1
            status = "B"
        if duration is None:
            invalid_time += 1
            status = "T"
        elif duration < 0:
            negative_duration += 1
            status = "D"
        elif duration > 24 * 60:
            long_duration += 1
            status = "L"

        step_km = None
        speed_kmh = None
        if prev is not None and valid_bbox(lon, lat) and valid_bbox(prev.get("lon"), prev.get("lat")):
            distance_m = haversine_m(prev["lon"], prev["lat"], lon, lat)
            if distance_m is not None:
                step_km = distance_m / 1000
                max_step = max(max_step, step_km)
                prev_start = prev.get("start_dt")
                current_start = row.get("start_dt")
                prev_end = prev.get("end_dt")
                if isinstance(prev_start, datetime) and isinstance(current_start, datetime):
                    gap_h = (current_start - prev_start).total_seconds() / 3600
                    if gap_h > 0:
                        speed_kmh = step_km / gap_h
                        max_speed = max(max_speed, speed_kmh)
                if isinstance(prev_end, datetime) and isinstance(current_start, datetime):
                    overlap = current_start < prev_end
                    if overlap and distance_m > 2000:
                        overlap_conflict += 1
                        drift_count += 1
                        status = "O"
                    elif speed_kmh is not None and speed_kmh > 200:
                        drift_count += 1
                        status = "V"
                    elif step_km > 5 and isinstance(prev_start, datetime) and isinstance(current_start, datetime):
                        gap_min = (current_start - prev_start).total_seconds() / 60
                        if 0 <= gap_min < 10:
                            drift_count += 1
                            status = "J"

        if len(events) < max_events:
            events.append(
                [
                    round(float(lon), 6) if lon is not None and math.isfinite(float(lon)) else None,
                    round(float(lat), 6) if lat is not None and math.isfinite(float(lat)) else None,
                    SOURCE_CODE.get(str(row.get("source")), "?"),
                    status,
                    order,
                    local_time(row.get("start_dt"), date),
                    local_time(row.get("end_dt"), date),
                    round(duration, 2) if duration is not None else None,
                    round(step_km, 3) if step_km is not None else None,
                    round(speed_kmh, 1) if speed_kmh is not None and math.isfinite(speed_kmh) else None,
                ]
            )
        prev = row

    valid_coords = [(float(r["lon"]), float(r["lat"])) for r in rows if valid_bbox(r.get("lon"), r.get("lat"))]
    if valid_coords:
        min_lon, max_lon = min(x for x, _ in valid_coords), max(x for x, _ in valid_coords)
        min_lat, max_lat = min(y for _, y in valid_coords), max(y for _, y in valid_coords)
        span_m = haversine_m(min_lon, min_lat, max_lon, max_lat) or 0
        center = [round((min_lon + max_lon) / 2, 6), round((min_lat + max_lat) / 2, 6)]
        bounds = [round(min_lon, 6), round(min_lat, 6), round(max_lon, 6), round(max_lat, 6)]
    else:
        span_m = 0
        center = []
        bounds = []

    quality_score = min(
        100,
        drift_count * 8
        + overlap_conflict * 6
        + out_bbox * 5
        + negative_duration * 8
        + long_duration * 3
        + (1 if max_speed > 200 else 0) * 12
        + (1 if span_m > 50_000 else 0) * 8,
    )
    return {
        "uuid": f"{uuid}__{date}",
        "raw_uuid": uuid,
        "date": date,
        "short": f"{uuid[:8]}...{uuid[-4:]} {date[-5:]}",
        "event_count": len(rows),
        "shown_event_count": len(events),
        "source_counts": dict(source_counts),
        "drift_count": drift_count,
        "overlap_conflict": overlap_conflict,
        "out_bbox": out_bbox,
        "negative_duration": negative_duration,
        "long_duration": long_duration,
        "invalid_time": invalid_time,
        "max_speed_kmh": round(max_speed, 1),
        "max_step_km": round(max_step, 3),
        "track_span_km": round(span_m / 1000, 3),
        "quality_score": quality_score,
        "events": events,
        "events_truncated": len(rows) > max_events,
        "center": center,
        "bounds": bounds,
    }


def build_payload(
    all_events: dict[str, pl.DataFrame],
    samples: dict[str, list[str]],
    duplicates: list[dict[str, str]],
    max_events_per_case: int,
) -> dict[str, Any]:
    date_summaries: list[dict[str, Any]] = []
    case_summaries: list[dict[str, Any]] = []
    cases: dict[str, dict[str, Any]] = {}
    source_totals = Counter()
    overall = Counter()
    bounds_all: list[tuple[float, float]] = []

    for date in sorted(samples):
        df = all_events.get(date)
        if df is None or df.is_empty():
            continue
        rows_by_uuid = defaultdict(list)
        for row in df.sort(["uuid", "start_dt", "end_dt", "source"]).to_dicts():
            rows_by_uuid[row["uuid"]].append(row)
        daily_cases = []
        for uuid in samples[date]:
            case = build_case(date, uuid, rows_by_uuid.get(uuid, []), max_events_per_case)
            cases[case["uuid"]] = case
            daily_cases.append(case)
            source_totals.update(case["source_counts"])
            if case["center"]:
                bounds_all.append(tuple(case["center"]))
            summary = {k: v for k, v in case.items() if k != "events"}
            case_summaries.append(summary)

        event_count = sum(case["event_count"] for case in daily_cases)
        drift_cases = sum(1 for case in daily_cases if case["drift_count"] > 0 or case["max_speed_kmh"] > 200)
        severe_cases = sum(1 for case in daily_cases if case["quality_score"] >= 40)
        daily_source = Counter()
        for case in daily_cases:
            daily_source.update(case["source_counts"])
        date_summaries.append(
            {
                "date": date,
                "sample_uuid_count": len(daily_cases),
                "event_count": event_count,
                "drift_case_count": drift_cases,
                "severe_case_count": severe_cases,
                "drift_case_rate": round(drift_cases / max(1, len(daily_cases)), 4),
                "avg_events_per_uuid": round(event_count / max(1, len(daily_cases)), 1),
                "max_speed_kmh": max((case["max_speed_kmh"] for case in daily_cases), default=0),
                "max_track_span_km": max((case["track_span_km"] for case in daily_cases), default=0),
                "source_counts": dict(daily_source),
            }
        )
        overall["sample_uuid_days"] += len(daily_cases)
        overall["event_count"] += event_count
        overall["drift_case_count"] += drift_cases
        overall["severe_case_count"] += severe_cases

    if bounds_all:
        min_lon, max_lon = min(x for x, _ in bounds_all), max(x for x, _ in bounds_all)
        min_lat, max_lat = min(y for _, y in bounds_all), max(y for _, y in bounds_all)
        map_bounds = [min_lon, min_lat, max_lon, max_lat]
    else:
        map_bounds = [120.85, 30.65, 122.2, 31.95]

    case_summaries.sort(key=lambda x: (-x["quality_score"], -x["drift_count"], -x["event_count"], x["uuid"]))
    conclusion = "存在明显漂移/异常长尾，需要重点核查" if overall["drift_case_count"] else "样本未检出明显漂移"
    if overall["sample_uuid_days"]:
        drift_rate = overall["drift_case_count"] / overall["sample_uuid_days"]
        severe_rate = overall["severe_case_count"] / overall["sample_uuid_days"]
    else:
        drift_rate = severe_rate = 0

    return {
        "generated_at": datetime.now(TZ).isoformat(),
        "title": "Shanghai residence mobile-data validation",
        "method": {
            "sample_rule": "For day index i, take distinct UUID ranks i*200+1 through i*200+100 from the first-appearance order of the sample source file.",
            "sample_source": "WifiStable preferred, source fallback if missing",
            "sources": SOURCE_ORDER,
            "bbox": SHANGHAI_BBOX,
            "drift_flags": {
                "V": "consecutive start-to-start speed > 200 km/h",
                "J": "jump > 5 km within 10 minutes",
                "O": "overlapping events more than 2 km apart",
                "B": "outside Shanghai bounding box",
                "D": "negative duration",
                "L": "duration longer than 24 hours",
            },
        },
        "duplicates_ignored": duplicates,
        "overall": {
            **dict(overall),
            "drift_case_rate": round(drift_rate, 4),
            "severe_case_rate": round(severe_rate, 4),
            "source_counts": dict(source_totals),
            "conclusion": conclusion,
        },
        "date_summaries": date_summaries,
        "case_summaries": case_summaries,
        "cases": cases,
        "map_bounds": [round(x, 6) for x in map_bounds],
    }


def write_payload(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")


def main() -> None:
    args = parse_args()
    configure_threads(args.threads)
    started = time.time()
    files, duplicates = discover_residence_files(args.raw_root)
    missing = [date for date in args.dates if date not in files]
    if missing:
        raise FileNotFoundError(f"Missing residence files for dates: {missing}")

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

    all_events: dict[str, pl.DataFrame] = {}
    for date in args.dates:
        date_parts = []
        uuids = set(samples[date])
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
                )
            )
        all_events[date] = pl.concat(date_parts, how="vertical") if date_parts else pl.DataFrame()
        print(f"[date] {date}: matched_events={all_events[date].height:,}", flush=True)

    payload = build_payload(all_events, samples, duplicates, args.max_events_per_case)
    payload["runtime_seconds"] = round(time.time() - started, 2)
    write_payload(args.out, payload)

    csv_path = args.out.parent / "case_summary.csv"
    with csv_path.open("w", encoding="utf-8-sig", newline="") as fh:
        fieldnames = [k for k in payload["case_summaries"][0] if k not in {"source_counts", "center", "bounds"}]
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        for row in payload["case_summaries"]:
            writer.writerow({k: row.get(k) for k in fieldnames})

    print(f"[done] payload={args.out} bytes={args.out.stat().st_size:,} runtime={payload['runtime_seconds']}s")


if __name__ == "__main__":
    main()
