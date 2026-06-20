from __future__ import annotations

import argparse
import json
import math
import random
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

import h3
import polars as pl

TZ = timezone(timedelta(hours=8))
PROD = Path(r"S:\GEO BIG data\Greater Bay Area data_operators 500G\mobility_fusion_production_v0_4_0")
H3_LOOKUP = PROD / "ref_inputs" / "gba_h3_lookup_res10.parquet"
NODES = PROD / "stage" / "full_od_nodes"
SEGMENTS = PROD / "stage" / "full_od_segments"
EVENT_CANDIDATES = [
    PROD / "stage" / "standardized_event_od_ranges",
    PROD / "stage" / "standardized_event_ranges",
]
DATES = ["2024-12-24", "2024-12-29"]

CITY_ABBR_BY_CODE = {
    "440100000000": "gz",
    "440300000000": "sz",
    "440400000000": "zh",
    "440600000000": "fs",
    "440700000000": "jm",
    "441200000000": "zq",
    "441300000000": "hz",
    "441900000000": "dg",
    "442000000000": "zs",
}
CITY_NAME_BY_ABBR = {
    "gz": "Guangzhou / 广州",
    "sz": "Shenzhen / 深圳",
    "zh": "Zhuhai / 珠海",
    "fs": "Foshan / 佛山",
    "jm": "Jiangmen / 江门",
    "zq": "Zhaoqing / 肇庆",
    "hz": "Huizhou / 惠州",
    "dg": "Dongguan / 东莞",
    "zs": "Zhongshan / 中山",
}
ROLE = {1: "DAY_START", 2: "STAY", 3: "STOP", 4: "DAY_END"}
CITY_NAME_BY_ABBR = {
    "gz": "Guangzhou",
    "sz": "Shenzhen",
    "zh": "Zhuhai",
    "fs": "Foshan",
    "jm": "Jiangmen",
    "zq": "Zhaoqing",
    "hz": "Huizhou",
    "dg": "Dongguan",
    "zs": "Zhongshan",
}
SOURCE_MASK = {1: "T", 2: "W", 4: "C", 8: "S"}
SOURCE_CODE = {
    "Timing": "T",
    "WifiStable": "W",
    "WiFiStable": "W",
    "WiFiConnect": "C",
    "Connect": "C",
    "SceneReco": "S",
}


def h3_center(cell: str) -> tuple[float, float]:
    lat, lon = h3.cell_to_latlng(cell)
    return round(float(lon), 6), round(float(lat), 6)


def h3_boundary(cell: str) -> list[list[float]]:
    coords = [[round(lng, 6), round(lat, 6)] for lat, lng in h3.cell_to_boundary(cell)]
    if coords and coords[0] != coords[-1]:
        coords.append(coords[0])
    return coords


def local_time(ms: int | None, date_label: str) -> str:
    if ms is None:
        return ""
    dt = datetime.fromtimestamp(int(ms) / 1000, tz=TZ)
    base = datetime.strptime(date_label, "%Y-%m-%d").replace(tzinfo=TZ).date()
    offset = (dt.date() - base).days
    clock = dt.strftime("%H:%M:%S")
    return clock if offset == 0 else f"{'+' if offset > 0 else ''}{offset}d {clock}"


def source_mask_label(mask: int | None) -> str:
    if mask is None:
        return ""
    return "+".join(label for bit, label in SOURCE_MASK.items() if int(mask) & bit)


def event_root() -> Path:
    for candidate in EVENT_CANDIDATES:
        if candidate.exists():
            return candidate
    raise FileNotFoundError("No standardized event stage found")


def collect_samples(dates: list[str], per_city_day: int) -> dict[tuple[str, str], list[str]]:
    lookup = (
        pl.scan_parquet(str(H3_LOOKUP))
        .select(pl.col("grid_id").alias("h3_10"), pl.col("city_code").cast(pl.Utf8))
        .filter(pl.col("city_code").is_in(sorted(CITY_ABBR_BY_CODE)))
    )
    samples: dict[tuple[str, str], list[str]] = {}
    for date in dates:
        print(f"[sample] {date}", flush=True)
        starts = (
            pl.scan_parquet(str(NODES / f"date={date}" / "*.parquet"))
            .filter(pl.col("node_role_code") == 1)
            .select(["uuid", "h3_10"])
            .join(lookup, on="h3_10", how="inner")
            .with_columns(pl.col("city_code").replace(CITY_ABBR_BY_CODE).alias("start_city"))
            .select(["uuid", "start_city"])
            .collect(engine="streaming")
        )
        for city in sorted(CITY_NAME_BY_ABBR):
            uuids = starts.filter(pl.col("start_city") == city)["uuid"].to_list()
            rng = random.Random(f"mobility-fusion-open-{date}-{city}")
            rng.shuffle(uuids)
            samples[(city, date)] = uuids[:per_city_day]
            print(f"  {city}: {len(samples[(city, date)])}/{len(uuids)}", flush=True)
    return samples


def read_city_date(date: str, uuids: list[str]) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    uuid_series = pl.Series("uuid", uuids)
    nodes = (
        pl.scan_parquet(str(NODES / f"date={date}" / "*.parquet"))
        .filter(pl.col("uuid").is_in(uuid_series.implode()))
        .collect(engine="streaming")
    )
    segments = (
        pl.scan_parquet(str(SEGMENTS / f"date={date}" / "*.parquet"))
        .filter(pl.col("uuid").is_in(uuid_series.implode()))
        .collect(engine="streaming")
    )
    events = (
        pl.scan_parquet(str(event_root() / f"date={date}" / "*" / "*.parquet"))
        .filter(pl.col("uuid").is_in(uuid_series.implode()))
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


def h3_city_lookup(cells: set[str], default_city: str = "") -> dict[str, dict[str, str]]:
    if not cells:
        return {}
    return {
        cell: {
            "code": default_city,
            "abbr": default_city,
            "name": CITY_NAME_BY_ABBR.get(default_city, ""),
        }
        for cell in cells
    }


def raw_event_status(row: dict) -> str:
    raw = str(row.get("raw_strategy_status") or "").upper()
    if bool(row.get("negative_duration_flag")) or "DROP" in raw or "HARD" in raw:
        return "D"
    return "K"


def build_processed_payload(city: str, date: str, nodes: pl.DataFrame, segments: pl.DataFrame) -> dict:
    h3_cells = set(nodes["h3_10"].drop_nulls().unique().to_list())
    h3_cells.update(segments["o_h3_10"].drop_nulls().unique().to_list())
    h3_cells.update(segments["d_h3_10"].drop_nulls().unique().to_list())
    city_lookup = h3_city_lookup(h3_cells, city)

    nodes_by_case: dict[str, list[dict]] = defaultdict(list)
    for row in nodes.sort(["uuid", "node_order"]).to_dicts():
        case_id = f"{row['uuid']}__{date}"
        lon, lat = h3_center(row["h3_10"])
        info = city_lookup.get(row["h3_10"], {})
        nodes_by_case[case_id].append(
            {
                "o": int(row["node_order"]),
                "r": ROLE.get(int(row["node_role_code"]), "STOP"),
                "h3": row["h3_10"],
                "lon": lon,
                "lat": lat,
                "t0": local_time(row["start_time_ms"], date),
                "t1": local_time(row["end_time_ms"], date),
                "ts": int(row["start_time_ms"]),
                "te": int(row["end_time_ms"]),
                "dur": int(row["duration_sec"] or 0),
                "ec": int(row["event_count"] or 0),
                "h10c": 1,
                "mh3": row["h3_10"],
                "mc": 1,
                "sr": int(row["has_scenereco"] or 0),
                "pn": row["scenereco_p_names"] or "",
                "qc": source_mask_label(row["source_mask"]),
                "city": info.get("abbr", ""),
                "h9": row["h3_9"],
            }
        )

    od_by_case: dict[str, list[dict]] = defaultdict(list)
    for row in segments.sort(["uuid", "segment_order"]).to_dicts():
        case_id = f"{row['uuid']}__{date}"
        speed = None
        if row["speed_kmh"] is not None and math.isfinite(float(row["speed_kmh"])):
            speed = round(float(row["speed_kmh"]), 1)
        o_city = city_lookup.get(row["o_h3_10"], {}).get("code", "")
        d_city = city_lookup.get(row["d_h3_10"], {}).get("code", "")
        od_by_case[case_id].append(
            {
                "so": int(row["segment_order"]),
                "on": int(row["o_node_order"]),
                "dn": int(row["d_node_order"]),
                "sr": "",
                "oh3": row["o_h3_10"],
                "dh3": row["d_h3_10"],
                "dt": local_time(row["depart_time_ms"], date),
                "at": local_time(row["arrive_time_ms"], date),
                "dtms": int(row["depart_time_ms"]),
                "atms": int(row["arrive_time_ms"]),
                "tt": int(row["travel_time_sec"]) if row["travel_time_sec"] is not None else None,
                "dm": round(float(row["distance_m"] or 0), 1),
                "spd": speed,
                "bd": 0,
                "cross": bool(o_city and d_city and o_city != d_city),
                "qc": "",
                "drop": 0,
                "valid": int(row["od_valid"] or 0),
                "inv": str(row["od_invalid_reason_code"] or ""),
                "ak": int(row["analysis_keep_default"] or 0),
                "sk": int(row["strict_keep_default"] or 0),
                "m": int(row["has_metro_station_evidence"] or 0),
                "r": int(row["has_rail_station_evidence"] or 0),
            }
        )

    uuid_data = {}
    uuid_summary = []
    for case_id, node_rows in nodes_by_case.items():
        node_rows.sort(key=lambda x: x["o"])
        od_rows = sorted(od_by_case.get(case_id, []), key=lambda x: x["so"])
        stays = sum(1 for n in node_rows if n["r"] == "STAY")
        stops = sum(1 for n in node_rows if n["r"] == "STOP")
        cross = sum(1 for o in od_rows if o["cross"])
        high = sum(1 for o in od_rows if o["spd"] is not None and o["spd"] > 170)
        total_events = sum(n["ec"] for n in node_rows)
        sources = defaultdict(int)
        for node in node_rows:
            for source in str(node["qc"]).split("+"):
                if source:
                    sources[source] += int(node["ec"])
        uuid_data[case_id] = {
            "nodes": node_rows,
            "od": od_rows,
            "drop_nodes": [],
            "event_summary": {
                "total": total_events,
                "kept": total_events,
                "dropped": 0,
                "anchor_suppressed": 0,
                "sources": dict(sources),
                "stays": stays,
                "stops": stops,
                "od_kept": len(od_rows),
                "od_speed_dropped": 0,
                "speed_pruned_nodes": 0,
            },
        }
        uuid = case_id.split("__", 1)[0]
        uuid_summary.append(
            {
                "uuid": case_id,
                "short": f"{uuid[:8]}...{uuid[-4:]} {date[-5:]}",
                "nodes": len(node_rows),
                "od": len(od_rows),
                "st": stays,
                "sp": stops,
                "cross": cross,
                "hs": high,
                "bf": 0,
                "pr": 0,
                "score": cross * 2 + high * 3 + stops,
            }
        )

    uuid_summary.sort(key=lambda x: (-x["score"], x["uuid"]))
    centers = [h3_center(c) for c in h3_cells if c]
    return {
        "generated_at": datetime.now(TZ).strftime("%Y-%m-%d %H:%M:%S"),
        "date": date,
        "city": city,
        "city_name": CITY_NAME_BY_ABBR[city],
        "chain_mode": "FULL_OD",
        "h3_res": 10,
        "stats": {
            "uuid_count": len(uuid_summary),
            "node_count": nodes.height,
            "od_count": segments.height,
            "event_count": int(nodes["event_count"].sum()),
            "stay_count": int(nodes.filter(pl.col("node_role_code") == 2).height),
            "stop_count": int(nodes.filter(pl.col("node_role_code") == 3).height),
        },
        "uuid_summary": uuid_summary,
        "uuid_data": uuid_data,
        "hex": {cell: h3_boundary(cell) for cell in sorted(h3_cells)},
        "h3_city": city_lookup,
        "city_boundary": {"type": "FeatureCollection", "features": []},
        "bounds": [min(x for x, _ in centers), min(y for _, y in centers), max(x for x, _ in centers), max(y for _, y in centers)] if centers else [],
    }


def build_raw_payload(date: str, events: pl.DataFrame, nodes: pl.DataFrame) -> dict:
    raw_nodes_by_case: dict[str, list[dict]] = defaultdict(list)
    for row in nodes.select(["uuid", "h3_10", "start_time_ms", "end_time_ms"]).to_dicts():
        raw_nodes_by_case[f"{row['uuid']}__{date}"].append(row)

    raw_by_case: dict[str, list[list]] = defaultdict(list)
    summary_by_case = defaultdict(lambda: {"total": 0, "kept": 0, "dropped": 0, "anchor_suppressed": 0, "sources": defaultdict(int)})
    for key, group in events.sort(["uuid", "start_time", "end_time"]).partition_by("uuid", as_dict=True).items():
        uuid = key[0] if isinstance(key, tuple) else key
        case_id = f"{uuid}__{date}"
        node_rows = raw_nodes_by_case.get(case_id, [])
        for order, row in enumerate(group.to_dicts(), start=1):
            source = str(row.get("source_norm") or row.get("point_source") or "")
            status = raw_event_status(row)
            if status == "K":
                start = int(row["start_time"])
                end = int(row["end_time"])
                captured = any(str(row["h3_10"]) == str(n["h3_10"]) and not (end < int(n["start_time_ms"]) or start > int(n["end_time_ms"])) for n in node_rows)
                if not captured:
                    status = "S"
            raw_by_case[case_id].append(
                [
                    round(float(row["lon"]), 6),
                    round(float(row["lat"]), 6),
                    SOURCE_CODE.get(source, source[:1] or "?"),
                    status,
                    order,
                    local_time(row.get("start_time"), date),
                    local_time(row.get("end_time"), date),
                ]
            )
            stats = summary_by_case[case_id]
            stats["total"] += 1
            stats["sources"][source] += 1
            if status == "D":
                stats["dropped"] += 1
            elif status == "S":
                stats["anchor_suppressed"] += 1
            else:
                stats["kept"] += 1

    return {
        "generated_at": datetime.now(TZ).strftime("%Y-%m-%d %H:%M:%S"),
        "date": date,
        "uuid_data": {
            case_id: {
                "events": events,
                "event_summary": {
                    **{k: v for k, v in summary.items() if k != "sources"},
                    "sources": dict(summary["sources"]),
                },
            }
            for case_id, events in raw_by_case.items()
            for summary in [summary_by_case[case_id]]
        },
    }


def write_json(path: Path, payload: dict) -> float:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")
    return path.stat().st_size / 1024 / 1024


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--per-city", type=int, default=1000, help="uuid-day cases per city across the selected dates")
    parser.add_argument("--dates", nargs="*", default=DATES)
    parser.add_argument("--cities", nargs="*", default=sorted(CITY_NAME_BY_ABBR))
    args = parser.parse_args()

    per_city_day = max(1, math.ceil(args.per_city / len(args.dates)))
    samples = collect_samples(args.dates, per_city_day)
    manifest = {
        "generated_at": datetime.now(TZ).isoformat(),
        "description": "Open public demo: processed FULL_OD chains and matching standardized raw events are stored separately and merged by the browser.",
        "h3_res": 10,
        "cities": {},
    }

    for date in args.dates:
        for city in args.cities:
            uuids = samples[(city, date)]
            print(f"[read] {date} {city}: {len(uuids)} uuid-day cases", flush=True)
            nodes, segments, events = read_city_date(date, uuids)
            print(f"       nodes={nodes.height:,}, od={segments.height:,}, raw_events={events.height:,}", flush=True)

            processed = build_processed_payload(city, date, nodes, segments)
            raw = build_raw_payload(date, events, nodes)

            processed_name = f"processed_{city}_{date}.json"
            raw_name = f"raw_{city}_{date}.json"
            processed_mb = write_json(args.out / "processed" / processed_name, processed)
            raw_mb = write_json(args.out / "raw" / raw_name, raw)
            print(f"[write] {city} {date}: processed={processed_mb:.2f} MB raw={raw_mb:.2f} MB", flush=True)

            manifest["cities"].setdefault(city, {"name": CITY_NAME_BY_ABBR[city], "files": []})
            manifest["cities"][city]["files"].append(
                {
                    "date": date,
                    "processed": f"data/processed/{processed_name}",
                    "raw": f"data/raw/{raw_name}",
                    "uuid_count": processed["stats"].get("uuid_count", 0),
                    "processed_size_mb": round(processed_mb, 2),
                    "raw_size_mb": round(raw_mb, 2),
                }
            )

    write_json(args.out / "manifest.json", manifest)


if __name__ == "__main__":
    main()
