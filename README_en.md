# Mobility Fusion: frozen-v8 public replay

[中文](README.md) · [GBA replay](https://yecao02.github.io/Mobility-fusion-open/demo/) · [Shanghai replay](https://yecao02.github.io/Mobility-fusion-open/demo/shanghai-validation/) · [Pipeline report](https://yecao02.github.io/Mobility-fusion-open/docs/pipeline-20261028/REPORT.html)

The refresh extracts existing accepted frozen-v8 trajectories: **2,000 unique identities per city** for the nine mainland GBA cities (18,000 total), and 2,000 fixed Shanghai residence identities. No trajectory inference is rerun. GBA covers only the delivered 2024-12-24 and 2024-12-29; Shanghai covers available observations within 2026-05-01–14. These are different periods, not a matched population comparison.

The two replays retain all available selected-user context and load one 200-ID date/page shard at a time. Empty date slots remain explicitly empty; they are not fabricated observations. This replaces the previous user-day samples with fixed cross-date identities.

The basemap now uses standard no-key OpenStreetMap raster tiles, with visible contributor attribution and normal browser caching. No bulk prefetch is implemented. See the [official tile policy](https://operations.osmfoundation.org/policies/tiles/) and [copyright](https://www.openstreetmap.org/copyright).

Fresh keyed pseudonyms replace source UUIDs. The private key and linkage stay outside this repository; POI names are withheld. Location and time are still visible for method demonstration, so pseudonymization is not an anonymity guarantee. The browser format is a reduced display projection, not the lossless seven-table archive.

Raw K/S/D labels are display proxies: matching the shown primary H3/time envelope, not matched, or an explicit input quality flag. They do not reproduce the engine's complete private audit lineage. Sample-city assignment uses the earliest available DAY_START representative H3 and an existing GBA lookup, not home city. Unknown administrative cells remain unknown; cross-city is null unless both endpoints are known. The sample is a deterministic candidate-partition convenience sample, not a population-representative draw.

See [method and field scope](docs/demo-refresh-20261028/METHOD.md), [validation receipt](docs/demo-refresh-20261028/PUBLIC_DATA_VALIDATION.json), and the Chinese README for exact row counts/build bindings. Current manifests are `demo/data/manifest_v8.json` and `demo/shanghai-validation/data/manifest_v8.json`. Old sample payloads are retained in Git history, while old builder scripts are historical references.
