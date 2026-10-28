# Fixed-user frozen-v8 replay: extraction contract

## Selection

Read the accepted publication's CURRENT and COMPLETE metadata using its bundled reader. Each selected partition is independently payload-verified and decoded. Candidate enumeration follows original catalog order, expands until each GBA city has at least2,000 eligible unique identities, and retains at most2,400 candidates per city; HMAC-keyed ranking chooses2,000 within that pool. This run inspected36 GBA partitions and8 Shanghai partitions. The sample is a convenience draw from these candidate partitions, not an equal-probability population sample.

Assign GBA identity once, using its earliest available DAY_START representative H3 and the existing city lookup. A global seen-set prevents the same source UUID from entering multiple GBA cities. Shanghai uses the residence cohort only, not flow+residence separately. All available delivered dates of each selected identity are retained. Different region/date namespaces do not establish linked natural persons.

## Private and public identities

A fresh32-byte secret outside the repository derives city-prefixed20-hex public IDs. The private source UUID mapping, selection parquet files, raw extracts and salt stay on J: and are excluded from version control. Source IDs are32hex/36UUID forms; coverage is asserted for all20,000 selected source identities, then all decompressed public shards are scanned for either original format. Pseudonyms do not anonymize visible trajectories. POI names are withheld; existing demo coordinate precision is retained.

## Input and row coverage

GBA uses the accepted full seven-table source build, not a cross-city subset or old500k sample. Two standardized sorted input sources covering the selected identities were independently SHA-readback verified against the accepted source request. Shanghai uses original formal analysis-day input views, directly read through a private mapping to their reversible recycle-bin location after cache cleanup. The581-source original plan is unchanged; all32 read source date/SHA/byte/mtime bindings match it. Thus long-duration cross-date event expansion exactly follows the accepted input, rather than assigning original standardized-file dates as analysis dates. An early unpublished standardized-file-date partial was abandoned and is not in public data.

The independent validator compares raw/nodes/segments counts for every selected identity/date against the private selected context. GBA has87 raw-only sample-date slots and5,846 empty slots; Shanghai has4,508 empty slots. These remain selectable with empty processed nodes or empty observations; they are not silently dropped or synthesized. Slot counts(36,000/28,000) are not unique-user counts(18,000/2,000).

## Browser projection semantics

Node and segment orders and segment endpoints come from frozen-v8 output. Representative node positions are H3 centroids, raw coordinates retain existing display rounding, and dates/times are display projections. Required full API fields not represented here are not recoverable from the demo. h10c/merged-members/duration use available exact decoded node fields; unavailable station-evidence/drop fields are null, not invented0/1 values. POI strings are intentionally empty. Raw K/S/D labels are primary-H3/time-envelope display matching and explicit input flags, not the full engine's audit classifications.

Per-node city is looked up independently. Cells outside the known lookup have city=null; a segment's cross field is null when either endpoint city is unknown. The sample-cohort city never fills an unknown node city. GBA lookup/H3-center boundary uncertainty remains; labels are not independent city-visit truth.

## Delivery and browser loading

GBA manifest covers nine cities and two actual dates; Shanghai manifest covers14 actual date slots. Every date is divided into ten200-ID pages. Only a manifest and one processed/raw gzip pair are requested initially, with standard browser DecompressionStream support. Base tiles use OpenStreetMap's official no-key endpoint with attribution and no prefetch. Dataset payload integrity and browser/runtime/network checks are reported separately.
