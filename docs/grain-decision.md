# Grain Decision: Source Endpoint & Fact-Table Grain

## Decision

The Silver fact table `fact_prices_5m` is sourced from the OSRS Wiki Prices API `/5m`
endpoint.

- **Grain:** one row per item per aligned 5-minute window.
- **Natural key:** `(item_id, window_timestamp)`.
- **`window_timestamp` source:** the response-level `timestamp` field, **not** the time
  the pipeline polled.

## Why the `/5m` endpoint and not the `/latest` endpoint

Both endpoints expose instabuy (`high`) and instasell (`low`) prices, but they differ in
a way that matters for a scheduled batch pipeline.

**`/latest`** returns, per item, the most recent instabuy and instasell *events* with
their own unix timestamps (`highTime`, `lowTime`). These timestamps are **per-item and
asynchronous** — this returns the latest time that an item was instabought or instasold, and these two
sides of an item can be either seconds or days apart. There is no single point-in-time
that groups a poll into a regular, repeatable interval. `/latest` answers "what is the
price right now"; it does not bucket cleanly into fixed intervals.

**`/5m`** returns, per item, the *average* instabuy/instasell price and the trade
**volume** for each side, aggregated over a fixed 5-minute window, plus a single
**response-level `timestamp`** shared by every item in the payload. This provides exactly
the clean, aligned point-in-time that `/latest` lacks, and the volume fields feed the
flip-opportunity ranking directly.

## Why the window timestamp, not poll time (idempotency)

The record is keyed on the response's `timestamp` (the window the data belongs to), not
on wall-clock poll time. This makes Bronze→Silver loads idempotent:

- If a scheduled run is late or Airflow retries it, poll time differs across attempts but
  the window timestamp is identical.
- A Delta `MERGE` on `(item_id, window_timestamp)` therefore treats retries of the same
  window as the same row and overwrites rather than duplicating.
- Keying on poll time would produce duplicate rows for a single underlying window on any
  retry or late run.

## Consequences

- Bronze lands raw `/5m` responses append-only, preserving the response `timestamp`.
- Silver MERGE is keyed on `(item_id, window_timestamp)` and is safe to retry.
- The 2% Grand Exchange tax is applied downstream in Gold when computing net flip margin;
  it is not part of the raw grain.