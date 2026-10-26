# Mars Cosmetics: Order & Inventory Sync Service (v1 design)

**Stack:** Node.js / NestJS, PostgreSQL (RDS Multi-AZ), Redis (ElastiCache, BullMQ), AWS ECS Fargate, ap-south-1.

![Architecture](architecture.png)

## 1. Architecture

One NestJS codebase, three ECS services: **`api`** (webhooks, ops UI), **`worker`** (BullMQ processors for orders, per-channel stock, ERP) and **`scheduler`** (Flipkart poll, Blinkit CSV, Business Central (BC) reconcile, sweeper; single active instance via a Postgres advisory lock). A modular monolith fits 3 developers and 6 weeks.

**PostgreSQL is the only source of truth; Redis only wakes workers.** Every unit of work — inbox event, stock push, ERP call — is a row with a status, a lease (`locked_until`) and an attempt count, written in the same transaction as the business change; jobs enqueue after commit. A 30-second **sweeper** re-enqueues `PENDING` rows and reclaims expired leases, so losing Redis costs ~30 s of catch-up, not data. `api` and `worker` autoscale on queue depth and CPU (min 2 / max 8 tasks, two AZs); RDS and ElastiCache are sized for peak.

## 2. Data model

- **`sku_stock`** — `sku` PK, `on_hand`, `reserved`, `version`, `sell_rate` (units/min); `CHECK (reserved >= 0 AND on_hand >= reserved)`.
- **`orders`** — `channel`, `external_order_id`, `status`, `source_updated_at`; **`UNIQUE(channel, external_order_id)`**. **`order_items`** — PK `(order_id, line_no)`, `sku`, `qty`, `unit_price`.
- **`order_events`** (inbox) — `channel`, `dedupe_key`, `type`, raw `JSONB`; **`UNIQUE(channel, dedupe_key)`**.
- **`stock_ledger`** (append-only) — `sku`, `reason` (RESERVE/RELEASE/DISPATCH/RECEIPT/ADJUST/RECONCILE), delta, before/after, `order_id`, `actor`, `ts`; **`UNIQUE(idempotency_key)`**.
- **`channel_stock_state`** — PK `(channel, sku)`, `mode`, `share_qty`, `consumed_qty`, `target_qty`/`target_version`/`computed_at`, `pushed_qty`/`pushed_version`.
- **`erp_outbox`** — `order_id`, `seq`, `type` (CREATE/CANCEL), `erp_doc_no`; **`UNIQUE(order_id, seq)`**. Plus `stock_push_log`, and `channel_cursor` (`cursor`, `observed_through`, `lag_seconds`).

All three work tables also carry `status`, `attempts`, `next_attempt_at`, with partial indexes on `status IN ('PENDING','RETRY')` by `next_attempt_at` and on `locked_until`; plus `stock_ledger(sku, id)`. **No duplicate orders**, in three layers: unique `(channel, dedupe_key)` on events (provider event ID, else `sha256(channel|order_id|type)`), unique `(channel, external_order_id)` on orders, and a unique ledger `idempotency_key` so a retried transaction cannot reserve twice. **States:** `RESERVED → DISPATCHED → RETURNED`; `RESERVED → CANCELLED`; `STOCK_SHORT → CANCELLED_BY_US`, forward only. An order is **confirmed**, and owed to the ERP, at `RESERVED`.

## 3. Order ingestion

- **Webhooks:** verify signature, `INSERT ... ON CONFLICT DO NOTHING`, return 200 after commit; on DB failure return 5xx so the channel retries. Enqueue is best-effort — the sweeper backs it up.
- **Flipkart polling:** every 2 min on an `(updated_ts, id)` cursor with 10 min overlap. Persist each page before advancing, never skip a failed page, re-read 24 h hourly for late orders.
- **Duplicates** are no-ops, still acked. **Out-of-order:** events older than `source_updated_at`, or non-forward, are stored and ignored. **Cancel before create:** the cancel inserts the order as `CANCELLED`; the late create sees a terminal state and reserves nothing.
- **Unknown SKU** is flagged `UNKNOWN_SKU` with its own alert, not treated as out of stock. **Channel down:** when `observed_through` falls more than 2× its expected `lag_seconds` behind, that channel's advertised tail stock drops to 0 — we stop selling what we cannot see.

## 4. Inventory consistency

**Key fact:** a marketplace order arrives *after* the customer has bought, so reservation cannot reject it — only record it or mark it short. **Reservation is the correctness boundary** (stock never goes negative; one order wins the last unit); **overselling is prevented by what we advertise.** A channel exceeding its share is never rejected — only insufficient *global* stock is.

**Reservation transaction** — one savepoint wraps the loop so a short order releases every line; rows lock in `sku` order, so concurrent orders cannot deadlock:

```text
BEGIN
  insert order_events ON CONFLICT DO NOTHING; duplicate -> COMMIT
  o = upsert orders FOR UPDATE; stale/terminal -> mark processed, COMMIT
  SAVEPOINT reserve_order
  for line in merged lines sorted by sku:
    row = UPDATE sku_stock SET reserved = reserved + :q, version = version + 1
          WHERE sku = :s AND on_hand - reserved >= :q   -- row lock serializes orders
    if no row: ROLLBACK TO reserve_order; o.status = STOCK_SHORT; alert ops; COMMIT
    insert stock_ledger(RESERVE, before/after, key = order:line:RESERVE)
    UPDATE channel_stock_state SET consumed_qty += :q   -- accounting, never rejects
  o.status = RESERVED; insert erp_outbox(order, seq=1, CREATE); mark skus dirty
COMMIT      -- then enqueue; sweeper covers a failed enqueue
```

Targets are recomputed by the stock worker off the `dirty` flag, so the hot row lock is held only for the conditional `UPDATE` and its ledger row — which matters on a launch SKU, where every order serializes on one row.

**What each channel is shown.** `available = on_hand − reserved`. One rule: **each marketplace gets a bounded, lag-discounted allocation; the website takes the residual** (it reads its target live from our DB, so its lag is ~0 and it never needs a push). Each marketplace has a lag `L_c` (§5) in which it may have sold units we have not seen, so we never advertise what that window could consume: `inflight(c) = ceil(sell_rate × L_c)`.

- **Shared mode**, when `available >= B + 3 × cap` (`cap` = display cap, e.g. 20; `B = ceil(1.5 × sell_rate × L_max)` is the same deduction applied once globally): each marketplace shows `cap`, the website shows `available − B − 3 × cap`. Even if all three sell their full display, `B` is untouched, so exposure is bounded by construction and healthy SKUs rarely need a push.
- **Tail mode** below that: each marketplace gets `share_qty` weighted by 7-day sales and advertises `max(0, share_qty − consumed_qty − inflight(c))`, under the invariant **`Σ advertised + Σ inflight <= available`**. An overrun never rejects the order; it lowers other channels' shares in the same transaction, and cancellations enter a free pool re-partitioned only once lowered values are acked. **This is a bound, not a proof** — a channel selling far above its 7-day rate inside its lag window can still overrun, and a lowering push takes up to `L_c` to land. That residual is measured (§7), not assumed away.

**Stale absolute pushes.** A channel may already have sold units we have not ingested, so "set to N" could overwrite its local decrement. Therefore **decreases push immediately; increases wait until that channel's `observed_through` reaches the target's `computed_at`**, and pushes are single-flight per `(channel, sku)` comparing on `target_version`. This delays restocking after a cancellation, deliberately.

**Last unit, two channels, same second.** `available = 1`, tail mode: Nykaa's share is 1, the others 0. But Flipkart's panel still shows 1 from *before* the lowering push landed (`L_c` ≈ 3 min) — precisely the residual above — so both sell. Order A reaches reservation first: the conditional `UPDATE` takes the row lock, `1 − 0 >= 1` holds, `reserved = 1`. Order B blocks on that lock, re-evaluates `1 − 1 >= 1`, matches no row, and is saved `STOCK_SHORT` — ops alerted, every channel pushed 0. Ops cancels B on the channel panel (A6), counting against the 0.5 % budget (80,000 × 0.5 % = 400 orders). Postgres guarantees one winner; the lag-discounted shares make B rare rather than routine.

**Cancel/return.** Before dispatch, release once (unique ledger key) and enqueue ERP `CANCEL` (seq 2); on dispatch (BC shipment poll), `on_hand −= q, reserved −= q`. Returns raise `on_hand` only after a receipt is entered. **`on_hand` moves only via ledger entries**; a 15-min BC pull writes `RECONCILE` rows and alerts on drift.

## 5. Stock push within 60 seconds

Target: 60 s from a stock change to an acked push, at normal load. The worker reads the latest target, sends an absolute value, records `pushed_version` on ack. Rapid changes coalesce (100 → 95 → 90 is one call); dirty rows go lowest-published-quantity first.

| Channel | Limit | Operate at | Lag `L_c` |
|---|---|---|---|
| Website | own DB | reads target live; no push | ~0 |
| Nykaa | 1 SKU/call, 5 rps | 4 rps = **240 SKU/min** | ~2 min |
| Flipkart | 50 SKU/call, 10 rps | 8 rps = 24,000 SKU/min | ~3 min (2-min poll) |
| Blinkit | REST + daily CSV, ~2 % timeouts | 5 concurrent (A7) | ~1 min |

Per-channel queues and limiters stop a slow channel blocking others; 429s honour `Retry-After`. Blinkit's CSV is generated immediately before upload, and any row newer than the snapshot is re-pushed by REST. **Nykaa is the binding constraint** at ~240 distinct SKUs/min: only tail SKUs generate calls, but a sale drives many into tail at once and peak volume (~600 line changes/min) can exceed it. Lowest-stock-first ordering and Nykaa's `inflight` deduction (from `L_c` = 2 min, which makes a stale display safe to sell) absorb this. Flipkart's 2-min poll delays *detection*, feeding `L_c`; its push latency meets 60 s easily.

## 6. Failure handling

- **429 / timeout / 5xx** (Blinkit ~2 %): honour `Retry-After`, else exponential backoff with jitter (2 s → 5 min); absolute values make repeats safe. **4xx (poison):** no retry, `FAILED`, surfaced to ops — stock rows retry every 5 min, ERP rows up to 48 h. **Crashed worker:** the sweeper reclaims expired `locked_until` leases.
- **Stuck scheduler:** the advisory lock gives mutual exclusion, not liveness — a hung-but-alive holder would silently stop the sweeper — so it writes a heartbeat each tick, and "last sweep > 2 min" pages.
- **ERP Sunday downtime:** `erp_outbox` stays `PENDING`; the breaker opens, probes each minute, then drains through a limiter in `seq` order, single-flight. A 2 h outage is ~700 orders normally, ~36,000 on a sale day, so we alert on *projected drain time*, not backlog size. If BC cannot sustain ~10 orders/s (A8), we bulk-insert to a BC staging table (100/call).
- **ERP exactly once:** a stable `MARS-<channel>-<external_order_id>` reference with lookup-before-create; on timeout we retry the create and treat BC's duplicate-reference rejection as success. Lookup alone is insufficient (a timed-out create may not be visible yet), so a daily outbox-vs-BC reconciliation catches strays.
- **Ops screen:** a NestJS page behind SSO listing `FAILED`/stuck rows across events, stock and ERP. **Retry** sets `PENDING` and enqueues; unique keys make double-creation impossible, and manual corrections write an `ADJUST` ledger row with the user as `actor`. Queues, limiters and breakers are per channel, so no outage blocks reservation.

## 7. Observability

Day-one alerts: (1) `STOCK_SHORT` / orders over 15 min (warn 0.3 %, page 0.5 %); (2) **exposure** — `Σ_sku max(0, Σ_channel advertised − available)`, the direct measure of §4's residual; (3) stock freshness — oldest dirty tail SKU per channel (warn 45 s, page 60 s) and Flipkart poll lag > 4 min; (4) oldest `PENDING` event > 30 s, webhook 5xx rate, sweeper heartbeat age; (5) ERP projected drain time and SKUs with BC drift.

**Audit:** "why did SKU X change, and when?" is `SELECT * FROM stock_ledger WHERE sku = X ORDER BY id`; "what did channel Y see?" is `stock_push_log`. **Validating 0.5 %** (to be measured, not claimed): a pre-launch sale-day simulation with same-SKU bursts, stale displays, Nykaa 429s, Flipkart lag and Blinkit timeouts. The units differ — 900 oversold *units* last sale versus a 0.5 % cap on cancelled *orders*.

## 8. Scope: left out of v1

**Second warehouse** (adds a location dimension to every stock row and to allocation). **Auto-cancel on marketplaces** (no cancel APIs in the brief; ops cancels manually). **Partial-line orders** — all-or-nothing, which at ~2 lines/order roughly doubles the order-level shortage rate. **Forecasting or auto-tuned allocation** (7-day weights suffice; ML is premature).

## 9. Assumptions

**A1** Webhooks signed or token-authenticated; secrets in Secrets Manager; ops UI behind SSO. **A2** Channels supply a stable order ID and a timestamp or version. **A3** Stock APIs take an absolute quantity and are idempotent for the same value; none offers a conditional update. **A4** Webhook channels retry on non-2xx and deliver within ~30 s. **A5** ~2 lines/order; sale day 80,000 orders. **A6** Marketplace cancellations are manual. **A7** Blinkit REST has no stated limit; 5 concurrent used. **A8** BC enforces a unique external document number (or idempotent create), sustains ~10 orders/s, and exposes shipments, on-hand and the SKU master. **A9** One warehouse; IST.

## 10. Trade-offs

| Decision | Chosen | Rejected | Why |
|---|---|---|---|
| Stock exposure | Lag-discounted allocations; website takes the residual | Static per-channel allocation; one global number | Static allocation strands stock and needs manual rebalancing mid-sale; one number caused the last oversell. |
| Reservation | Atomic conditional `UPDATE` in Postgres | Redis counter or distributed lock | Redis can lose writes on failover; the DB row lock is the single correctness boundary. |
| Queueing | BullMQ + Postgres rows + sweeper | SQS + outbox relay + DLQs | Postgres already holds every unit of work, so the queue need not be durable. Cost: ~30 s catch-up on Redis loss. |
| ERP delivery | Unique reference, lookup-before-create, reconciliation | "Exactly-once" from queue semantics | No queue gives exactly-once to a third party; receiver-side identity does. |

**Cost (order of magnitude, ap-south-1):** ECS ~₹10k, RDS Multi-AZ ~₹12k, ElastiCache ~₹5k, ALB ~₹2.5k, NAT ~₹4k, CloudWatch/S3/Secrets ~₹5k — about **₹40k/month**, leaving headroom in the ₹50k budget.

**AI usage:** ChatGPT drafted my first design (per-channel allocation, SQS); Claude critiqued it and proposed the allocation model and BullMQ-plus-sweeper queueing used here; ChatGPT then reviewed that and I worked through its objections. I chose between the designs and can defend every decision here.
