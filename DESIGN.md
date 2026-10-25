# Mars Cosmetics: Order & Inventory Sync Service (v1 design)

**Stack:** Node.js / NestJS (TypeScript), PostgreSQL (RDS), Redis (ElastiCache, BullMQ), AWS ECS. **Date:** 24 Oct 2026.

![Architecture](architecture.png)

## 1. Architecture

One NestJS codebase, one Docker image, **three ECS services**: `api` (webhooks, ops API, ops UI), `worker` (BullMQ processors: order, per-channel stock, ERP) and `scheduler` (Flipkart poll, Blinkit CSV, Business Central reconcile, sweeper). A modular monolith is right for 3 developers and 6 weeks; workers still scale independently.

**Principle: PostgreSQL is the only source of truth. Redis only wakes workers up.** Every unit of work (an inbox event, a stock target to push, an ERP call) is a row with a status. Jobs are enqueued after commit; a 30-second **sweeper** re-enqueues any row still `PENDING`. If Redis is lost, nothing is lost. This replaces a separate outbox relay and SQS (see trade-offs).

**Flow:** (1) webhook is verified and written to `order_events` (poll results too), acked only after commit. (2) order processor normalizes and dedupes. (3) one DB transaction reserves stock, updates order state, writes ledger rows, recomputes channel targets and inserts the ERP outbox row. (4) stock workers push targets. (5) ERP worker creates the sales order in Business Central. (6) scheduler pulls shipments and on-hand from Business Central.

**Capacity:** peak 300 orders/min is 5/s, with about 2 lines per order. A single Postgres primary handles this easily; the hot path is one indexed `UPDATE` per line.

## 2. Data model

| Table | Key columns / constraints |
|---|---|
| `sku_stock` | `sku` PK, `on_hand`, `reserved`, `version`, `peak_rate`; `CHECK (reserved >= 0 AND on_hand >= reserved)` |
| `orders` | `id` PK, `channel`, `external_order_id`, `status`, `source_updated_at`; **`UNIQUE(channel, external_order_id)`** |
| `order_items` | `order_id` FK, `sku` FK, `qty`, `unit_price`; PK `(order_id, sku)` |
| `order_events` (inbox) | `id`, `channel`, `dedupe_key`, `type`, `event_time`, raw `JSONB`, `status`; **`UNIQUE(channel, dedupe_key)`** |
| `stock_ledger` (append-only) | `id`, `sku`, `reason` (RESERVE, RELEASE, DISPATCH, RECEIPT, ADJUST, RECONCILE), `delta_on_hand`, `delta_reserved`, before/after, `order_id`, `actor`, `ts`; **`UNIQUE(idempotency_key)`** |
| `channel_stock_state` | PK `(channel, sku)`, `target_qty`, `target_version`, `pushed_version`, `share_qty` (null = shared mode), `status`, `attempts`, `next_attempt_at`, `last_error` |
| `stock_push_log` (append-only) | `channel`, `sku`, `qty`, `version`, `result`, `http_status`, `ts` |
| `erp_outbox` | `order_id`, `seq`, `type` (CREATE, CANCEL), `status`, `attempts`, `erp_doc_no`, `last_error`; **`UNIQUE(order_id, seq)`** |
| `poll_cursors` | `channel` PK, `cursor`, `updated_at` |

**Duplicate orders** are blocked in three layers: `UNIQUE(channel, dedupe_key)` on events, `UNIQUE(channel, external_order_id)` on orders, and `UNIQUE(idempotency_key)` on ledger rows (so a retried transaction cannot reserve twice). `dedupe_key` is the provider event ID if there is one, otherwise `sha256(channel | order_id | event_type | source_updated_at)`.

**Order states:** `RESERVED → DISPATCHED → RETURNED`; `RESERVED → CANCELLED`; `STOCK_SHORT → CANCELLED_BY_US`. Only forward transitions are applied.

## 3. Order ingestion

- **Webhooks (website, Nykaa, Blinkit):** verify signature, `INSERT ... ON CONFLICT DO NOTHING` into `order_events`, return 200 after commit. If the insert fails, return 5xx so the channel retries. Enqueue is best effort; the sweeper covers misses.
- **Flipkart polling:** every 2 minutes with a **10-minute overlap window**. Overlap is harmless because of dedupe. Persist each page before advancing the cursor; never advance past a failed page.
- **Duplicates (Nykaa):** absorbed by the unique keys above; a duplicate is a no-op that is still acked.
- **Out of order:** an event older than `orders.source_updated_at`, or not a forward transition, is stored and ignored. **Cancel before create:** the cancel creates an `orders` row directly in `CANCELLED`; the late `ORDER_CREATED` then sees a terminal state and reserves nothing.
- **Channel down:** Flipkart poll failing means the cursor stays put and alerts fire. If we cannot see a channel's orders for more than 5 minutes (Flipkart lag > 5 min), that channel's **published stock for low-stock SKUs drops to 0** until it catches up, so we stop selling what we cannot see. Webhook channels that cannot reach us rely on the channel's own retry (assumption A4); our API runs on 2+ tasks across AZs.

## 4. Inventory consistency (the core problem)

**Key fact:** a marketplace order arrives *after* the customer bought. Reservation cannot reject it; it can only bookkeep it or flag it for cancellation. **Overselling is therefore prevented by what we publish, not by the reservation.** The reservation guarantees only that stock never goes negative and exactly one order wins the last unit.

**Reservation (hard guarantee).** One atomic conditional update per line, in sorted SKU order, all lines in one transaction:

```text
BEGIN
  insert order_events ON CONFLICT DO NOTHING; if duplicate -> COMMIT (no-op)
  o = upsert orders ... FOR UPDATE; if stale/terminal -> mark processed, COMMIT
  for line in lines sorted by sku:
    row = UPDATE sku_stock SET reserved = reserved + :q, version = version + 1
          WHERE sku = :s AND on_hand - reserved >= :q
          RETURNING on_hand, reserved                 -- row lock serializes concurrent orders
    if no row: ROLLBACK TO savepoint; o.status = STOCK_SHORT; insert ops alert; COMMIT
    insert stock_ledger(RESERVE, before/after, idempotency_key = order:line:RESERVE)
  o.status = RESERVED
  insert erp_outbox(order_id, seq=1, CREATE)
  recompute_targets(skus)   -- upsert channel_stock_state, bump target_version if changed
COMMIT   -- then enqueue jobs; sweeper covers a failed enqueue
```

**What each channel is shown (the actual oversell defence).** `available = on_hand - reserved`.

- **Healthy stock (`available > B`): shared mode.** Publish `min(display_cap, available - B)` to every channel, where **B = ceil(1.5 × sku_sell_rate × L)** and L is the worst-case lag (Flipkart: 2 min poll + processing, about 3 min). B covers what all channels can sell before they see each other's sales. Example: a SKU selling 20/min needs B ≈ 90. Display cap (e.g. 20) also means most orders do not change the pushed number at all, which matters for Nykaa (§5).
- **Tail stock (`available <= B`): partitioned mode.** Freeze a share per channel (`share_qty`, weighted by 7-day channel sales). Each channel shows only its remaining share, so the sum of shares never exceeds real stock and **two channels cannot sell the same unit**. Shares are only lowered by that channel's own sales; units from cancellations go to a free pool and are re-partitioned only after all lowered values are acknowledged and one lag period passes. Stranding is bounded to B units per SKU.

**Last unit, two channels, same second.** `available = 1`, so the SKU is in tail mode and only Nykaa holds the share (it has the highest weight); the others show 0. The realistic failure is stale display: a website sale 30 s ago left 1 unit but pushes have not landed, so Nykaa and Flipkart both still show 1 and both sell. Orders arrive as separate events. Order A runs the conditional `UPDATE`: row lock, `1 - 0 >= 1`, success, `reserved = 1`. Order B waits for the lock, then `1 - 1 >= 1` fails, 0 rows, so B is saved as `STOCK_SHORT` with no ledger row and an ops alert. All channels are pushed 0. Ops cancels B on the channel panel (assumption A6) and it counts against the 0.5% budget. **The DB guarantees one winner and no negative stock; shared-mode buffer and tail partition keep the loser rare.**

**Budget check:** 0.5% of 80,000 sale-day orders is 400 cancellations (last sale: about 900 units oversold). Because stale-display exposure is limited to the tail and to fast-moving SKUs with a buffer, the residual should be a small fraction of 400; measure it on the first sale (§7).

**Cancel/return.** Before dispatch: release the reservation once (`UNIQUE` ledger key), enqueue ERP `CANCEL` (seq 2, processed after CREATE). Dispatch (from Business Central shipment poll): `on_hand -= q, reserved -= q`. Post-dispatch returns add to `on_hand` only after warehouse receipt is entered (ops screen or Business Central receipt), then become sellable. **Where on_hand comes from:** only ledger entries change it (DISPATCH, RECEIPT, ADJUST); a 15-minute Business Central on-hand pull is a *drift check* that writes `RECONCILE` ledger rows and alerts when drift is large.

## 5. Stock push within 60 seconds

Targets live in `channel_stock_state`; the queue carries only `(channel, sku)`. A worker reads the **latest** target just before sending, sends an **absolute "set to N"**, and marks `pushed_version` only on acknowledgement. Rapid changes coalesce; retries and replays are harmless. Dirty rows are picked **lowest published quantity first**, so SKUs closest to selling out go first.

| Channel | Limit | Operate at | Notes |
|---|---|---|---|
| Nykaa | 1 SKU/call, 5 rps | 4 rps = **240 SKU/min** | the bottleneck: refreshing all 1,200 SKUs takes 5 min, so full refresh is nightly only |
| Flipkart | 50 SKU/call, 10 rps | 8 rps = 400 SKU/s | never the bottleneck; honour `Retry-After` on 429 |
| Blinkit | REST + daily CSV, ~2% timeouts | 5 concurrent (A7) | retry same absolute value; CSV generated **just before upload** from current targets, then every row with `target_version` newer than the CSV snapshot is re-pushed by REST (avoids stale CSV overwriting urgent changes) |
| Website | own DB | n/a | read live from `sku_stock` (no push lag) |

Per-channel BullMQ queues with Redis rate limiters keep a slow channel from blocking others. Honest limit: Nykaa meets 60 s only while fewer than about 240 distinct tail SKUs change per minute; display cap and lowest-stock-first keep that true in practice, and the buffer covers the rest. Flipkart cannot meet 60 s end to end because orders are only visible via 2-minute polling; that is why L includes it in B.

## 6. Failure handling

| Case | Behaviour |
|---|---|
| 429 | Honour `Retry-After`, re-delay the job, do not count an attempt. |
| Timeout / 5xx (Blinkit ~2%) | Exponential backoff with jitter (2 s doubling to 5 min); the push is idempotent so a timed-out call is simply repeated. Row stays dirty. |
| ERP Sunday downtime | `erp_outbox` rows stay `PENDING`; circuit breaker opens after N failures and probes every minute; on recovery the backlog drains through a rate limiter. Per-order ordering (CREATE before CANCEL) via `seq` and single-flight per order. A normal 2 h outage is about 700 orders; a sale-day outage up to about 36,000, so Business Central must accept 10+ orders/s (A8) or backlog drain exceeds the window. |
| ERP exactly once | Stable external document no. `MARS-<channel>-<external_order_id>`. Before any retry, **look up by that number first, then create** (A8 requires Business Central lookup by external doc no.). At-least-once transport plus lookup plus single-flight per order gives exactly-once business effect. |
| 4xx validation (poison) | No retry. Marked `FAILED` with the error; appears in ops screen. |
| Retries exhausted | Stock: `FAILED`, still dirty and retried by sweeper every 5 min. ERP: retries up to 48 h, then `FAILED`. |
| Ops screen | Lists `FAILED`/stuck rows from the DB (not from Redis). **Retry** sets status `PENDING` and enqueues; it can never create a second order or reservation because of the unique keys. Retries are logged with user ID. |
| Isolation | Separate queue, rate limiter and breaker per channel and for ERP; a channel/ERP outage never blocks reservation. |

## 7. Observability (day one)

1. **Oversell:** `STOCK_SHORT` / orders, rolling 15 min. Warn at 0.3%, page at 0.5%.
2. **Stock freshness:** age of oldest dirty `channel_stock_state` row for tail SKUs, per channel. Warn 45 s, page 60 s. Also Flipkart poll lag > 4 min.
3. **Ingest lag:** age of the oldest `PENDING` order event (> 30 s) and webhook 5xx rate.
4. **ERP backlog:** `PENDING` count and oldest age. Alert at 30 min outside the Sunday window, 90 min inside.
5. **Failures and drift:** rows in `FAILED` or exhausted, and SKUs where our `on_hand` differs from Business Central after reconcile.

**Audit:** "why did SKU X change?" is `SELECT * FROM stock_ledger WHERE sku = X ORDER BY id`; "what did channel Y see?" is `stock_push_log`.

## 8. Scope: left out of v1

| Left out | Why |
|---|---|
| Second warehouse / routing | Out of brief; adds a location dimension to every stock row. Later: `(warehouse, sku)` stock and routing rules. |
| Auto-cancel on marketplaces | No cancel APIs in the brief; ops cancels manually. |
| Returns inspection workflow | Needs warehouse process we do not control; v1 takes a receipt entry only. |
| Demand forecasting / auto-allocation tuning | Weights from 7-day sales are enough; ML is premature. |
| LLM "why did SKU X oversell" assistant | Ledger and push log already give the answer; build after data exists. |
| Per-line partial order handling | Order is all-or-nothing; ops decides on `STOCK_SHORT`. |

## 9. Assumptions

- **A1** Webhooks are signed (HMAC) or token-authenticated; secrets live in Secrets Manager.
- **A2** Every channel gives a stable order ID and a timestamp or version; a provider event ID is used when present.
- **A3** All stock APIs accept an absolute quantity and are idempotent for the same value.
- **A4** Nykaa and Blinkit retry webhooks on non-2xx for a reasonable period.
- **A5** An order is "confirmed" on arrival (channels only send paid or COD-accepted orders); average 2 lines per order.
- **A6** Marketplace cancellations are done manually by ops (no cancel API in the brief).
- **A7** Blinkit REST has no stated rate limit; we use 5 concurrent as a conservative default.
- **A8** Business Central supports lookup by external document number, accepts 10+ orders/s, and exposes shipments and on-hand via API.
- **A9** SKU master (1,200 SKUs) is loaded nightly from Business Central; single warehouse; IST throughout.
- **A10** Sale-day volume is 80,000 orders; peak 300/min.

## 10. Trade-offs

| Decision | Chosen | Rejected | Why |
|---|---|---|---|
| Stock exposure | Shared pool minus buffer B; partition only the tail | Static per-channel allocation for all stock; same global number on every channel | Static allocation strands stock and needs constant rebalancing; the same number everywhere is what caused the 900 oversells. Tail-only partition gives a hard guarantee exactly where it matters. |
| Reservation | Atomic conditional `UPDATE` in Postgres | Redis counter or distributed lock | Redis can lose writes on failover and drift from DB; the DB row lock is the single correctness boundary. |
| Queueing | BullMQ on Redis + Postgres status rows + sweeper | SQS + outbox relay + DLQs | Redis is already in the stack; BullMQ gives retries, delay and rate limiting; fewer moving parts for 3 devs. Cost: Redis loss means sweeper catch-up (about 30 s), not data loss. |
| App shape | Modular monolith, 3 ECS services | Microservices | 6 weeks / 3 developers; scaling per role is still possible. |
| ERP delivery | Lookup-before-create + stable external doc no. + single-flight | "Exactly-once" via queue semantics | No queue gives exactly-once to a third party; receiver-side identity does. |
| Stock updates | Absolute "set to N" with latest-target read | Delta updates | Deltas double-apply on retry and break on timeouts (Blinkit). |

**Cost (rough, ap-south-1, ±25%, verify in AWS Pricing Calculator):** ECS Fargate ~₹10k, RDS Multi-AZ db.t4g.medium + storage ~₹12k, ElastiCache 2 small nodes ~₹5k, ALB ~₹2.5k, NAT ~₹4k, CloudWatch + S3 + Secrets ~₹5k = **about ₹40k/month**, leaving headroom for sale-day scale-out.

**AI usage:** ChatGPT drafted my first version; Claude reviewed it, found gaps and helped rewrite this version. I reviewed and can defend every decision.
