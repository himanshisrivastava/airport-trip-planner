# Airport Trip Planner

## 1. Purpose

The Airport Trip Planner recommends shops and restaurants for a traveller to visit inside an airport before their flight. Recommendations are personalised to three signals:

- **User preferences** — the traveller's stated tastes (e.g. luxury fashion, cheap fast food, coffee).
- **Flight timing** — how much time the traveller has before boarding, and where their gate is, so every recommendation is physically reachable and fits the available window.
- **Live sales** — active discounts at airport outlets, so a relevant deal can surface a shop the traveller might otherwise have skipped.

The core modelling challenge is that outlets describe themselves with **non-standardised, brand-supplied keywords** (McDonald's → *fast food, cheap, takeaway*; Coach → *luxury, fashion, service*). There is no shared taxonomy, so matching preferences to outlets is a semantic problem, not an exact-match one. The system solves this with embeddings and vector similarity search rather than keyword equality.

### Requirements

**Functional**

- A user can see recommended shops/restaurants based on their preferences and flight timing, on mobile or desktop.
- Recommendations are delivered to the user via push notification, and on demand via API when they open the app.

**Non-functional**

- **Availability over consistency** — an outlet missing from a recommendation list momentarily is acceptable; the system should stay available rather than block on perfect data.
- **Low latency** — recommendation reads target ≤ 500 ms.
- **Scale** — 100s of airports * 100s of outlets * 1000s of travellers

---
## 2. Assumptions

The design rests on a few explicit assumptions. Each is a simplifying choice that keeps the current design lean; the "if this changes" note records what would need revisiting.

**A user is at one airport at a time.** A traveller has a single active flight context, so recommendations are always scoped to one airport. This is why the redis key can be `rec:{userId}:{airport}` and why the in-memory flight context fetched at the filter step is unambiguous. *If this changed* (e.g. connecting flights handled simultaneously, or a multi-leg itinerary), the key and the context model would need to carry a leg/segment dimension rather than a single airport.

**Store (outlet) metadata changes infrequently.** Outlet details — name, category, keywords, location — are treated as slow-moving. This is what makes the offline embedding pipeline viable: the `keywords-embedding-service` regenerates embeddings on an hourly schedule rather than reacting to every change, and retrieve reads a near-static index. *If this changed* (frequent menu/keyword churn, pop-up outlets), embedding regeneration would need to become event-driven, and the retrieve step's assumption of a stable candidate set would weaken. Note this is deliberately separate from **sales**, which *are* expected to change frequently and are therefore handled on the real-time Kafka path, not the offline embedding path.

**A 6-hour recommendation window.** The hourly batch generates recommendations for users boarding within the next 6 hours, and 6 hours is the upper bound on cache TTL. The reasoning: it is long enough to cover a realistic pre-flight arrival window (most travellers are airside somewhere between roughly 1 and 3 hours before departure, with international and buffer-conscious travellers arriving earlier), so a user who lands airside early still has recommendations ready; but it is short enough that the batch isn't precomputing for flights so far out that gate assignments, sales, or the traveller's own plans are likely to change before they matter. The actual TTL is `min(6 hr, time-to-boarding)` so the window only ever shrinks toward boarding, never serving a list that outlives the flight. *If this changed*, the 6-hour figure is a single tunable constant — it can be shortened to reduce wasted precompute or lengthened to widen coverage, without structural changes.

**User preference embeddings are produced by the user-service.** When a user sets or updates their preferences, that service embeds them — using the same versioned model the keywords-embedding-service uses for outlets, so user and outlet vectors share one space — and stores the vector on the user record. The recommendation service's retrieve step reads this precomputed vector; it does not embed at request time, keeping embedding off the latency-sensitive path. 

---


## 3. Flow

The recommendation service is triggered in **three ways**, all of which converge on the same four-step pipeline.

### Triggers

1. **Hourly scheduler (batch push).** Every hour the service fetches all users with a flight booking in the next 6 hours, generates recommendations, caches them, and pushes them to the user's phone.
2. **`GET /users/{userId}/recommendations` (on-demand pull).** Fired when a user opens the app or website. The service takes the user's preferences and current flight context and returns recommendations for display within the app.
3. **Live sales feed (event-driven update).** The service subscribes to a live sales feed. When a sale event arrives, it finds which users have that outlet in their cached recommendation list, overwrites the affected cached lists with the sale reflected, and pushes the update to those users.

### The four-step pipeline

Every trigger runs the same core process inside the recommendation service:

1. **Retrieve** — semantic search against the Vector DB using the user's preference embedding returns the top-K candidate outlets (recall-oriented; approximate and fast). This is candidate generation, not the final answer.
2. **Filter** — apply hard feasibility constraints using outlet metadata (from `airport-outlets-service`) and flight context (from `user-service`): is the outlet open, reachable before the gate cutoff, and does its dwell time fit the remaining window? This step is the **last I/O boundary** — outlet metadata and flight context are fetched here and held in memory for the remaining steps, so rerank does not make further service calls.
3. **Rerank** — score the surviving candidates on the in-memory set using preference match, live-sale boost, time-fit, and proximity to the gate. This is where the volatile, business-critical signals (flight window, live sales) are applied — precision on a small candidate set.
4. **Push / respond** — return the ranked, sequenced itinerary as an API response, or push it as a notification, depending on the trigger.

### Ingestion (offline, keyword → embedding)

Separately and asynchronously, a `keywords-embedding-service` runs on its own hourly schedule: it fetches outlet metadata from `airport-outlets-service`, obtains an embedding model (cached from Hugging Face), creates embeddings from each outlet's brand keywords, and writes them to the Vector DB as tag embeddings. This decouples the expensive embedding work from the request path — the retrieve step just does a nearest-neighbour lookup.

---

## 4. Technologies 

**Vector DB (pgvector) for outlet matching.** Brand keywords are free-text and non-standardised, so exact matching fails ("coffee" vs "espresso bar"). Embedding the keywords and doing approximate nearest-neighbour search captures semantic similarity. pgvector is chosen so vector search sits alongside familiar relational storage rather than introducing a separate purpose-built vector store — appropriate at hundreds of shops per airport, where the index is small and query cost is low.

**Hugging Face embedding model, cached.** The embedding model is pulled from Hugging Face and cached by the `keywords-embedding-service` rather than called per request. Embedding generation happens offline on a schedule, keeping it off the latency-sensitive read path.

**Redis for the hot read path.** The ≤ 500 ms read target is met by serving precomputed recommendations from an in-memory cache. Redis holds the serving copy of each user's recommendation list.

- **Key**: `rec:{userId}:{airport}` — the lookup is always by user (and airport), so the lookup dimensions form the key.
- **Value**: a serialised JSON payload of the ranked, sequenced list (recommendationId, airport, generatedAt, boardingTime, and the store list with scores/sale flags). A single `GET` returns everything needed to serve — one round-trip, no field-level access required.
- **TTL**: `min(6 hr, time-to-boarding)` — the cache never outlives the flight it was computed for, so a traveller boarding in 45 minutes is never served a stale 6-hour list.

**PostgreSQL for the durable record and the sales reverse-index.** The `user-recommendation` table persists recommendations (userId, recommendationId, recommendationTime, airport, store) and — critically — is queryable **by store**. This is what enables the sales fan-out: when a sale event names an outlet, postgres answers "which users have this store in their recommendation list?" so the service can overwrite and push only the affected users. This is the reason the system keeps two recommendation stores: **redis is keyed by user for fast reads; postgres is queryable by store for sale-driven updates.** They have different shapes because they serve different access patterns.

**Kafka for the live sales feed.** Sales are time-sensitive events produced independently by outlets. A durable, subscribable event stream fits event-driven fan-out better than polling, and decouples sale producers from the recommendation consumer.

**API Gateway (auth + load balancing) fronting a `user-auth-service`.** Standard edge concerns — authentication and load balancing — are handled at the gateway so downstream services stay focused on domain logic.

**Python recommendation and embedding services.** The recommendation and embedding logic sits naturally in the Python ML/embedding ecosystem.

---

## 5. Design tradeoffs

**Availability over consistency (by requirement).** An outlet missing from a list, or a briefly stale ranking, is acceptable; the system favours serving *something* fast over blocking for perfectly current data. The cached-then-refreshed model is a direct expression of this choice.

**Cache freshness vs. read latency.** Serving from redis meets the latency target but risks staleness within the TTL window. The mitigations are the `min(6 hr, time-to-boarding)` TTL and the sale-driven overwrite (a sale recomputes and overwrites the affected cached list, then pushes), which keep the cached list and the notification consistent rather than letting a sale leave a stale list behind.

**Two recommendation stores (redis + postgres).** Maintaining both adds operational surface and a consistency concern between them, but each serves an access pattern the other cannot: user-keyed fast reads vs. store-queryable sales fan-out. The duplication is deliberate and justified by the two distinct query shapes.

**Sales fan-out cost at scale.** Reacting to a sale by finding and updating all affected users is a fan-out per event. At a terminal-wide holiday sale this can be large. The intended mitigations are batching the recomputes, rate-limiting the push queue, and deduplicating so a user whose list contains several now-discounted outlets receives one notification rather than many. This is the main scaling pressure point to watch as user counts grow.

**pgvector vs. a purpose-built vector store.** pgvector keeps the stack simple and co-locates vector and relational data, which suits hundreds of shops per airport. If per-airport catalogue size or query volume grew substantially, a dedicated vector store with more advanced indexing might be warranted — deferred until the scale justifies the added complexity.
