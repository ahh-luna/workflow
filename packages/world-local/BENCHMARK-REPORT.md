# @workflow/world-local — Performance Benchmark Report

**Date:** 2025-07-18  
**Package:** `@workflow/world-local@5.0.0-beta.6`  
**Runtime:** Node.js 22 / vitest 4.0.18  
**Environment:** Linux container (Docker), tmpfs-backed temp directories  
**Benchmark Suite:** 37 tests across 5 files, ~85s total execution

---

## Executive Summary

The `world-local` package delivers **~30-35 complete workflow executions per second** (5 steps each, ~570 events/sec) for sequential workloads. Point reads are fast (~200-500µs), but **list/pagination queries are the primary bottleneck** at 25-50ms per page due to the readdir→read→parse→filter pipeline.

### Key Findings

| Metric | Value | Assessment |
|---|---|---|
| Workflow throughput (5 steps) | **~34 workflows/sec** | ✅ Good for local dev |
| Event creation throughput | **~570 events/sec** | ✅ Good |
| Single run read (`runs.get`) | **~240µs (p50)** | ✅ Excellent |
| Run list (200 runs, limit=20) | **~43ms (p50)** | ⚠️ Slow at scale |
| Events list (152 events) | **~34ms (p50)** | ⚠️ Slow at scale |
| Paginated query (500 files) | **~157ms (p50)** | 🔴 Major bottleneck |
| Concurrent workflows (c=25) | **249 ops/sec aggregate** | ✅ Scales well |
| Queue dispatch (direct) | **~18,000 ops/sec** | ✅ Excellent |
| Stream chunk write | **~2,100 ops/sec** (sequential, same stream) | ✅ Good |
| Tagged world overhead | **~24% slower** than untagged | ⚠️ Measurable |

---

## 1. Filesystem Primitives

These are the foundation of all storage operations.

### Writes

| Operation | ops/sec | p50 | p95 | p99 | max |
|---|---|---|---|---|---|
| writeJSON (200B object) | 1,129 | 388µs | 3.08ms | 6.71ms | 10.38ms |
| writeJSON (~2KB object) | 1,215 | 575µs | 1.90ms | 3.75ms | 10.37ms |
| writeJSON (~50KB object) | 536 | 1.31ms | 4.47ms | 9.35ms | 48.92ms |
| write (1KB raw buffer) | 1,285 | 477µs | 2.02ms | 4.99ms | 7.77ms |
| write (64KB raw buffer) | 410 | 1.62ms | 7.46ms | 11.76ms | 27.08ms |

**Observation:** The atomic write pattern (write temp → rename) adds ~200-400µs overhead compared to direct writes. JSON serialization cost grows linearly with payload size. The p95-p99 tail is 3-10x the median, likely from `ensureDir` mkdir checks and filesystem sync.

### Reads

| Operation | ops/sec | p50 | p95 | p99 |
|---|---|---|---|---|
| readJSON (small + Zod parse) | 821 | 822µs | 3.49ms | 7.73ms |
| readJSON (ENOENT miss) | 3,792 | 171µs | 787µs | 1.49ms |

**Observation:** Zod parsing adds significant overhead to reads. An ENOENT miss is ~5x faster than a successful read because it skips JSON.parse + Zod validation entirely.

### Directory Listing

| Operation | ops/sec | p50 | p95 |
|---|---|---|---|
| listJSONFiles (100 files) | 5,934 | 138µs | 346µs |
| listJSONFiles (500 files) | 3,278 | 234µs | 609µs |
| listJSONFiles (2,000 files) | 775 | 838µs | 4.47ms |

**Observation:** `readdir` scales linearly with file count. At 2,000 files it takes ~1ms just for the directory listing, before any file reads.

### Paginated Queries (🔴 BOTTLENECK)

| Operation | ops/sec | p50 | p95 | p99 |
|---|---|---|---|---|
| paginatedQuery (limit=20, 500 files) | 5 | 157ms | 524ms | 945ms |
| paginatedQuery (limit=100, 500 files) | 8 | 112ms | 188ms | 264ms |

**Root cause:** `paginatedFileSystemQuery` reads *all files*, parses *every one* through Zod, sorts them, then filters to the requested page. For 500 files, that's 500× (readFile + JSON.parse + Zod.parse). The ULID-based cursor optimization helps skip files by timestamp, but only after the initial readdir + sort. **This is the dominant cost in any `.list()` operation.**

### Conflict Detection Cache

| Operation | ops/sec | p50 |
|---|---|---|
| Cache hit (no disk I/O) | 136,638 | 6µs |

**Observation:** The `createdFilesCache` Set lookup is ~150x faster than hitting the filesystem. This cache is essential for performance — without it, every write would need an `fs.access()` call.

---

## 2. Storage Layer (Event-Sourced Pipeline)

### Run Operations

| Operation | ops/sec | p50 | p95 | p99 |
|---|---|---|---|---|
| run_created event | 1,062 | 731µs | 2.42ms | 3.25ms |
| Full lifecycle (create→start→complete) | 190 | 4.36ms | 12.73ms | 19.33ms |
| runs.get | 2,024 | 400µs | 881µs | 1.69ms |
| runs.list (200 runs, limit=20) | 22 | 42.58ms | 70.03ms | 77.57ms |
| runs.list (filter by name) | 18 | 51.02ms | 99.81ms | 143.70ms |
| runs.list (walk all pages) | 13 | 71.46ms | 262.86ms | - |

**Observation:** A single `run_created` event writes 2 files (run entity + event). Each lifecycle event also reads the current run, validates terminal state, then writes 2 files. The ~5ms for a full 3-event lifecycle is dominated by 6 writes + 2 reads.

### Step Operations

| Operation | ops/sec | p50 | p95 | p99 |
|---|---|---|---|---|
| step_created event | 744 | 1.27ms | 2.32ms | 2.84ms |
| Full lifecycle (create→start→complete) | 223 | 3.69ms | 9.13ms | 17.64ms |
| steps.get | 3,148 | 257µs | 667µs | 981µs |
| steps.list (100 steps, limit=20) | 36 | 24.12ms | 46.97ms | 61.25ms |

**Observation:** Step events are ~50% slower than run events because each step event acquires a per-step in-process mutex (`withStepLock`). The lock ensures terminal-state checks are atomic, but costs serialization overhead even for non-conflicting steps.

### Event Queries

| Operation | ops/sec | p50 | p95 |
|---|---|---|---|
| events.list (152 events, limit=20) | 28 | 33.96ms | 67.08ms |
| events.list (152 events, limit=100) | 29 | 32.66ms | 48.14ms |

**Observation:** `events.list` is slightly faster than `runs.list` because event files have ULID-based names that allow better cursor-based pre-filtering. Still, at ~34ms per page for 152 events, listing is expensive.

### Hooks & Waits

| Operation | ops/sec | p50 |
|---|---|---|
| hook_created event | 767 | 1.31ms |
| hooks.get | 4,705 | 198µs |
| hooks.list (100 hooks) | 31 | 26.60ms |
| wait_created event | 561 | 1.68ms |

**Observation:** Hook creation is in line with other entity creation (~1.3ms). Wait creation is slower (~1.7ms) because it involves additional file operations for the wait entity. Hook list is similarly bottlenecked by readdir+parse like other list operations.

---

## 3. Streamer (Chunk I/O)

### In-Memory Operations

| Operation | ops/sec | p50 |
|---|---|---|
| serializeChunk (35B) | 629,642 | <1µs |
| serializeChunk (1KB) | 342,531 | <1µs |
| serializeChunk (64KB) | 35,146 | 12µs |
| deserializeChunk (35B) | 2,130,947 | <1µs |
| deserializeChunk (1KB) | 2,036,159 | <1µs |
| deserializeChunk (64KB) | 65,102 | 4µs |

**Observation:** Chunk serialization is essentially just a Buffer.concat with a 1-byte header. Deserialization creates a copy (`Buffer.from(subarray(1))`). Both are sub-microsecond for small chunks. The 64KB serialize is slower (12µs) due to Buffer allocation.

### Disk-Backed Stream I/O

| Operation | ops/sec | p50 | p95 |
|---|---|---|---|
| stream.write (50B, new stream) | 1,101 | 887µs | 1.39ms |
| stream.write (1KB) | 826 | 1.11ms | 2.38ms |
| stream.write (sequential, same stream) | 2,164 | 451µs | 618µs |
| writeMulti (batch=5) | 759 | 1.35ms | 1.70ms |
| writeMulti (batch=20) | 366 | 2.73ms | 3.68ms |
| writeMulti (batch=50) | 198 | 4.62ms | 7.97ms |
| getChunks (limit=20, from start) | 217 | 4.24ms | 9.23ms |
| getChunks (limit=50, from start) | 98 | 10.02ms | 14.57ms |
| getChunks (cursor at idx 100) | 37 | 25.04ms | 48.34ms |
| getChunks (walk 200 chunks) | 8 | 122.28ms | 213.65ms |
| getInfo (100 chunks) | 43 | 21.80ms | 35.46ms |
| streams.list (20 streams) | 6,629 | 156µs | 197µs |

**Observation:** Sequential writes to the same stream are 2x faster than new-stream writes because `registerStreamForRun` is cached. `writeMulti` scales sub-linearly — 50 chunks in 4.6ms vs 50 × 0.45ms = 22.5ms theoretical, so parallel writes provide ~5x speedup. Reading is bottlenecked by the same readdir→read pattern: `getChunks` from a cursor still needs to scan all preceding files to check for EOF.

---

## 4. Queue

| Operation | ops/sec | p50 | p95 |
|---|---|---|---|
| queue dispatch (workflow, direct handler) | 18,060 | 36µs | 85µs |
| queue dispatch (step, direct handler) | 13,692 | 54µs | 193µs |
| queue dispatch (with idempotency key) | 16,237 | 47µs | 127µs |
| createQueueHandler (valid request) | 46,174 | 18µs | 31µs |
| createQueueHandler (invalid request) | 53,173 | 17µs | 27µs |
| handler with timeout response | 49,459 | 16µs | 41µs |
| getDeploymentId (cached) | 2,111,375 | <1µs | <1µs |

**Observation:** The queue layer is not a bottleneck. Direct handlers bypass HTTP entirely and process messages in ~36-54µs. The queue dispatch is fire-and-forget (returns immediately, processes async), so the measured latency is just serialization + Map lookup + semaphore check. `getDeploymentId` with caching is essentially free.

---

## 5. End-to-End Workflows

### Sequential Execution

| Scenario | ops/sec | p50 | Events/sec |
|---|---|---|---|
| 5-step workflow (create→complete) | 34 | 26.31ms | ~574 |
| Workflow with hook + wait | 93 | 10.45ms | - |
| Workflow with 50 stream chunks | 14 | 58.42ms | - |

### Concurrent Execution

| Concurrency | ops/sec | p50 (per-op) |
|---|---|---|
| c=5 (3-step workflows) | 91 | 59.02ms |
| c=10 (3-step workflows) | 150 | 61.78ms |
| c=25 (3-step workflows) | 249 | 100.76ms |

**Observation:** Throughput scales near-linearly up to c=25 (91→150→249 ops/sec) because each workflow writes to different run files. Per-operation latency increases at high concurrency due to filesystem contention and Node.js event loop saturation.

### Tagged vs Untagged

| Variant | ops/sec | p50 |
|---|---|---|
| Tagged workflow | 114 | 7.72ms |
| Untagged workflow | 142 | 6.85ms |

**Observation:** Tagged worlds are ~24% slower due to the extra `assertSafeEntityId` calls on the tag and the longer filenames that need to be matched during `readJSONWithFallback` (tagged path first, then untagged fallback).

---

## 6. Optimization Opportunities

### 🔴 Critical: `paginatedFileSystemQuery` (5-8 ops/sec on 500 files)

**Problem:** Every `.list()` call reads and parses *every file in the directory*, even with cursor-based pagination. For 200 runs, that's 200 × (readFile + JSON.parse + Zod.parse) = ~43ms.

**Potential optimizations:**
1. **SQLite index**: Maintain a lightweight SQLite index (runId, status, workflowName, createdAt) alongside JSON files. Queries hit the index; only fetch full JSON for the matching page.
2. **ULID-based cursor pruning**: The code already does some cursor-based filtering via `getCreatedAt(filename)`, but it still reads files that pass the filename filter. More aggressive pruning by filename sort could eliminate reads.
3. **In-memory cache**: Cache parsed file metadata in-memory (invalidated by write operations). Listing would be an O(n) filter over in-memory objects instead of O(n) disk reads.
4. **Lazy Zod parsing**: Use `z.lazy()` or a two-phase approach: parse only the cursor/sort fields first, then full-parse only the files in the result page.

### ⚠️ Moderate: Step Lock Contention

**Problem:** The `withStepLock` mutex serializes all events for the same step. Sequential step lifecycles pay ~1ms per lock acquisition. Under concurrent workloads with different steps, locks rarely contend, but the Map lookup + promise chain still adds overhead.

**Potential optimizations:**
1. **Batch lock acquisition**: For sequential step lifecycles (create→start→complete), acquire the lock once for the entire batch.
2. **Lock-free terminal check**: Use `writeExclusive` (O_CREAT|O_EXCL) as the atomicity primitive instead of an in-process lock. This already exists for some paths.

### ⚠️ Moderate: Zod Validation Overhead

**Problem:** Every `readJSON` call runs Zod validation on the parsed JSON. For known-good files (written by the same process), this is redundant.

**Potential optimizations:**
1. **Trust-local reads**: Skip Zod validation when reading files that were written by the current process (tracked via `createdFilesCache`).
2. **Schema caching**: Use `z.preprocess()` or compiled validators to reduce per-parse overhead.

### ℹ️ Minor: Stream getChunks cursor scan

**Problem:** `getChunks` with a cursor still scans all files before the cursor position to check for EOF markers. With 200 chunks and a cursor at index 100, it reads 100 files just to skip them.

**Potential optimizations:**
1. **Positional index file**: Maintain a manifest file that maps chunk indices to filenames, eliminating the need to scan preceding files.
2. **EOF marker in manifest**: Track whether a stream is closed in the run-streams mapping file, avoiding the need to check individual chunk EOF markers.

### ℹ️ Minor: Concurrent Write Throughput

**Problem:** At c=50, concurrent writes show increased tail latency (p50 jumps from ~1ms to ~37ms) due to `ensureDir` contention on the same parent directories.

**Potential optimizations:**
1. **Directory pre-creation**: Create all entity directories at `start()` time instead of lazily during writes.
2. **ensureDir caching**: Track which directories have been created (like `createdFilesCache` for files) to skip redundant `mkdir` calls.

---

## 7. Conclusions

`world-local` is **well-suited for local development** where individual workflow executions complete in ~30ms and the expected concurrent load is low (< 50 workflows). The architecture is clean, the queue layer is fast, and the filesystem-based approach keeps dependencies minimal.

For **testing at scale** (hundreds of runs, frequent list queries), the `paginatedFileSystemQuery` bottleneck becomes the dominant factor. A lightweight index (SQLite or in-memory) for the pagination path would yield 10-50x improvements in list operations without changing the JSON-on-disk storage model.

The **streaming subsystem** performs well for AI agent use cases (50 chunks at ~60ms total), though reading back large streams (~120ms for 200 chunks) could benefit from a positional index.
