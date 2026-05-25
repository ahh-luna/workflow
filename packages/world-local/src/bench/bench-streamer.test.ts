/**
 * Streamer benchmarks.
 *
 * Tests stream write, read, and chunk retrieval throughput for the
 * filesystem-backed streaming implementation.
 */
import { promises as fs } from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { clearCreatedFilesCache } from '../fs.js';
import {
  createStreamer,
  serializeChunk,
  deserializeChunk,
} from '../streamer.js';
import type { Streamer } from '@workflow/world';
import { bench, benchConcurrent, printResults, printSection } from './utils.js';

describe('Streamer Benchmarks', () => {
  let dataDir: string;
  let streamer: Streamer;

  beforeEach(async () => {
    dataDir = await fs.mkdtemp(path.join(os.tmpdir(), 'bench-stream-'));
    await fs.mkdir(path.join(dataDir, 'streams', 'runs'), { recursive: true });
    await fs.mkdir(path.join(dataDir, 'streams', 'chunks'), {
      recursive: true,
    });
    clearCreatedFilesCache();
    streamer = createStreamer(dataDir);
  });

  afterEach(async () => {
    await fs.rm(dataDir, { recursive: true, force: true });
  });

  // ─── Chunk serialization ────────────────────────────────────────────

  it('chunk serialization / deserialization (in-memory)', async () => {
    const results = [];
    const N = 10000;

    const smallChunk = Buffer.from('Hello, world! This is a small chunk.');
    const medChunk = Buffer.alloc(1024, 0x41); // 1KB
    const largeChunk = Buffer.alloc(64 * 1024, 0x42); // 64KB

    results.push(
      await bench(
        'serializeChunk (small, 35B)',
        async () => {
          serializeChunk({ chunk: smallChunk, eof: false });
        },
        N
      )
    );

    results.push(
      await bench(
        'serializeChunk (1KB)',
        async () => {
          serializeChunk({ chunk: medChunk, eof: false });
        },
        N
      )
    );

    results.push(
      await bench(
        'serializeChunk (64KB)',
        async () => {
          serializeChunk({ chunk: largeChunk, eof: false });
        },
        N
      )
    );

    const serializedSmall = serializeChunk({ chunk: smallChunk, eof: false });
    const serializedMed = serializeChunk({ chunk: medChunk, eof: false });
    const serializedLarge = serializeChunk({ chunk: largeChunk, eof: false });

    results.push(
      await bench(
        'deserializeChunk (small)',
        async () => {
          deserializeChunk(serializedSmall);
        },
        N
      )
    );

    results.push(
      await bench(
        'deserializeChunk (1KB)',
        async () => {
          deserializeChunk(serializedMed);
        },
        N
      )
    );

    results.push(
      await bench(
        'deserializeChunk (64KB)',
        async () => {
          deserializeChunk(serializedLarge);
        },
        N
      )
    );

    printSection('Chunk Serialization (In-Memory)');
    printResults(results);
    // In-memory ops should be very fast
    expect(results[0].p50Ms).toBeLessThan(1);
  });

  // ─── Stream writes ──────────────────────────────────────────────────

  it('stream.write — single chunk writes', async () => {
    const results = [];
    const N = 300;

    results.push(
      await bench(
        'streams.write (small string, ~50B)',
        async (i) => {
          await streamer.streams.write(
            `run-${i}`,
            `stream-small-${i}`,
            `chunk data ${i} - small payload`
          );
        },
        N
      )
    );

    results.push(
      await bench(
        'streams.write (1KB Uint8Array)',
        async (i) => {
          await streamer.streams.write(
            'run-write-med',
            `stream-med-${i}`,
            new Uint8Array(1024).fill(0x41)
          );
        },
        N
      )
    );

    results.push(
      await bench(
        'streams.write (same stream, sequential chunks)',
        async (i) => {
          await streamer.streams.write(
            'run-seq',
            'stream-sequential',
            `chunk-${i}: ${'x'.repeat(100)}`
          );
        },
        N
      )
    );

    printSection('Stream Write — Single Chunks');
    printResults(results);
  });

  it('streams.writeMulti — batched writes', async () => {
    const results = [];

    for (const batchSize of [5, 20, 50]) {
      const chunks = Array.from(
        { length: batchSize },
        (_, i) => `batch chunk ${i}: ${'y'.repeat(100)}`
      );

      results.push(
        await bench(
          `streams.writeMulti (batch=${batchSize}, ~100B chunks)`,
          async (i) => {
            await streamer.streams.writeMulti(
              'run-multi',
              `stream-multi-${batchSize}-${i}`,
              chunks
            );
          },
          50
        )
      );
    }

    printSection('Stream WriteMulti — Batched Writes');
    printResults(results);
  });

  // ─── Stream reads ───────────────────────────────────────────────────

  it('streams.getChunks — read throughput', async () => {
    // Seed a stream with chunks
    const runId = 'run-read-bench';
    const streamName = 'stream-read-bench';
    const chunkCount = 200;

    for (let i = 0; i < chunkCount; i++) {
      await streamer.streams.write(
        runId,
        streamName,
        `chunk ${i}: ${'z'.repeat(200)}`
      );
    }
    await streamer.streams.close(runId, streamName);

    const results = [];

    results.push(
      await bench(
        `getChunks (limit=20, ${chunkCount} chunks, from start)`,
        async () => {
          await streamer.streams.getChunks(runId, streamName, { limit: 20 });
        },
        100
      )
    );

    results.push(
      await bench(
        `getChunks (limit=50, ${chunkCount} chunks, from start)`,
        async () => {
          await streamer.streams.getChunks(runId, streamName, { limit: 50 });
        },
        50
      )
    );

    // Get chunks from the middle (using cursor)
    const firstPage = await streamer.streams.getChunks(runId, streamName, {
      limit: 100,
    });
    if (firstPage.cursor) {
      results.push(
        await bench(
          `getChunks (limit=20, from cursor at idx 100)`,
          async () => {
            await streamer.streams.getChunks(runId, streamName, {
              limit: 20,
              cursor: firstPage.cursor!,
            });
          },
          100
        )
      );
    }

    // Full stream read
    results.push(
      await bench(
        `getChunks (walk full stream, ${chunkCount} chunks)`,
        async () => {
          let cursor: string | undefined | null;
          let totalChunks = 0;
          do {
            const page = await streamer.streams.getChunks(runId, streamName, {
              limit: 50,
              cursor: cursor ?? undefined,
            });
            totalChunks += page.data.length;
            cursor = page.cursor;
          } while (cursor);
        },
        20
      )
    );

    printSection('Stream getChunks — Read Throughput');
    printResults(results);
  });

  it('streams.getInfo — metadata read', async () => {
    const runId = 'run-info-bench';
    const streamName = 'stream-info-bench';

    for (let i = 0; i < 100; i++) {
      await streamer.streams.write(runId, streamName, `chunk ${i}`);
    }
    await streamer.streams.close(runId, streamName);

    const results = [];

    results.push(
      await bench(
        'streams.getInfo (100 chunks, closed)',
        async () => {
          await streamer.streams.getInfo(runId, streamName);
        },
        50
      )
    );

    printSection('Stream getInfo');
    printResults(results);
  });

  it('streams.list — stream listing', async () => {
    const runId = 'run-list-bench';
    for (let i = 0; i < 20; i++) {
      await streamer.streams.write(runId, `stream-${i}`, `data-${i}`);
    }

    const results = [];

    results.push(
      await bench(
        'streams.list (20 streams)',
        async () => {
          const streams = await streamer.streams.list(runId);
          if (streams.length !== 20)
            throw new Error(`Expected 20, got ${streams.length}`);
        },
        100
      )
    );

    printSection('Stream List');
    printResults(results);
  });

  // ─── Concurrent stream writes ──────────────────────────────────────

  it('concurrent stream writes (same stream)', async () => {
    const results = [];

    for (const concurrency of [5, 20]) {
      results.push(
        await benchConcurrent(
          `concurrent stream write (c=${concurrency}, 200 chunks, same stream)`,
          async (i) => {
            await streamer.streams.write(
              'run-conc',
              `stream-conc-${concurrency}`,
              `concurrent chunk ${i}: ${'a'.repeat(100)}`
            );
          },
          200,
          concurrency
        )
      );
    }

    printSection('Concurrent Stream Writes');
    printResults(results);
  });
});
