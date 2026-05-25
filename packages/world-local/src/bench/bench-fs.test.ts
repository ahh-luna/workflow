/**
 * Filesystem primitive benchmarks.
 *
 * Tests raw read/write throughput of the core filesystem operations
 * that underpin all storage: writeJSON, readJSON, write (atomic rename),
 * the in-memory createdFilesCache, directory listing, and paginated queries.
 */
import { promises as fs } from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import {
  clearCreatedFilesCache,
  readJSON,
  write,
  writeJSON,
  listJSONFiles,
  paginatedFileSystemQuery,
} from '../fs.js';
import { z } from 'zod';
import { monotonicFactory } from 'ulid';
import { bench, benchConcurrent, printResults, printSection } from './utils.js';

const ulid = monotonicFactory();

describe('Filesystem Primitive Benchmarks', () => {
  let dataDir: string;

  beforeEach(async () => {
    dataDir = await fs.mkdtemp(path.join(os.tmpdir(), 'bench-fs-'));
    clearCreatedFilesCache();
  });

  afterEach(async () => {
    await fs.rm(dataDir, { recursive: true, force: true });
  });

  it('writeJSON — sequential single-file writes', async () => {
    const results = [];
    const N = 500;

    results.push(
      await bench(
        'writeJSON (small object, 200B)',
        async (i) => {
          const filePath = path.join(dataDir, `small-${i}.json`);
          await writeJSON(
            filePath,
            { id: `item-${i}`, status: 'running', ts: Date.now() },
            { overwrite: true }
          );
        },
        N
      )
    );

    results.push(
      await bench(
        'writeJSON (medium object, ~2KB)',
        async (i) => {
          const filePath = path.join(dataDir, `med-${i}.json`);
          await writeJSON(
            filePath,
            {
              runId: `wrun_${ulid()}`,
              deploymentId: 'dpl_local@1.0.0',
              status: 'running',
              workflowName: 'bench-workflow',
              input: { data: 'x'.repeat(1500) },
              output: undefined,
              error: undefined,
              createdAt: new Date(),
              updatedAt: new Date(),
              startedAt: new Date(),
              completedAt: undefined,
              specVersion: 2,
            },
            { overwrite: true }
          );
        },
        N
      )
    );

    results.push(
      await bench(
        'writeJSON (large object, ~50KB)',
        async (i) => {
          const filePath = path.join(dataDir, `large-${i}.json`);
          await writeJSON(
            filePath,
            {
              runId: `wrun_${ulid()}`,
              data: Array.from({ length: 100 }, (_, j) => ({
                key: `item-${j}`,
                value: 'x'.repeat(400),
              })),
            },
            { overwrite: true }
          );
        },
        N
      )
    );

    printSection('writeJSON — Sequential Writes');
    printResults(results);
    expect(results[0].opsPerSec).toBeGreaterThan(100); // Sanity: at least 100 ops/s
  });

  it('write (raw) — atomic rename pattern', async () => {
    const results = [];
    const N = 500;

    results.push(
      await bench(
        'write (1KB buffer, atomic rename)',
        async (i) => {
          const filePath = path.join(dataDir, `raw-${i}.bin`);
          await write(filePath, Buffer.alloc(1024, 0x42), { overwrite: true });
        },
        N
      )
    );

    results.push(
      await bench(
        'write (64KB buffer, atomic rename)',
        async (i) => {
          const filePath = path.join(dataDir, `raw64-${i}.bin`);
          await write(filePath, Buffer.alloc(64 * 1024, 0x42), {
            overwrite: true,
          });
        },
        N
      )
    );

    printSection('write — Atomic Rename Pattern');
    printResults(results);
  });

  it('readJSON — sequential reads', async () => {
    const Schema = z.object({
      id: z.string(),
      status: z.string(),
      ts: z.number(),
    });

    // Seed files
    const N = 500;
    for (let i = 0; i < N; i++) {
      await writeJSON(
        path.join(dataDir, `read-${i}.json`),
        { id: `item-${i}`, status: 'completed', ts: Date.now() },
        { overwrite: true }
      );
    }

    clearCreatedFilesCache();

    const results = [];

    results.push(
      await bench(
        'readJSON (small, with zod parse)',
        async (i) => {
          const filePath = path.join(dataDir, `read-${i % N}.json`);
          const data = await readJSON(filePath, Schema);
          if (!data) throw new Error('unexpected null');
        },
        N
      )
    );

    results.push(
      await bench(
        'readJSON (miss — ENOENT)',
        async (i) => {
          const filePath = path.join(dataDir, `nonexistent-${i}.json`);
          await readJSON(filePath, Schema);
        },
        N
      )
    );

    printSection('readJSON — Sequential Reads');
    printResults(results);
  });

  it('listJSONFiles — directory listing at scale', async () => {
    const results = [];

    // Seed directories with different file counts
    for (const count of [100, 500, 2000]) {
      const dir = path.join(dataDir, `list-${count}`);
      await fs.mkdir(dir, { recursive: true });
      for (let i = 0; i < count; i++) {
        await fs.writeFile(path.join(dir, `item-${i}.json`), '{}');
      }

      results.push(
        await bench(
          `listJSONFiles (${count} files)`,
          async () => {
            const files = await listJSONFiles(dir);
            if (files.length !== count)
              throw new Error(`Expected ${count}, got ${files.length}`);
          },
          100
        )
      );
    }

    printSection('listJSONFiles — Directory Listing');
    printResults(results);
  });

  it('paginatedFileSystemQuery — pagination throughput', async () => {
    const ItemSchema = z.object({
      id: z.string(),
      status: z.string(),
      createdAt: z.coerce.date(),
    });

    // Seed 500 files
    const dir = path.join(dataDir, 'paginated');
    await fs.mkdir(dir, { recursive: true });
    for (let i = 0; i < 500; i++) {
      const id = ulid();
      await writeJSON(
        path.join(dir, `evnt_${id}.json`),
        { id: `evnt_${id}`, status: 'completed', createdAt: new Date() },
        { overwrite: true }
      );
    }
    clearCreatedFilesCache();

    const results = [];

    results.push(
      await bench(
        'paginatedQuery (limit=20, 500 files)',
        async () => {
          await paginatedFileSystemQuery({
            directory: dir,
            schema: ItemSchema,
            limit: 20,
            getCreatedAt: (filename: string) => {
              const id = filename.replace(/\.json$/, '').replace(/^evnt_/, '');
              try {
                const ts = parseInt(id.substring(0, 10), 36);
                return new Date(ts);
              } catch {
                return null;
              }
            },
            getId: (item) => item.id,
          });
        },
        100
      )
    );

    results.push(
      await bench(
        'paginatedQuery (limit=100, 500 files)',
        async () => {
          await paginatedFileSystemQuery({
            directory: dir,
            schema: ItemSchema,
            limit: 100,
            getCreatedAt: (filename: string) => {
              const id = filename.replace(/\.json$/, '').replace(/^evnt_/, '');
              try {
                const ts = parseInt(id.substring(0, 10), 36);
                return new Date(ts);
              } catch {
                return null;
              }
            },
            getId: (item) => item.id,
          });
        },
        50
      )
    );

    printSection('paginatedFileSystemQuery — Pagination');
    printResults(results);
  });

  it('createdFilesCache — conflict detection perf', async () => {
    const results = [];
    const N = 1000;

    // Seed cache: write N files so the cache knows they exist
    for (let i = 0; i < N; i++) {
      await writeJSON(
        path.join(dataDir, `cached-${i}.json`),
        { i },
        { overwrite: true }
      );
    }

    // Now attempt writes WITHOUT overwrite — should hit cache and throw fast
    results.push(
      await bench(
        'conflict detection (cache hit, no disk I/O)',
        async (i) => {
          try {
            await writeJSON(path.join(dataDir, `cached-${i % N}.json`), { i });
          } catch {
            // Expected: EntityConflictError
          }
        },
        N
      )
    );

    printSection('createdFilesCache — Conflict Detection');
    printResults(results);
    // Cache hits should be sub-millisecond
    expect(results[0].p50Ms).toBeLessThan(1);
  });

  it('concurrent writes — parallel file creation', async () => {
    const results = [];

    for (const concurrency of [5, 20, 50]) {
      clearCreatedFilesCache();
      const subDir = path.join(dataDir, `conc-${concurrency}`);

      results.push(
        await benchConcurrent(
          `concurrent writeJSON (c=${concurrency}, 200 ops)`,
          async (i) => {
            await writeJSON(
              path.join(subDir, `item-${i}.json`),
              { id: i, data: 'x'.repeat(200) },
              { overwrite: true }
            );
          },
          200,
          concurrency
        )
      );
    }

    printSection('Concurrent Writes');
    printResults(results);
  });
});
