/**
 * Storage layer benchmarks.
 *
 * Tests the full event-sourced storage pipeline: creating runs, steps, events,
 * hooks, and waits through the `events.create` pathway, then querying them.
 * This exercises the real Zod validation, filesystem I/O, and step-lock mutex.
 */
import { promises as fs } from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import type { Storage, WorkflowRun } from '@workflow/world';
import { SPEC_VERSION_CURRENT } from '@workflow/world';
import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { clearCreatedFilesCache } from '../fs.js';
import { createStorage } from '../storage.js';
import {
  createHook,
  createRun,
  createStep,
  createWait,
  updateRun,
  updateStep,
} from '../test-helpers.js';
import { bench, benchConcurrent, printResults, printSection } from './utils.js';

describe('Storage Layer Benchmarks', () => {
  let dataDir: string;
  let storage: Storage;

  beforeEach(async () => {
    dataDir = await fs.mkdtemp(path.join(os.tmpdir(), 'bench-storage-'));
    // Create subdirectories that the storage layer expects
    await fs.mkdir(path.join(dataDir, 'runs'), { recursive: true });
    await fs.mkdir(path.join(dataDir, 'steps'), { recursive: true });
    await fs.mkdir(path.join(dataDir, 'events'), { recursive: true });
    await fs.mkdir(path.join(dataDir, 'hooks'), { recursive: true });
    await fs.mkdir(path.join(dataDir, 'hooks', 'tokens'), { recursive: true });
    await fs.mkdir(path.join(dataDir, 'waits'), { recursive: true });
    await fs.mkdir(path.join(dataDir, '.locks'), { recursive: true });
    clearCreatedFilesCache();
    storage = createStorage(dataDir);
  });

  afterEach(async () => {
    await fs.rm(dataDir, { recursive: true, force: true });
  });

  // ─── Run lifecycle ──────────────────────────────────────────────────

  it('run creation throughput', async () => {
    const results = [];
    const N = 200;

    results.push(
      await bench(
        'events.create(run_created)',
        async (i) => {
          await createRun(storage, {
            deploymentId: 'dpl_local@1.0.0',
            workflowName: `bench-wf-${i}`,
            input: { iteration: i },
          });
        },
        N
      )
    );

    printSection('Run Creation');
    printResults(results);
    expect(results[0].opsPerSec).toBeGreaterThan(50);
  });

  it('run full lifecycle (create → start → complete)', async () => {
    const results = [];
    const N = 100;

    results.push(
      await bench(
        'full run lifecycle (3 events)',
        async (i) => {
          const run = await createRun(storage, {
            deploymentId: 'dpl_local@1.0.0',
            workflowName: `lifecycle-wf-${i}`,
            input: { i },
          });
          await updateRun(storage, run.runId, 'run_started');
          await updateRun(storage, run.runId, 'run_completed', {
            output: { result: 'done' },
          });
        },
        N
      )
    );

    printSection('Run Full Lifecycle');
    printResults(results);
  });

  it('runs.get — read throughput', async () => {
    // Seed runs
    const runIds: string[] = [];
    for (let i = 0; i < 100; i++) {
      const run = await createRun(storage, {
        deploymentId: 'dpl_local@1.0.0',
        workflowName: `read-wf-${i}`,
        input: { i },
      });
      runIds.push(run.runId);
    }

    const results = [];
    results.push(
      await bench(
        'runs.get (100 seeded runs)',
        async (i) => {
          await storage.runs.get(runIds[i % runIds.length]);
        },
        500
      )
    );

    printSection('runs.get — Read Throughput');
    printResults(results);
  });

  it('runs.list — pagination at scale', async () => {
    // Seed runs
    for (let i = 0; i < 200; i++) {
      const run = await createRun(storage, {
        deploymentId: 'dpl_local@1.0.0',
        workflowName: i < 100 ? 'workflow-a' : 'workflow-b',
        input: { i },
      });
      if (i % 3 === 0) {
        await updateRun(storage, run.runId, 'run_started');
        await updateRun(storage, run.runId, 'run_completed', {
          output: { done: true },
        });
      }
    }

    const results = [];

    results.push(
      await bench(
        'runs.list (no filter, limit=20, 200 runs)',
        async () => {
          await storage.runs.list({ pagination: { limit: 20 } });
        },
        50
      )
    );

    results.push(
      await bench(
        'runs.list (filter by name, limit=20, 200 runs)',
        async () => {
          await storage.runs.list({
            workflowName: 'workflow-a',
            pagination: { limit: 20 },
          });
        },
        50
      )
    );

    results.push(
      await bench(
        'runs.list (filter by status, limit=20, 200 runs)',
        async () => {
          await storage.runs.list({
            status: 'completed',
            pagination: { limit: 20 },
          });
        },
        50
      )
    );

    // Full pagination walk
    results.push(
      await bench(
        'runs.list (walk all pages, limit=20, 200 runs)',
        async () => {
          let cursor: string | undefined;
          let pages = 0;
          do {
            const result = await storage.runs.list({
              pagination: { limit: 20, cursor },
            });
            cursor = result.pagination?.cursor ?? undefined;
            pages++;
          } while (cursor && pages < 20);
        },
        10
      )
    );

    printSection('runs.list — Pagination');
    printResults(results);
  });

  // ─── Step lifecycle ─────────────────────────────────────────────────

  it('step creation + lifecycle throughput', async () => {
    // Create a run to host steps
    const run = await createRun(storage, {
      deploymentId: 'dpl_local@1.0.0',
      workflowName: 'step-bench',
      input: {},
    });
    await updateRun(storage, run.runId, 'run_started');

    const results = [];
    const N = 200;

    // Use a global counter to avoid ID collisions between warmup and measured iterations
    let stepCounter = 0;
    results.push(
      await bench(
        'step_created event',
        async () => {
          const id = stepCounter++;
          await createStep(storage, run.runId, {
            stepId: `step_${id}`,
            stepName: `bench-step-${id}`,
            input: { data: `payload-${id}` },
          });
        },
        N
      )
    );

    // Full step lifecycle
    const run2 = await createRun(storage, {
      deploymentId: 'dpl_local@1.0.0',
      workflowName: 'step-lifecycle',
      input: {},
    });
    await updateRun(storage, run2.runId, 'run_started');

    let lifecycleCounter = 0;
    results.push(
      await bench(
        'step full lifecycle (create → start → complete)',
        async () => {
          const id = lifecycleCounter++;
          const stepId = `step_lc${id}`;
          await createStep(storage, run2.runId, {
            stepId,
            stepName: `lifecycle-step-${id}`,
            input: { i: id },
          });
          await updateStep(storage, run2.runId, stepId, 'step_started');
          await updateStep(storage, run2.runId, stepId, 'step_completed', {
            result: { value: id * 2 },
          });
        },
        100
      )
    );

    printSection('Step Lifecycle');
    printResults(results);
  });

  it('steps.list — query throughput', async () => {
    const run = await createRun(storage, {
      deploymentId: 'dpl_local@1.0.0',
      workflowName: 'step-list-bench',
      input: {},
    });
    await updateRun(storage, run.runId, 'run_started');

    // Seed steps
    for (let i = 0; i < 100; i++) {
      await createStep(storage, run.runId, {
        stepId: `step_${i}`,
        stepName: `step-${i}`,
        input: { i },
      });
    }

    const results = [];

    results.push(
      await bench(
        'steps.list (100 steps, limit=20)',
        async () => {
          await storage.steps.list({
            runId: run.runId,
            pagination: { limit: 20 },
          });
        },
        50
      )
    );

    results.push(
      await bench(
        'steps.get (single step)',
        async (i) => {
          await storage.steps.get(run.runId, `step_${i % 100}`);
        },
        200
      )
    );

    printSection('steps.list/get — Query Throughput');
    printResults(results);
  });

  // ─── Events ─────────────────────────────────────────────────────────

  it('events.list — event log query at scale', async () => {
    const run = await createRun(storage, {
      deploymentId: 'dpl_local@1.0.0',
      workflowName: 'events-bench',
      input: {},
    });
    await updateRun(storage, run.runId, 'run_started');

    // Create many steps to generate lots of events
    for (let i = 0; i < 50; i++) {
      const stepId = `step_${i}`;
      await createStep(storage, run.runId, {
        stepId,
        stepName: `step-${i}`,
        input: { i },
      });
      await updateStep(storage, run.runId, stepId, 'step_started');
      await updateStep(storage, run.runId, stepId, 'step_completed', {
        result: { v: i },
      });
    }
    // That's ~2 (run) + 50*3 (steps) = 152 events

    const results = [];

    results.push(
      await bench(
        'events.list (152 events, limit=20)',
        async () => {
          await storage.events.list({
            runId: run.runId,
            pagination: { limit: 20 },
          });
        },
        50
      )
    );

    results.push(
      await bench(
        'events.list (152 events, limit=100)',
        async () => {
          await storage.events.list({
            runId: run.runId,
            pagination: { limit: 100 },
          });
        },
        30
      )
    );

    printSection('events.list — Event Log Query');
    printResults(results);
  });

  // ─── Hooks ──────────────────────────────────────────────────────────

  it('hook creation + lookup throughput', async () => {
    const run = await createRun(storage, {
      deploymentId: 'dpl_local@1.0.0',
      workflowName: 'hooks-bench',
      input: {},
    });
    await updateRun(storage, run.runId, 'run_started');

    const results = [];
    const hookIds: string[] = [];

    let hookCounter = 0;
    results.push(
      await bench(
        'hook_created event',
        async () => {
          const id = hookCounter++;
          const hookId = `hook_${String(id).padStart(6, '0')}`;
          hookIds.push(hookId);
          await createHook(storage, run.runId, {
            hookId,
            token: `tok_${hookId}_${Date.now()}_${Math.random()}`,
          });
        },
        100
      )
    );

    results.push(
      await bench(
        'hooks.get (by id)',
        async (i) => {
          await storage.hooks.get(hookIds[i % hookIds.length]);
        },
        200
      )
    );

    results.push(
      await bench(
        'hooks.list (100 hooks)',
        async () => {
          await storage.hooks.list({ runId: run.runId });
        },
        30
      )
    );

    printSection('Hooks — Creation & Lookup');
    printResults(results);
  });

  // ─── Waits ──────────────────────────────────────────────────────────

  it('wait creation throughput', async () => {
    const run = await createRun(storage, {
      deploymentId: 'dpl_local@1.0.0',
      workflowName: 'waits-bench',
      input: {},
    });
    await updateRun(storage, run.runId, 'run_started');

    const results = [];

    let waitCounter = 0;
    results.push(
      await bench(
        'wait_created event',
        async () => {
          const id = waitCounter++;
          await createWait(storage, run.runId, {
            waitId: `wait_${String(id).padStart(6, '0')}`,
            resumeAt: new Date(Date.now() + 60000),
          });
        },
        100
      )
    );

    printSection('Waits — Creation');
    printResults(results);
  });

  // ─── Concurrent operations ──────────────────────────────────────────

  it('concurrent run creation', async () => {
    const results = [];

    for (const concurrency of [5, 20, 50]) {
      clearCreatedFilesCache();

      results.push(
        await benchConcurrent(
          `concurrent run creation (c=${concurrency}, 100 ops)`,
          async (i) => {
            await createRun(storage, {
              deploymentId: 'dpl_local@1.0.0',
              workflowName: `conc-wf-${i}`,
              input: { i },
            });
          },
          100,
          concurrency
        )
      );
    }

    printSection('Concurrent Run Creation');
    printResults(results);
  });

  it('concurrent step lifecycle on same run', async () => {
    const run = await createRun(storage, {
      deploymentId: 'dpl_local@1.0.0',
      workflowName: 'conc-steps',
      input: {},
    });
    await updateRun(storage, run.runId, 'run_started');

    const results = [];

    // Each concurrency level gets its own run to avoid step state conflicts
    for (const concurrency of [5, 20]) {
      const concRun = await createRun(storage, {
        deploymentId: 'dpl_local@1.0.0',
        workflowName: `conc-steps-c${concurrency}`,
        input: {},
      });
      await updateRun(storage, concRun.runId, 'run_started');

      const totalSteps = 100;
      for (let i = 0; i < totalSteps; i++) {
        await createStep(storage, concRun.runId, {
          stepId: `step_${i}`,
          stepName: `conc-step-${i}`,
          input: { i },
        });
      }

      results.push(
        await benchConcurrent(
          `concurrent step start+complete (c=${concurrency}, ${totalSteps} steps)`,
          async (i) => {
            const stepId = `step_${i}`;
            await updateStep(storage, concRun.runId, stepId, 'step_started');
            await updateStep(storage, concRun.runId, stepId, 'step_completed', {
              result: { v: i },
            });
          },
          totalSteps,
          concurrency
        )
      );
    }

    printSection('Concurrent Step Lifecycle (same run)');
    printResults(results);
  });

  // ─── Mixed workload ─────────────────────────────────────────────────

  it('mixed workload — simulate real workflow execution', async () => {
    const results = [];
    const N = 30; // number of complete workflow executions

    results.push(
      await bench(
        `mixed workload (${N} workflows × 5 steps each)`,
        async (i) => {
          // 1. Create run
          const run = await createRun(storage, {
            deploymentId: 'dpl_local@1.0.0',
            workflowName: 'realistic-workflow',
            input: { userId: `user-${i}`, action: 'process' },
          });

          // 2. Start run
          await updateRun(storage, run.runId, 'run_started');

          // 3. Execute 5 steps sequentially
          for (let s = 0; s < 5; s++) {
            const stepId = `step_${s}`;
            await createStep(storage, run.runId, {
              stepId,
              stepName: `step-${s}`,
              input: { stepData: `data-${s}` },
            });
            await updateStep(storage, run.runId, stepId, 'step_started');
            await updateStep(storage, run.runId, stepId, 'step_completed', {
              result: { output: `result-${s}` },
            });
          }

          // 4. Complete run
          await updateRun(storage, run.runId, 'run_completed', {
            output: { finalResult: 'success' },
          });

          // 5. Read back the run
          await storage.runs.get(run.runId);
        },
        N
      )
    );

    // Calculate events per workflow: run_created + run_started + 5*(step_created + step_started + step_completed) + run_completed = 17
    const eventsPerWorkflow = 17;
    const totalEvents = N * eventsPerWorkflow;

    printSection(
      `Mixed Workload — ${N} Workflows × 5 Steps (${totalEvents} total events)`
    );
    printResults(results);

    // Report workflows/second
    console.log(`  → Workflows/sec: ${(results[0].opsPerSec).toFixed(1)}`);
    console.log(
      `  → Events/sec: ${(results[0].opsPerSec * eventsPerWorkflow).toFixed(1)}`
    );
  });
});
