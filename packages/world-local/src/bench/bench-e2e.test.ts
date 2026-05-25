/**
 * End-to-end LocalWorld benchmarks.
 *
 * Tests the fully-assembled createLocalWorld() instance including storage,
 * queue (with direct handlers), and streamer working together — the same
 * code path a real workflow execution would take.
 */
import { promises as fs } from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { createLocalWorld, type LocalWorld } from '../index.js';
import { clearCreatedFilesCache } from '../fs.js';
import { bench, benchConcurrent, printResults, printSection } from './utils.js';
import {
  createRun,
  createStep,
  updateRun,
  updateStep,
  createHook,
  createWait,
  completeWait,
} from '../test-helpers.js';

describe('End-to-End LocalWorld Benchmarks', () => {
  let dataDir: string;
  let world: LocalWorld;

  beforeEach(async () => {
    dataDir = await fs.mkdtemp(path.join(os.tmpdir(), 'bench-e2e-'));
    world = createLocalWorld({
      dataDir,
      baseUrl: 'http://localhost:9999', // won't be used
      recoverActiveRuns: false,
    });
    await world.start();
  });

  afterEach(async () => {
    await world.close();
    await fs.rm(dataDir, { recursive: true, force: true });
  });

  it('world.start() + world.clear() cycle', async () => {
    const results = [];

    results.push(
      await bench(
        'world.clear() (empty world)',
        async () => {
          await world.clear();
        },
        50
      )
    );

    // Seed some data then clear
    for (let i = 0; i < 20; i++) {
      const run = await createRun(world, {
        deploymentId: 'dpl_local@1.0.0',
        workflowName: `clear-wf-${i}`,
        input: { i },
      });
      await updateRun(world, run.runId, 'run_started');
      await createStep(world, run.runId, {
        stepId: `step_0`,
        stepName: 'test-step',
        input: { i },
      });
    }

    results.push(
      await bench(
        'world.clear() (20 runs + steps)',
        async () => {
          await world.clear();
          // Re-seed for next iteration
          for (let i = 0; i < 20; i++) {
            const run = await createRun(world, {
              deploymentId: 'dpl_local@1.0.0',
              workflowName: `clear-wf-${i}`,
              input: { i },
            });
          }
        },
        10
      )
    );

    printSection('world.clear()');
    printResults(results);
  });

  it('full workflow execution through world interface', async () => {
    const results = [];
    const N = 30;

    results.push(
      await bench(
        `complete workflow execution (5 steps, via world)`,
        async (i) => {
          // Create & start run
          const run = await createRun(world, {
            deploymentId: 'dpl_local@1.0.0',
            workflowName: 'e2e-workflow',
            input: { userId: `user-${i}`, action: 'process' },
          });
          await updateRun(world, run.runId, 'run_started');

          // Execute steps
          for (let s = 0; s < 5; s++) {
            const stepId = `step_${s}`;
            await createStep(world, run.runId, {
              stepId,
              stepName: `step-${s}`,
              input: { stepData: s },
            });
            await updateStep(world, run.runId, stepId, 'step_started');
            await updateStep(world, run.runId, stepId, 'step_completed', {
              result: { output: s * 10 },
            });
          }

          // Complete run
          await updateRun(world, run.runId, 'run_completed', {
            output: { finalResult: 'done' },
          });
        },
        N
      )
    );

    printSection(`E2E Workflow (${N} runs × 5 steps = ${N * 17} events)`);
    printResults(results);
    console.log(`  → Workflows/sec: ${results[0].opsPerSec.toFixed(1)}`);
    console.log(`  → Events/sec: ${(results[0].opsPerSec * 17).toFixed(1)}`);
  });

  it('workflow with hooks and waits', async () => {
    const results = [];
    const N = 20;

    let hookWaitCounter = 0;
    results.push(
      await bench(
        'workflow with hook + wait (per run)',
        async () => {
          const id = hookWaitCounter++;
          const run = await createRun(world, {
            deploymentId: 'dpl_local@1.0.0',
            workflowName: 'hook-wait-workflow',
            input: { id },
          });
          await updateRun(world, run.runId, 'run_started');

          // Create a step
          await createStep(world, run.runId, {
            stepId: 'step_0',
            stepName: 'fetch-data',
            input: { id },
          });
          await updateStep(world, run.runId, 'step_0', 'step_started');

          // Create a hook (webhook callback)
          await createHook(world, run.runId, {
            hookId: `hook_${String(id).padStart(6, '0')}`,
            token: `tok_bench_${id}_${Date.now()}_${Math.random()}`,
            metadata: { source: 'test' },
          });

          // Create a wait (sleep)
          await createWait(world, run.runId, {
            waitId: `wait_${String(id).padStart(6, '0')}`,
            resumeAt: new Date(Date.now() + 60000),
          });

          // Complete step
          await updateStep(world, run.runId, 'step_0', 'step_completed', {
            result: { ok: true },
          });

          // Complete run
          await updateRun(world, run.runId, 'run_completed', {
            output: { result: 'done' },
          });
        },
        N
      )
    );

    printSection('Workflow with Hooks + Waits');
    printResults(results);
  });

  it('workflow + streaming data', async () => {
    const results = [];
    const N = 20;

    results.push(
      await bench(
        'workflow with 50 stream chunks per run',
        async (i) => {
          const run = await createRun(world, {
            deploymentId: 'dpl_local@1.0.0',
            workflowName: 'streaming-workflow',
            input: { i },
          });
          await updateRun(world, run.runId, 'run_started');

          // Write stream chunks (simulating AI streaming output)
          const streamName = `output-${run.runId}`;
          for (let c = 0; c < 50; c++) {
            await world.streams.write(
              run.runId,
              streamName,
              `token ${c}: ${'word '.repeat(5)}`
            );
          }
          await world.streams.close(run.runId, streamName);

          // Read back the stream
          const info = await world.streams.getInfo(run.runId, streamName);
          const chunks = await world.streams.getChunks(run.runId, streamName, {
            limit: 100,
          });

          await updateRun(world, run.runId, 'run_completed', {
            output: { chunks: chunks.data.length },
          });
        },
        N
      )
    );

    printSection('Workflow + Streaming (50 chunks/run)');
    printResults(results);
  });

  it('concurrent workflow execution', async () => {
    const results = [];

    for (const concurrency of [5, 10, 25]) {
      clearCreatedFilesCache();

      results.push(
        await benchConcurrent(
          `concurrent workflows (c=${concurrency}, 3 steps each)`,
          async (i) => {
            const run = await createRun(world, {
              deploymentId: 'dpl_local@1.0.0',
              workflowName: 'conc-e2e',
              input: { i },
            });
            await updateRun(world, run.runId, 'run_started');

            for (let s = 0; s < 3; s++) {
              const stepId = `step_${s}`;
              await createStep(world, run.runId, {
                stepId,
                stepName: `step-${s}`,
                input: { s },
              });
              await updateStep(world, run.runId, stepId, 'step_started');
              await updateStep(world, run.runId, stepId, 'step_completed', {
                result: { v: s },
              });
            }

            await updateRun(world, run.runId, 'run_completed', {
              output: { done: true },
            });
          },
          50,
          concurrency
        )
      );
    }

    printSection('Concurrent Workflow Execution');
    printResults(results);
    console.log(
      '  → Note: step-level mutex serializes per-step events; higher c benefits new runs'
    );
  });

  it('read-heavy mixed workload (80% reads, 20% writes)', async () => {
    // Seed data
    const runIds: string[] = [];
    for (let i = 0; i < 50; i++) {
      const run = await createRun(world, {
        deploymentId: 'dpl_local@1.0.0',
        workflowName: `read-heavy-wf-${i % 5}`,
        input: { i },
      });
      await updateRun(world, run.runId, 'run_started');
      await createStep(world, run.runId, {
        stepId: 'step_0',
        stepName: 'initial-step',
        input: { i },
      });
      await updateStep(world, run.runId, 'step_0', 'step_started');
      await updateStep(world, run.runId, 'step_0', 'step_completed', {
        result: { v: i },
      });
      await updateRun(world, run.runId, 'run_completed', {
        output: { result: i },
      });
      runIds.push(run.runId);
    }

    const results = [];
    const N = 500;

    results.push(
      await bench(
        'read-heavy workload (80% reads, 20% writes)',
        async (i) => {
          if (i % 5 === 0) {
            // 20%: create a new run
            await createRun(world, {
              deploymentId: 'dpl_local@1.0.0',
              workflowName: 'read-heavy-new',
              input: { i },
            });
          } else if (i % 5 === 1) {
            // 20%: list runs
            await world.runs.list({ pagination: { limit: 10 } });
          } else if (i % 5 === 2) {
            // 20%: get specific run
            await world.runs.get(runIds[i % runIds.length]);
          } else if (i % 5 === 3) {
            // 20%: list steps
            await world.steps.list({
              runId: runIds[i % runIds.length],
              pagination: { limit: 10 },
            });
          } else {
            // 20%: list events
            await world.events.list({
              runId: runIds[i % runIds.length],
              pagination: { limit: 10 },
            });
          }
        },
        N
      )
    );

    printSection('Read-Heavy Mixed Workload');
    printResults(results);
  });

  // ─── Tagged world isolation ─────────────────────────────────────────

  it('tagged world — isolation overhead', async () => {
    // Create a tagged world
    const taggedWorld = createLocalWorld({
      dataDir,
      baseUrl: 'http://localhost:9999',
      recoverActiveRuns: false,
      tag: 'bench-tag',
    });
    await taggedWorld.start();

    const results = [];
    const N = 50;

    results.push(
      await bench(
        'tagged: create + complete workflow',
        async (i) => {
          const run = await createRun(taggedWorld, {
            deploymentId: 'dpl_local@1.0.0',
            workflowName: 'tagged-wf',
            input: { i },
          });
          await updateRun(taggedWorld, run.runId, 'run_started');
          await createStep(taggedWorld, run.runId, {
            stepId: 'step_0',
            stepName: 'step',
            input: {},
          });
          await updateStep(taggedWorld, run.runId, 'step_0', 'step_started');
          await updateStep(taggedWorld, run.runId, 'step_0', 'step_completed', {
            result: {},
          });
          await updateRun(taggedWorld, run.runId, 'run_completed', {
            output: {},
          });
        },
        N
      )
    );

    // Compare with untagged
    results.push(
      await bench(
        'untagged: create + complete workflow',
        async (i) => {
          const run = await createRun(world, {
            deploymentId: 'dpl_local@1.0.0',
            workflowName: 'untagged-wf',
            input: { i },
          });
          await updateRun(world, run.runId, 'run_started');
          await createStep(world, run.runId, {
            stepId: 'step_0',
            stepName: 'step',
            input: {},
          });
          await updateStep(world, run.runId, 'step_0', 'step_started');
          await updateStep(world, run.runId, 'step_0', 'step_completed', {
            result: {},
          });
          await updateRun(world, run.runId, 'run_completed', {
            output: {},
          });
        },
        N
      )
    );

    results.push(
      await bench(
        'tagged: world.clear()',
        async () => {
          // Seed some data
          const run = await createRun(taggedWorld, {
            deploymentId: 'dpl_local@1.0.0',
            workflowName: 'clear-test',
            input: {},
          });
          await taggedWorld.clear();
        },
        20
      )
    );

    await taggedWorld.close();

    printSection('Tagged vs Untagged World');
    printResults(results);
  });
});
