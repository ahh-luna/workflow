/**
 * Queue benchmarks.
 *
 * Tests the local queue's in-process handler path (direct handler, bypassing HTTP),
 * message serialization, header parsing, and concurrency semaphore behavior.
 */
import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { createQueue, type DirectHandler } from '../queue.js';
import type { Config } from '../config.js';
import { bench, benchConcurrent, printResults, printSection } from './utils.js';

describe('Queue Benchmarks', () => {
  let cleanup: (() => Promise<void>) | null = null;

  afterEach(async () => {
    if (cleanup) {
      await cleanup();
      cleanup = null;
    }
  });

  it('queue message dispatch — direct handler', async () => {
    const config: Partial<Config> = {
      dataDir: '/tmp/bench-queue',
      baseUrl: 'http://localhost:9999', // won't be used with direct handler
    };
    const queue = createQueue(config);
    cleanup = () => queue.close();

    // Register a minimal direct handler that returns immediately
    let handlerCalls = 0;
    const handler: DirectHandler = async (req) => {
      handlerCalls++;
      // Consume the body to avoid leaks
      await req.text();
      return Response.json({ ok: true });
    };

    queue.registerHandler('__wkf_workflow_', handler);
    queue.registerHandler('__wkf_step_', handler);

    const results = [];
    const N = 500;

    // Queue workflow messages
    results.push(
      await bench(
        'queue (workflow message, direct handler)',
        async (i) => {
          await queue.queue(
            `__wkf_workflow_bench-${i}` as any,
            { runId: `wrun_${i}`, data: { test: true } },
            {}
          );
        },
        N
      )
    );

    // Queue step messages
    results.push(
      await bench(
        'queue (step message, direct handler)',
        async (i) => {
          await queue.queue(
            `__wkf_step_bench-${i}` as any,
            {
              workflowRunId: `wrun_${i}`,
              stepId: `step_${i}`,
              data: { test: true },
            },
            {}
          );
        },
        N
      )
    );

    // Queue with idempotency key (first send)
    results.push(
      await bench(
        'queue (with idempotency key, unique)',
        async (i) => {
          await queue.queue(
            `__wkf_workflow_idemp-${i}` as any,
            { runId: `wrun_idemp_${i}` },
            { idempotencyKey: `key-${i}-${Date.now()}` }
          );
        },
        N
      )
    );

    // Give async handlers time to complete
    await new Promise((resolve) => setTimeout(resolve, 500));

    printSection('Queue Dispatch — Direct Handler');
    printResults(results);
    expect(handlerCalls).toBeGreaterThan(0);
  });

  it('queue handler creation + request processing', async () => {
    const config: Partial<Config> = {
      dataDir: '/tmp/bench-queue-handler',
      baseUrl: 'http://localhost:9999',
    };
    const q = createQueue(config);
    cleanup = () => q.close();

    // Create a queue handler
    const queueHandler = q.createQueueHandler(
      '__wkf_workflow_',
      async (body, meta) => {
        // Simulate minimal processing
        return undefined;
      }
    );

    const results = [];
    const N = 500;

    // Simulate incoming requests to the handler
    results.push(
      await bench(
        'createQueueHandler processing (valid request)',
        async (i) => {
          const req = new Request('http://localhost/flow', {
            method: 'POST',
            headers: {
              'content-type': 'application/json',
              'x-vqs-queue-name': `__wkf_workflow_bench-${i}`,
              'x-vqs-message-id': `msg_${String(i).padStart(8, '0')}`,
              'x-vqs-message-attempt': '1',
            },
            body: JSON.stringify({ runId: `wrun_${i}`, data: { test: true } }),
          });
          const response = await queueHandler(req);
          if (!response.ok)
            throw new Error(`Handler returned ${response.status}`);
        },
        N
      )
    );

    // Invalid request (missing headers)
    results.push(
      await bench(
        'createQueueHandler processing (invalid — missing headers)',
        async () => {
          const req = new Request('http://localhost/flow', {
            method: 'POST',
            headers: { 'content-type': 'application/json' },
            body: JSON.stringify({}),
          });
          const response = await queueHandler(req);
          if (response.status !== 400)
            throw new Error(`Expected 400, got ${response.status}`);
        },
        N
      )
    );

    printSection('Queue Handler Processing');
    printResults(results);
  });

  it('queue handler with timeout response', async () => {
    const config: Partial<Config> = {
      dataDir: '/tmp/bench-queue-timeout',
      baseUrl: 'http://localhost:9999',
    };
    const q = createQueue(config);
    cleanup = () => q.close();

    // Handler that returns a timeout (simulating sleep/wait)
    let callCount = 0;
    const queueHandler = q.createQueueHandler(
      '__wkf_workflow_',
      async (body, meta) => {
        callCount++;
        if (callCount % 2 === 1) {
          // First call: return timeout
          return { timeoutSeconds: 0 }; // 0 = immediate re-delivery
        }
        // Second call: complete
        return undefined;
      }
    );

    const results = [];

    results.push(
      await bench(
        'handler with timeoutSeconds=0 response',
        async (i) => {
          callCount = 0; // Reset for each iteration
          const req = new Request('http://localhost/flow', {
            method: 'POST',
            headers: {
              'content-type': 'application/json',
              'x-vqs-queue-name': `__wkf_workflow_timeout-${i}`,
              'x-vqs-message-id': `msg_${String(i).padStart(8, '0')}`,
              'x-vqs-message-attempt': '1',
            },
            body: JSON.stringify({ runId: `wrun_${i}` }),
          });
          const response = await queueHandler(req);
          if (!response.ok)
            throw new Error(`Unexpected status ${response.status}`);
        },
        200
      )
    );

    printSection('Queue Handler — Timeout Responses');
    printResults(results);
  });

  it('getDeploymentId — caching', async () => {
    const config: Partial<Config> = {
      dataDir: '/tmp/bench-queue-deploy',
      baseUrl: 'http://localhost:9999',
    };
    const q = createQueue(config);
    cleanup = () => q.close();

    const results = [];

    results.push(
      await bench(
        'getDeploymentId (cached)',
        async () => {
          await q.getDeploymentId();
        },
        1000
      )
    );

    printSection('getDeploymentId');
    printResults(results);
    // Should be extremely fast after first call (cached)
    expect(results[0].p50Ms).toBeLessThan(1);
  });
});
