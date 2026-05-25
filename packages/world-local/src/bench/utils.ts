/**
 * Benchmark utilities — lightweight timing harness for world-local perf tests.
 */

export interface BenchResult {
  name: string;
  ops: number;
  totalMs: number;
  avgMs: number;
  p50Ms: number;
  p95Ms: number;
  p99Ms: number;
  minMs: number;
  maxMs: number;
  opsPerSec: number;
}

/**
 * Run `fn` repeatedly for the given number of iterations,
 * collecting per-iteration wall-clock timings.
 */
export async function bench(
  name: string,
  fn: (i: number) => Promise<void>,
  iterations: number
): Promise<BenchResult> {
  const timings: number[] = [];

  // Warmup (10% of iterations, min 1)
  const warmup = Math.max(1, Math.floor(iterations * 0.1));
  for (let i = 0; i < warmup; i++) {
    await fn(i);
  }

  const t0 = performance.now();
  for (let i = 0; i < iterations; i++) {
    const start = performance.now();
    await fn(i);
    timings.push(performance.now() - start);
  }
  const totalMs = performance.now() - t0;

  timings.sort((a, b) => a - b);
  const avg = timings.reduce((s, t) => s + t, 0) / timings.length;

  return {
    name,
    ops: iterations,
    totalMs,
    avgMs: avg,
    p50Ms: timings[Math.floor(timings.length * 0.5)],
    p95Ms: timings[Math.floor(timings.length * 0.95)],
    p99Ms: timings[Math.floor(timings.length * 0.99)],
    minMs: timings[0],
    maxMs: timings[timings.length - 1],
    opsPerSec: (iterations / totalMs) * 1000,
  };
}

/**
 * Run `fn` concurrently with the given concurrency level.
 */
export async function benchConcurrent(
  name: string,
  fn: (i: number) => Promise<void>,
  totalOps: number,
  concurrency: number
): Promise<BenchResult> {
  const timings: number[] = [];

  const t0 = performance.now();
  let nextIdx = 0;

  async function worker() {
    while (true) {
      const idx = nextIdx++;
      if (idx >= totalOps) break;
      const start = performance.now();
      await fn(idx);
      timings.push(performance.now() - start);
    }
  }

  await Promise.all(Array.from({ length: concurrency }, () => worker()));
  const totalMs = performance.now() - t0;

  timings.sort((a, b) => a - b);
  const avg = timings.reduce((s, t) => s + t, 0) / timings.length;

  return {
    name,
    ops: totalOps,
    totalMs,
    avgMs: avg,
    p50Ms: timings[Math.floor(timings.length * 0.5)],
    p95Ms: timings[Math.floor(timings.length * 0.95)],
    p99Ms: timings[Math.floor(timings.length * 0.99)],
    minMs: timings[0],
    maxMs: timings[timings.length - 1],
    opsPerSec: (totalOps / totalMs) * 1000,
  };
}

export function printResults(results: BenchResult[]) {
  console.log('\n' + '═'.repeat(100));
  console.log(
    `${'Benchmark'.padEnd(50)} ${'ops/s'.padStart(10)} ${'avg'.padStart(8)} ${'p50'.padStart(8)} ${'p95'.padStart(8)} ${'p99'.padStart(8)} ${'max'.padStart(8)}`
  );
  console.log('─'.repeat(100));
  for (const r of results) {
    console.log(
      `${r.name.padEnd(50)} ${r.opsPerSec.toFixed(0).padStart(10)} ${fmt(r.avgMs).padStart(8)} ${fmt(r.p50Ms).padStart(8)} ${fmt(r.p95Ms).padStart(8)} ${fmt(r.p99Ms).padStart(8)} ${fmt(r.maxMs).padStart(8)}`
    );
  }
  console.log('═'.repeat(100));
}

function fmt(ms: number): string {
  if (ms < 1) return `${(ms * 1000).toFixed(0)}µs`;
  if (ms < 1000) return `${ms.toFixed(2)}ms`;
  return `${(ms / 1000).toFixed(2)}s`;
}

export function printSection(title: string) {
  console.log(
    `\n${'▓'.repeat(4)} ${title} ${'▓'.repeat(Math.max(0, 93 - title.length))}`
  );
}
