import { DBOS, WorkflowQueue, StatusString } from '../src';
import { DBOSConfig, DBOSExecutor } from '../src/dbos-executor';
import { generateDBOSTestConfig, setUpDBOSTestSysDb, Event } from './helpers';
import { randomUUID } from 'node:crypto';
import { Client } from 'pg';
import { DBOS_QUEUE_WAKEUP_CHANNEL, DBOS_WORKFLOW_COMPLETION_CHANNEL } from '../src/system_database';

/**
 * Wake-on-enqueue (fork feature) tests.
 *
 * Two hint-only wakes ride the existing coalesced NOTIFY notifier:
 *  - enqueue wake: an ENQUEUED-transition trips the queue scheduler before its next poll.
 *  - completion wake: a terminal transition trips a getResult() waiter before its next poll.
 *
 * The proof pattern (matching the repo's stream/recv hybrid tests): set the relevant poll
 * interval HIGH so the poll floor cannot rescue the timing, then assert the wake resolves the
 * wait far inside that interval. Each such test reddens if any link (emit / LISTEN / map /
 * scheduler hook / getResult race) breaks. Separate suites prove the poll floor still carries
 * the case when wakes are disabled or LISTEN/NOTIFY is unavailable.
 */

// A very high poll floor: if a wake is doing the work, the wait ends in well under this; if it
// is NOT, the test would have to wait ~30s (and fails the sub-2s bound first).
const HIGH_POLL_MS = 30_000;
// Comfortable upper bound for a wake-driven resolution on CI/dev hardware.
const WAKE_BOUND_MS = 4_000;
// Coalesce window used by the "enabled" suite: wide enough to make the coalescing proof deterministic.
const COALESCE_MS = 100;

// Module-scope registration (once): registration persists across launch/shutdown cycles, so the
// per-suite configs below each relaunch against the same workflow. A test hook lets a body signal
// its start time without persisting anything.
let onWorkflowStart: (() => void) | undefined;

const wakeWorkflow = DBOS.registerWorkflow(
  (value: string): Promise<string> => {
    onWorkflowStart?.();
    return Promise.resolve(`${value}!`);
  },
  { name: 'wakeOnEnqueueTestWorkflow' },
);

function rawListenClient(channel: string, onNotify: (payload: string | null) => void) {
  const url = generateDBOSTestConfig().systemDatabaseUrl!;
  const client = new Client({ connectionString: url });
  return {
    async start() {
      await client.connect();
      client.on('notification', (msg) => {
        if (msg.channel === channel) onNotify(msg.payload ?? null);
      });
      await client.query(`LISTEN ${channel}`);
    },
    async stop() {
      await client.end().catch(() => undefined);
    },
  };
}

describe('wake-on-enqueue: enabled (Postgres, wakes on)', () => {
  let config: DBOSConfig;
  // In-memory queues are created in beforeAll BEFORE launch (DBOS.shutdown clears the queue
  // registry between suites): created pre-launch, they are tracked from dispatch setup with no
  // reconcile race, so an enqueue immediately after launch can only be dispatched fast by the wake.
  let slowQueue: WorkflowQueue; // poll floor far above the wake bound: only the enqueue wake dispatches fast
  let fastQueue: WorkflowQueue; // fast dispatch, so a HIGH getResult poll can only be waiting on the completion wake

  beforeAll(async () => {
    config = generateDBOSTestConfig();
    // Widen the coalesce window well above its 10ms default so the coalescing test is a deterministic
    // proof rather than a wall-clock race: a burst then collapses into one or two whole windows
    // regardless of enqueue count. Still far below the wake bound, so the latency proofs are unaffected.
    config.notificationCoalesceMs = COALESCE_MS;
    await setUpDBOSTestSysDb(config);
    DBOS.setConfig(config);
    slowQueue = new WorkflowQueue(`wakeSlowQ_${randomUUID().slice(0, 8)}`, { minPollingIntervalMs: HIGH_POLL_MS });
    fastQueue = new WorkflowQueue(`wakeFastQ_${randomUUID().slice(0, 8)}`, { minPollingIntervalMs: 100 });
    await DBOS.launch();
    // Wakes must actually be enabled for this suite to be meaningful.
    expect(DBOSExecutor.globalInstance!.systemDatabase.wakeNotificationsEnabled).toBe(true);
  });

  afterAll(async () => {
    onWorkflowStart = undefined;
    await DBOS.shutdown();
  });

  test('immediate enqueue wake dispatches far inside a 30s poll floor', async () => {
    const started = new Event();
    onWorkflowStart = () => started.set();

    const enqueuedAt = Date.now();
    const handle = await DBOS.startWorkflow(wakeWorkflow, {
      workflowID: randomUUID(),
      queueName: slowQueue.name,
    })('enqueue');

    await started.wait();
    const latency = Date.now() - enqueuedAt;
    expect(latency).toBeLessThan(WAKE_BOUND_MS); // would be ~30s if the enqueue wake were broken

    expect(await handle.getResult()).toBe('enqueue!');
    onWorkflowStart = undefined;
  });

  test('immediate completion wake resolves getResult far inside a 30s getResult poll', async () => {
    onWorkflowStart = undefined; // do not signal; we measure getResult, not body start

    // Fast queue → dispatch + run happen within ~100ms; the ONLY high-latency wait is getResult's
    // own poll, set to 30s. If the completion wake works, getResult returns in well under WAKE_BOUND.
    const handle = await DBOS.startWorkflow(wakeWorkflow, {
      workflowID: randomUUID(),
      queueName: fastQueue.name,
    })('complete');

    const calledAt = Date.now();
    const result = await handle.getResult({ pollingIntervalMs: HIGH_POLL_MS });
    const latency = Date.now() - calledAt;

    expect(result).toBe('complete!');
    expect(latency).toBeLessThan(WAKE_BOUND_MS); // would be ~30s if the completion wake were broken
  });

  test('a burst of same-queue enqueues coalesces: NOTIFYs are bounded by time, not by enqueue count', async () => {
    let enqueueNotifies = 0;
    const listener = rawListenClient(DBOS_QUEUE_WAKEUP_CHANNEL, () => {
      enqueueNotifies++;
    });
    await listener.start();
    try {
      const n = 25;
      // Fire the whole burst concurrently so the enqueues land in as few 10ms coalesce windows as
      // possible — the same-queue payload collapses to one entry per window it was signalled in.
      const t0 = Date.now();
      const handles = await Promise.all(
        Array.from({ length: n }, (_, i) =>
          DBOS.startWorkflow(wakeWorkflow, { workflowID: randomUUID(), queueName: fastQueue.name })(`b${i}`),
        ),
      );
      const burstMs = Date.now() - t0;
      await Promise.all(handles.map((h) => h.getResult()));
      // Give the coalescing notifier a few flush windows to deliver everything it will deliver.
      await new Promise((r) => setTimeout(r, 500));

      // The coalescer emits at most one same-queue NOTIFY per coalesce window (COALESCE_MS), so the
      // count tracks the burst DURATION, not the enqueue COUNT — the direct answer to the >40K/s cost
      // objection. Without coalescing this would be n (one NOTIFY per enqueue), far above this
      // time-based bound for any burst tighter than ~n windows.
      const windowBound = Math.ceil(burstMs / COALESCE_MS) + 2;
      expect(enqueueNotifies).toBeGreaterThan(0);
      expect(enqueueNotifies).toBeLessThan(n);
      expect(enqueueNotifies).toBeLessThanOrEqual(windowBound);
    } finally {
      await listener.stop();
    }
  });
});

describe('wake-on-enqueue: poll floor intact when wakes are disabled', () => {
  let config: DBOSConfig;
  let pollQueue: WorkflowQueue;

  beforeAll(async () => {
    config = generateDBOSTestConfig();
    config.enableWakeNotifications = false; // NOTIFY on for streams/events, wakes off
    await setUpDBOSTestSysDb(config);
    DBOS.setConfig(config);
    pollQueue = new WorkflowQueue(`wakePollQ_${randomUUID().slice(0, 8)}`, { minPollingIntervalMs: 200 });
    await DBOS.launch();
    expect(DBOSExecutor.globalInstance!.systemDatabase.wakeNotificationsEnabled).toBe(false);
  });

  afterAll(async () => {
    await DBOS.shutdown();
  });

  test('enqueue still dispatches and getResult still resolves via the poll floor', async () => {
    onWorkflowStart = undefined;
    const handle = await DBOS.startWorkflow(wakeWorkflow, {
      workflowID: randomUUID(),
      queueName: pollQueue.name,
    })('poll');
    // 200ms queue poll + 1s default getResult poll → resolves via polling alone, no wake involved.
    expect(await handle.getResult()).toBe('poll!');
  });

  test('no queue-wakeup or completion NOTIFYs are emitted while wakes are disabled', async () => {
    let wakeNotifies = 0;
    const q = rawListenClient(DBOS_QUEUE_WAKEUP_CHANNEL, () => wakeNotifies++);
    const c = rawListenClient(DBOS_WORKFLOW_COMPLETION_CHANNEL, () => wakeNotifies++);
    await q.start();
    await c.start();
    try {
      const handles = [];
      for (let i = 0; i < 8; i++) {
        handles.push(
          await DBOS.startWorkflow(wakeWorkflow, { workflowID: randomUUID(), queueName: pollQueue.name })(`n${i}`),
        );
      }
      await Promise.all(handles.map((h) => h.getResult()));
      await new Promise((r) => setTimeout(r, 500));
      expect(wakeNotifies).toBe(0); // emit is fully suppressed when wakes are off
    } finally {
      await q.stop();
      await c.stop();
    }
  });
});

describe('wake-on-enqueue: useListenNotify=false fallback (pure poll)', () => {
  let config: DBOSConfig;
  let noListenQueue: WorkflowQueue;

  beforeAll(async () => {
    config = generateDBOSTestConfig();
    config.useListenNotify = false; // disables LISTEN/NOTIFY entirely (PgBouncer-style deployment)
    await setUpDBOSTestSysDb(config);
    DBOS.setConfig(config);
    noListenQueue = new WorkflowQueue(`wakeNoListenQ_${randomUUID().slice(0, 8)}`, { minPollingIntervalMs: 200 });
    await DBOS.launch();
    // Wakes require LISTEN/NOTIFY, so they are off transitively even though enableWakeNotifications defaults on.
    expect(DBOSExecutor.globalInstance!.systemDatabase.wakeNotificationsEnabled).toBe(false);
    expect(DBOSExecutor.globalInstance!.systemDatabase.shouldUseDBNotifications).toBe(false);
  });

  afterAll(async () => {
    await DBOS.shutdown();
  });

  test('workflows still enqueue, dispatch, and complete on the poll floor with no listener', async () => {
    onWorkflowStart = undefined;
    const handle = await DBOS.startWorkflow(wakeWorkflow, {
      workflowID: randomUUID(),
      queueName: noListenQueue.name,
    })('fallback');
    expect(await handle.getResult()).toBe('fallback!');
    expect((await handle.getStatus())?.status).toBe(StatusString.SUCCESS);
  });
});
