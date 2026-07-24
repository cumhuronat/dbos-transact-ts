"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.wfQueueRunner = exports.WorkflowQueue = exports.logQueue = void 0;
const dbos_executor_1 = require("./dbos-executor");
const dbos_1 = require("./dbos");
const debugpoint_1 = require("./debugpoint");
const utils_1 = require("./utils");
/**
 * Log a single queue's name and its set parameters. Unset parameters are
 * omitted, matching `Queue: <name> (concurrency=…, worker_concurrency=…,
 * limit=N/Ts, priority, partitioned)`.
 */
function logQueue(logger, q) {
    const opts = [];
    if (q.concurrency !== undefined)
        opts.push(`concurrency=${q.concurrency}`);
    if (q.workerConcurrency !== undefined)
        opts.push(`worker_concurrency=${q.workerConcurrency}`);
    if (q.rateLimit !== undefined)
        opts.push(`limit=${q.rateLimit.limitPerPeriod}/${q.rateLimit.periodSec}s`);
    if (q.priorityEnabled)
        opts.push('priority');
    if (q.partitionQueue)
        opts.push('partitioned');
    const optsStr = opts.length > 0 ? ` (${opts.join(', ')})` : '';
    logger.info(`Queue: ${q.name}${optsStr}`);
}
exports.logQueue = logQueue;
/**
 * Per-instance association of a client-bound queue to its `SystemDatabase`.
 * Stored off-class because any class member — including TS `private` — gives
 * the class a nominal brand, so the type-only members below all live as
 * module-level helpers to keep `WorkflowQueue` structurally compatible across
 * separate compiled copies of this package.
 */
const clientSystemDatabases = new WeakMap();
function requireDatabaseBacked(q) {
    if (!q.databaseBacked) {
        throw new Error(`Cannot configure queue ${q.name}: dynamic configuration is only supported for queues registered via DBOS.registerQueue.`);
    }
}
function sysDBFor(q) {
    const clientDb = clientSystemDatabases.get(q);
    if (clientDb)
        return clientDb;
    const exec = dbos_executor_1.DBOSExecutor.globalInstance;
    if (!exec) {
        throw new Error(`Cannot access system database for queue ${q.name}: DBOS has not been launched.`);
    }
    return exec.systemDatabase;
}
/**
 * Re-read the queue's row from the database and update the cached fields on
 * `q` in place. No-op for in-memory queues. Throws if the row has been
 * deleted.
 */
async function refreshFromDb(q) {
    if (!q.databaseBacked)
        return;
    const record = await sysDBFor(q).getQueue(q.name);
    if (record === null) {
        throw new Error(`Queue '${q.name}' was not found in the database.`);
    }
    q.concurrency = record.concurrency ?? undefined;
    q.workerConcurrency = record.workerConcurrency ?? undefined;
    q.rateLimit =
        record.rateLimitMax !== null && record.rateLimitPeriodSec !== null
            ? { limitPerPeriod: record.rateLimitMax, periodSec: record.rateLimitPeriodSec }
            : undefined;
    q.priorityEnabled = record.priorityEnabled;
    q.partitionQueue = record.partitionQueue;
    q.minPollingIntervalMs = record.pollingIntervalSec * 1000;
}
/**
 * Settings structure for a named workflow queue.
 * Workflow queues limit the rate and concurrency at which DBOS executes workflows.
 * Queue policies apply to workflows started by `DBOS.startWorkflow`,
 *   `DBOS.withWorkflowQueue`, etc.
 */
class WorkflowQueue {
    name;
    /**
     * Last-known cached values. May be stale for database-backed queues if
     * another process has modified the row. Use getters instead.
     */
    concurrency;
    rateLimit;
    workerConcurrency;
    priorityEnabled = false;
    partitionQueue = false;
    minPollingIntervalMs;
    /**
     * When true, this queue's configuration is persisted in the `queues` system
     * table and may be mutated at runtime via the `setX` methods. When false,
     * the queue's configuration is fixed at construction and lives only in
     * process memory.
     */
    databaseBacked = false;
    /**
     * True when configuration reads/writes target a `DBOSClient`-supplied
     * SystemDatabase rather than the global executor's. The actual handle is
     * kept off this class's public type — see the module-level WeakMap below —
     * so that `WorkflowQueue` does not transitively depend on `SystemDatabase`.
     */
    clientBound = false;
    constructor(name, arg2, rateLimit) {
        this.name = name;
        if (dbos_1.DBOS.isInitialized()) {
            dbos_1.DBOS.logger.warn(`In-memory workflow queue '${name}' was created after DBOS initialization and will not be picked up by the queue dispatcher. ` +
                `Use DBOS.registerQueue to register a database-backed queue at runtime.`);
        }
        let params;
        if (typeof arg2 === 'object' && arg2 !== null) {
            params = arg2;
        }
        else {
            params = { concurrency: arg2, rateLimit };
        }
        WorkflowQueue.validateQueueParams(params);
        this.concurrency = params.concurrency;
        this.rateLimit = params.rateLimit;
        this.workerConcurrency = params.workerConcurrency;
        this.priorityEnabled = params.priorityEnabled ?? false;
        this.partitionQueue = params.partitionQueue ?? false;
        this.minPollingIntervalMs = params.minPollingIntervalMs;
        if (exports.wfQueueRunner.wfQueuesByName.has(name)) {
            throw Error(`Workflow Queue '${name}' defined multiple times`);
        }
        exports.wfQueueRunner.wfQueuesByName.set(name, this);
    }
    /** Throws if any combination of queue parameters is invalid. */
    static validateQueueParams(params) {
        const { concurrency, workerConcurrency, rateLimit, minPollingIntervalMs } = params;
        if (workerConcurrency !== undefined && concurrency !== undefined && workerConcurrency > concurrency) {
            throw new Error('concurrency must be greater than or equal to workerConcurrency');
        }
        if (minPollingIntervalMs !== undefined && minPollingIntervalMs <= 0) {
            throw new Error('minPollingIntervalMs must be positive');
        }
        if (rateLimit !== undefined && (rateLimit.limitPerPeriod === undefined || rateLimit.periodSec === undefined)) {
            throw new Error('rateLimit must specify both limitPerPeriod and periodSec');
        }
    }
    /** Build a persistable record from user-supplied registration parameters. */
    static recordFromParams(name, params) {
        return {
            name,
            concurrency: params.concurrency ?? null,
            workerConcurrency: params.workerConcurrency ?? null,
            rateLimitMax: params.rateLimit ? params.rateLimit.limitPerPeriod : null,
            rateLimitPeriodSec: params.rateLimit ? params.rateLimit.periodSec : null,
            priorityEnabled: params.priorityEnabled ?? false,
            partitionQueue: params.partitionQueue ?? false,
            pollingIntervalSec: (params.minPollingIntervalMs ?? 1000) / 1000,
        };
    }
    /**
     * Construct a database-backed queue from a persisted record. Bypasses the
     * legacy constructor so the instance is not added to the global registry —
     * the queues table is the source of truth.
     * @internal
     */
    static _fromRecord(record, clientSystemDatabase) {
        // Allocate without invoking the constructor (which would auto-register
        // in `wfQueuesByName`) and strip `readonly` so we can set the fields here.
        const q = Object.create(WorkflowQueue.prototype);
        q.name = record.name;
        q.concurrency = record.concurrency ?? undefined;
        q.workerConcurrency = record.workerConcurrency ?? undefined;
        q.rateLimit =
            record.rateLimitMax !== null && record.rateLimitPeriodSec !== null
                ? { limitPerPeriod: record.rateLimitMax, periodSec: record.rateLimitPeriodSec }
                : undefined;
        q.priorityEnabled = record.priorityEnabled;
        q.partitionQueue = record.partitionQueue;
        q.minPollingIntervalMs = record.pollingIntervalSec * 1000;
        q.databaseBacked = true;
        q.clientBound = clientSystemDatabase !== undefined;
        if (clientSystemDatabase !== undefined) {
            clientSystemDatabases.set(q, clientSystemDatabase);
        }
        return q;
    }
    async setConcurrency(value) {
        requireDatabaseBacked(this);
        if (value !== undefined && this.workerConcurrency !== undefined && this.workerConcurrency > value) {
            throw new Error('workerConcurrency must be less than or equal to concurrency');
        }
        await sysDBFor(this).updateQueue(this.name, { concurrency: value ?? null });
        this.concurrency = value;
    }
    async setWorkerConcurrency(value) {
        requireDatabaseBacked(this);
        if (value !== undefined && this.concurrency !== undefined && value > this.concurrency) {
            throw new Error('workerConcurrency must be less than or equal to concurrency');
        }
        await sysDBFor(this).updateQueue(this.name, { workerConcurrency: value ?? null });
        this.workerConcurrency = value;
    }
    async setRateLimit(value) {
        requireDatabaseBacked(this);
        if (value !== undefined && (value.limitPerPeriod === undefined || value.periodSec === undefined)) {
            throw new Error('rateLimit must specify both limitPerPeriod and periodSec');
        }
        await sysDBFor(this).updateQueue(this.name, {
            rateLimitMax: value ? value.limitPerPeriod : null,
            rateLimitPeriodSec: value ? value.periodSec : null,
        });
        this.rateLimit = value;
    }
    async setPriorityEnabled(value) {
        requireDatabaseBacked(this);
        await sysDBFor(this).updateQueue(this.name, { priorityEnabled: value });
        this.priorityEnabled = value;
    }
    async setPartitionQueue(value) {
        requireDatabaseBacked(this);
        await sysDBFor(this).updateQueue(this.name, { partitionQueue: value });
        this.partitionQueue = value;
    }
    async setMinPollingIntervalMs(value) {
        requireDatabaseBacked(this);
        if (value <= 0) {
            throw new Error('minPollingIntervalMs must be positive');
        }
        await sysDBFor(this).updateQueue(this.name, { pollingIntervalSec: value / 1000 });
        this.minPollingIntervalMs = value;
    }
    async getConcurrency() {
        await refreshFromDb(this);
        return this.concurrency;
    }
    async getWorkerConcurrency() {
        await refreshFromDb(this);
        return this.workerConcurrency;
    }
    async getRateLimit() {
        await refreshFromDb(this);
        return this.rateLimit;
    }
    async getPriorityEnabled() {
        await refreshFromDb(this);
        return this.priorityEnabled;
    }
    async getPartitionQueue() {
        await refreshFromDb(this);
        return this.partitionQueue;
    }
    async getMinPollingIntervalMs() {
        await refreshFromDb(this);
        return this.minPollingIntervalMs;
    }
}
exports.WorkflowQueue = WorkflowQueue;
class WFQueueRunner {
    wfQueuesByName = new Map();
    /**
     * Queues fed by this process's own pollers (e.g. a Kafka consumer). Always dispatched,
     * regardless of any listenQueues filter, so this process executes what it enqueues.
     */
    pollerQueueNames = new Set();
    isRunning = false;
    abortController;
    listenQueueNames = null;
    /** Per-queue scheduling state, keyed by queue name. */
    states = new Map();
    /** Names already warned about colliding with an in-memory queue (warn-once). */
    conflictWarned = new Set();
    static defaultMinPollingIntervalMs = 1000;
    static defaultMaxPollingIntervalMs = 120000;
    static reconcileIntervalMs = 1000;
    static transitionIntervalMs = 1000;
    backoffFactor = 2.0;
    scalebackFactor = 0.9;
    jitterMin = 0.95;
    jitterMax = 1.05;
    stop() {
        if (!this.isRunning)
            return;
        this.isRunning = false;
        this.abortController?.abort();
    }
    clearRegistrations() {
        this.wfQueuesByName.clear();
        this.pollerQueueNames.clear();
    }
    async dispatchLoop(exec, listenQueuesArg, maxConcurrentQueueDispatches = 3) {
        this.isRunning = true;
        this.states.clear();
        this.conflictWarned.clear();
        this.listenQueueNames = listenQueuesArg
            ? new Set(listenQueuesArg.map((entry) => (typeof entry === 'string' ? entry : entry.name)))
            : null;
        this.abortController = new AbortController();
        const startNow = Date.now();
        // The internal queue is process-private and bypasses the listenQueues filter.
        const internal = this.wfQueuesByName.get(utils_1.INTERNAL_QUEUE_NAME);
        if (internal)
            this.ensureState(internal, startNow);
        // Unmatched string entries are deferred to refreshDbQueues as DB-backed queues.
        for (const q of this.resolveInMemoryQueues(listenQueuesArg)) {
            this.ensureState(q, startNow);
        }
        // Add pre-launch DB-backed queues now so an immediate enqueue can't race the first reconcile.
        await this.refreshDbQueues(exec, startNow);
        // Log everything we're now dispatching for, before the loop starts.
        this.logRunningQueues(exec);
        // One loop drives global maintenance; queue polls run in a bounded set of independent lanes.
        await this.schedulerLoop(exec, startNow, maxConcurrentQueueDispatches);
    }
    /** Resolve the listenQueues argument to the set of in-memory queues to dispatch for. */
    resolveInMemoryQueues(listenQueuesArg) {
        if (listenQueuesArg === null) {
            return Array.from(this.wfQueuesByName.values()).filter((q) => q.name !== utils_1.INTERNAL_QUEUE_NAME);
        }
        const result = [];
        for (const entry of listenQueuesArg) {
            if (typeof entry === 'string') {
                const q = this.wfQueuesByName.get(entry);
                if (q)
                    result.push(q);
            }
            else {
                result.push(entry);
            }
        }
        // Poller-fed queues are always dispatched: this process enqueues onto them, so under a
        // listenQueues filter their workflows would otherwise sit ENQUEUED forever.
        for (const name of this.pollerQueueNames) {
            const q = this.wfQueuesByName.get(name);
            if (q && !result.some((r) => r.name === name))
                result.push(q);
        }
        return result;
    }
    /** Begin tracking a queue if it isn't already, scheduling its first poll one interval out. */
    ensureState(queue, now) {
        if (this.states.has(queue.name))
            return;
        const interval = queue.minPollingIntervalMs ?? WFQueueRunner.defaultMinPollingIntervalMs;
        this.states.set(queue.name, { queue, currentPollingMs: interval, nextPollAt: now + interval });
    }
    /** Reconcile DB-backed queues against the queues table in one query: refresh, add, or drop them. */
    async refreshDbQueues(exec, now) {
        let records;
        try {
            records = await exec.systemDatabase.listQueues();
        }
        catch (e) {
            exec.logger.warn(`Error listing database-backed queues: ${e.message}`);
            return;
        }
        const present = new Set();
        for (const record of records) {
            if (record.name === utils_1.INTERNAL_QUEUE_NAME)
                continue;
            if (this.wfQueuesByName.has(record.name)) {
                if (!this.conflictWarned.has(record.name)) {
                    this.conflictWarned.add(record.name);
                    exec.logger.warn(`Database-backed queue '${record.name}' has the same name as an in-memory queue. ` +
                        `The in-memory queue's configuration is being used; the database-backed queue is ignored. ` +
                        `Rename one of them to resolve the conflict.`);
                }
                continue;
            }
            if (this.listenQueueNames !== null &&
                !this.listenQueueNames.has(record.name) &&
                !this.pollerQueueNames.has(record.name)) {
                continue;
            }
            present.add(record.name);
            const existing = this.states.get(record.name);
            if (existing) {
                // Refresh config in place, preserving this queue's polling/backoff state.
                existing.queue = WorkflowQueue._fromRecord(record);
            }
            else {
                this.ensureState(WorkflowQueue._fromRecord(record), now);
            }
        }
        // A database-backed queue whose row is gone stops being dispatched.
        for (const [name, state] of this.states) {
            if (!state.queue.databaseBacked)
                continue;
            if (!present.has(name)) {
                exec.logger.info(`Queue '${name}' has been deleted from the database; no longer dispatching it.`);
                this.states.delete(name);
            }
        }
    }
    /** Log every queue this process will dispatch for, once at startup after discovery. */
    logRunningQueues(exec) {
        const names = Array.from(this.states.keys()).filter((n) => n !== utils_1.INTERNAL_QUEUE_NAME);
        exec.logger.info(`Listening to ${names.length} queues:`);
        for (const name of names) {
            logQueue(exec.logger, this.states.get(name).queue);
        }
    }
    /** Reconcile queues and schedule due polls across a bounded number of independent lanes. */
    async schedulerLoop(exec, startNow, maxConcurrentQueueDispatches) {
        const signal = this.abortController.signal;
        const inFlightPolls = new Map();
        let wakePending = false;
        let wakeScheduler;
        const wake = () => {
            if (wakeScheduler) {
                wakeScheduler();
            }
            else {
                wakePending = true;
            }
        };
        const waitForWakeOrTimeout = async (ms) => {
            if (signal.aborted)
                return;
            if (wakePending) {
                wakePending = false;
                return;
            }
            await new Promise((resolve) => {
                const finish = () => {
                    clearTimeout(timer);
                    signal.removeEventListener('abort', onAbort);
                    if (wakeScheduler === finish)
                        wakeScheduler = undefined;
                    resolve();
                };
                const onAbort = () => finish();
                wakeScheduler = finish;
                signal.addEventListener('abort', onAbort, { once: true });
                const timer = setTimeout(finish, ms);
            });
        };
        // Wake the scheduler when a workflow becomes ENQUEUED. The SystemDatabase emits an app-side,
        // coalesced NOTIFY at ENQUEUED-transitions only (never per status update — the workflow_status
        // table is far too hot for a trigger; see its wake-channel note), and delivers the woken queue
        // name here. We mark that queue due and trip waitForWakeOrTimeout, so an enqueue dispatches in
        // ~one coalesce window instead of waiting up to one poll interval. Strictly a hint: the poll
        // cadence below is unchanged and stays the correctness floor, so a missed NOTIFY costs at most
        // one interval. `registerQueueWake` returns undefined (a no-op) when wakes are disabled or
        // LISTEN/NOTIFY is unavailable, leaving the pure-poll behavior byte-for-byte intact.
        const queueWakeHandle = exec.systemDatabase.registerQueueWake((queueName) => {
            if (queueName !== undefined) {
                const state = this.states.get(queueName);
                // Untracked queue (e.g. a brand-new DB-backed queue not yet reconciled): the poll floor and
                // the next reconcile still pick it up, so a bare wake is safe and sufficient.
                if (state)
                    state.nextPollAt = 0;
            }
            wake();
        });
        // Discovery already ran during setup; defer the next reconcile a full interval.
        let lastReconcileAt = startNow;
        // Global op: run on a fixed cadence, not once per wake (destaggered wakeups would push it to ~N/sec).
        let lastTransitionAt = 0;
        while (this.isRunning) {
            const now = Date.now();
            // Reconcile DB-backed queues with a single query, independent of queue count.
            if (now - lastReconcileAt >= WFQueueRunner.reconcileIntervalMs) {
                await this.refreshDbQueues(exec, now);
                lastReconcileAt = now;
            }
            // Transition delayed workflows at most once per interval — it is global, so one call covers every queue.
            if (now - lastTransitionAt >= WFQueueRunner.transitionIntervalMs) {
                try {
                    await exec.systemDatabase.transitionDelayedWorkflows();
                }
                catch (e) {
                    exec.logger.warn(`Error transitioning delayed workflows: ${e.message}`);
                }
                lastTransitionAt = now;
            }
            this.scheduleDueQueues(exec, now, maxConcurrentQueueDispatches, inFlightPolls, wake);
            if (!this.isRunning)
                break;
            // Sleep until global maintenance or an idle queue's next poll.
            let nextWakeAt = Math.min(lastReconcileAt + WFQueueRunner.reconcileIntervalMs, lastTransitionAt + WFQueueRunner.transitionIntervalMs);
            // Skip queue times while all lanes are busy: a completing poll wakes us, so folding a due-but-unlaned queue in would spin at 0ms.
            if (inFlightPolls.size < maxConcurrentQueueDispatches) {
                for (const state of this.states.values()) {
                    if (!inFlightPolls.has(state.queue.name) && state.nextPollAt < nextWakeAt)
                        nextWakeAt = state.nextPollAt;
                }
            }
            const sleepMs = Math.max(0, nextWakeAt - Date.now());
            await waitForWakeOrTimeout(sleepMs);
        }
        // Stop receiving enqueue wakes before we drain: the loop is exiting, so any further wake would
        // only set wakePending on a dead scheduler. Safe even if the handle is undefined (wakes off).
        exec.systemDatabase.deregisterQueueWake(queueWakeHandle);
        await Promise.allSettled(Array.from(inFlightPolls.values()));
    }
    /** Start due queue polls up to the lane limit, in nextPollAt order so the longest-overdue queue goes first. */
    scheduleDueQueues(exec, now, maxConcurrentQueueDispatches, inFlightPolls, wake) {
        if (!this.isRunning)
            return;
        // Earliest nextPollAt first: a queue passed over while the lanes were full keeps its older
        // nextPollAt, so it outranks freshly-scheduled queues on the next pass and cannot be starved.
        const due = Array.from(this.states.values())
            .filter((state) => now >= state.nextPollAt && !inFlightPolls.has(state.queue.name))
            .sort((a, b) => a.nextPollAt - b.nextPollAt);
        for (const state of due) {
            if (inFlightPolls.size >= maxConcurrentQueueDispatches)
                break;
            inFlightPolls.set(state.queue.name, this.runQueuePoll(exec, state, inFlightPolls, wake));
        }
    }
    /** Run one queue's poll while reserving that queue's lane until its backoff state is updated. */
    async runQueuePoll(exec, state, inFlightPolls, wake) {
        const queueName = state.queue.name;
        // pollQueue swallows DB errors, so a rejection here is abnormal: back off instead of scaling back toward the minimum interval.
        let contentionDetected = true;
        try {
            contentionDetected = await this.pollQueue(exec, state.queue);
        }
        catch (e) {
            exec.logger.warn(`Unexpected error polling queue ${queueName}: ${e.message}`);
        }
        finally {
            this.adjustInterval(exec, state, contentionDetected);
            inFlightPolls.delete(queueName);
            wake();
        }
    }
    /** Poll one queue once, starting ready workflows; returns true if DB contention was detected. */
    async pollQueue(exec, queue) {
        let contentionDetected = false;
        // Helper function that starts dequeued workflows
        const dispatch = async (wfids) => {
            if (wfids.length > 0) {
                await (0, debugpoint_1.debugTriggerPoint)(debugpoint_1.DEBUG_TRIGGER_WORKFLOW_QUEUE_START);
            }
            for (const wfid of wfids) {
                try {
                    await exec.executeWorkflowId(wfid, { isQueueDispatch: true });
                }
                catch (e) {
                    exec.logger.warn(`Could not execute workflow with id ${wfid}: ${e.message}`);
                }
            }
        };
        // Dequeue workflows for this queue. If the queue is partitioned, successively dequeue and start
        // workflows from each active partition.
        try {
            if (queue.partitionQueue) {
                const partitionKeys = await exec.systemDatabase.getQueuePartitions(queue.name);
                for (const partitionKey of partitionKeys) {
                    const partitionWfids = await exec.systemDatabase.findAndMarkStartableWorkflows(queue, exec.executorID, utils_1.globalParams.appVersion, partitionKey);
                    await dispatch(partitionWfids);
                    await (0, debugpoint_1.debugTriggerPoint)(debugpoint_1.DEBUG_TRIGGER_BETWEEN_PARTITION_DISPATCHES);
                }
            }
            else {
                const wfids = await exec.systemDatabase.findAndMarkStartableWorkflows(queue, exec.executorID, utils_1.globalParams.appVersion, undefined);
                await dispatch(wfids);
            }
        }
        catch (e) {
            const err = e;
            // Handle serialization errors and lock contention with backoff
            if ('code' in err && (err.code === '40001' || err.code === '55P03')) {
                // 40001: serialization_failure, 55P03: lock_not_available
                contentionDetected = true;
                exec.logger.warn(`Contention detected in queue ${queue.name}.`);
            }
            else {
                exec.logger.warn(`Error getting startable workflows for queue ${queue.name}: ${err.message}`);
            }
        }
        return contentionDetected;
    }
    /** After a poll, grow the interval on contention or shrink it toward the minimum, then schedule the next poll with jitter. */
    adjustInterval(exec, state, contentionDetected) {
        const minPollingMs = state.queue.minPollingIntervalMs ?? WFQueueRunner.defaultMinPollingIntervalMs;
        const maxPollingMs = WFQueueRunner.defaultMaxPollingIntervalMs;
        if (contentionDetected) {
            state.currentPollingMs = Math.min(maxPollingMs, state.currentPollingMs * this.backoffFactor);
            exec.logger.warn(`Increasing polling interval for queue ${state.queue.name} to ${(state.currentPollingMs / 1000).toFixed(2)}s due to contention.`);
        }
        else {
            state.currentPollingMs = Math.max(minPollingMs, state.currentPollingMs * this.scalebackFactor);
        }
        // Clamp into the current [min, max] range in case config changed under us.
        state.currentPollingMs = Math.max(minPollingMs, Math.min(state.currentPollingMs, maxPollingMs));
        const jitter = this.jitterMin + Math.random() * (this.jitterMax - this.jitterMin);
        state.nextPollAt = Date.now() + state.currentPollingMs * jitter;
    }
}
exports.wfQueueRunner = new WFQueueRunner();
//# sourceMappingURL=wfqueue.js.map