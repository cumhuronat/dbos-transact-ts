import { DBOSExecutor } from './dbos-executor';
import type { QueueRecord, SystemDatabase } from './system_database';
import type { GlobalLogger } from './telemetry/logs';
/**
 * Log a single queue's name and its set parameters. Unset parameters are
 * omitted, matching `Queue: <name> (concurrency=…, worker_concurrency=…,
 * limit=N/Ts, priority, partitioned)`.
 */
export declare function logQueue(logger: GlobalLogger, q: WorkflowQueue): void;
/**
 * Limit the maximum number of functions started from a `WorkflowQueue`
 *   per given time period.
 * If the limit is 5 and the period is 10, no more than 5 functions can be
 *   started per 10 seconds.
 */
export interface QueueRateLimit {
    /** Number of queue dispateches per `periodSec` */
    limitPerPeriod: number;
    /** Period of time during which `limitPerPeriod` queued workflows may be dispatched */
    periodSec: number;
}
/**
 * Limit the number of concurrent workflows running for a queue.
 * This limit may be per worker or global
 */
export interface QueueParameters {
    /** If defined, this limits the number of running workflows for a single DBOS process */
    workerConcurrency?: number;
    /** If defined, this limits the number of running workflows globally in the app */
    concurrency?: number;
    /** If set, this limits the rate at which queued workflows are started */
    rateLimit?: QueueRateLimit;
    /** If set, this queue supports priority */
    priorityEnabled?: boolean;
    /** If set, this queue supports partitioning */
    partitionQueue?: boolean;
    /** Base (minimum) polling interval in ms for this queue's dispatch loop (default 1000) */
    minPollingIntervalMs?: number;
}
/**
 * Behavior of `DBOS.registerQueue` / `DBOSClient.registerQueue` when a queue
 * with the same name already has a row in the `queues` table.
 *
 * - `update_if_latest_version`: overwrite the existing row only when the
 *   running application version is the latest registered version. Older
 *   versions in a rolling deploy will not overwrite a newer config.
 * - `always_update`: always overwrite the existing row.
 * - `never_update`: leave the existing row unchanged. The returned queue
 *   reflects the persisted config, not the supplied parameters.
 */
export type QueueConflictResolution = 'update_if_latest_version' | 'always_update' | 'never_update';
export interface RegisterQueueOptions extends QueueParameters {
    /** How to behave when a queue with the same name already exists. */
    onConflict?: QueueConflictResolution;
}
/**
 * Settings structure for a named workflow queue.
 * Workflow queues limit the rate and concurrency at which DBOS executes workflows.
 * Queue policies apply to workflows started by `DBOS.startWorkflow`,
 *   `DBOS.withWorkflowQueue`, etc.
 */
export declare class WorkflowQueue {
    readonly name: string;
    /**
     * Last-known cached values. May be stale for database-backed queues if
     * another process has modified the row. Use getters instead.
     */
    concurrency?: number;
    rateLimit?: QueueRateLimit;
    workerConcurrency?: number;
    priorityEnabled: boolean;
    partitionQueue: boolean;
    minPollingIntervalMs?: number;
    /**
     * When true, this queue's configuration is persisted in the `queues` system
     * table and may be mutated at runtime via the `setX` methods. When false,
     * the queue's configuration is fixed at construction and lives only in
     * process memory.
     */
    readonly databaseBacked: boolean;
    /**
     * True when configuration reads/writes target a `DBOSClient`-supplied
     * SystemDatabase rather than the global executor's. The actual handle is
     * kept off this class's public type — see the module-level WeakMap below —
     * so that `WorkflowQueue` does not transitively depend on `SystemDatabase`.
     */
    readonly clientBound: boolean;
    constructor(name: string);
    /**
     *
     * @param name - Name to give the `WorkflowQueue`, accepted by `DBOS.startWorkflow`
     * @param queueParameters - Policy for limiting workflow initiation rate and execution concurrency
     */
    constructor(name: string, queueParameters: QueueParameters);
    /** Throws if any combination of queue parameters is invalid. */
    static validateQueueParams(params: QueueParameters): void;
    /** Build a persistable record from user-supplied registration parameters. */
    static recordFromParams(name: string, params: QueueParameters): QueueRecord;
    /**
     * Construct a database-backed queue from a persisted record. Bypasses the
     * legacy constructor so the instance is not added to the global registry —
     * the queues table is the source of truth.
     * @internal
     */
    static _fromRecord(record: QueueRecord, clientSystemDatabase?: SystemDatabase): WorkflowQueue;
    setConcurrency(value: number | undefined): Promise<void>;
    setWorkerConcurrency(value: number | undefined): Promise<void>;
    setRateLimit(value: QueueRateLimit | undefined): Promise<void>;
    setPriorityEnabled(value: boolean): Promise<void>;
    setPartitionQueue(value: boolean): Promise<void>;
    setMinPollingIntervalMs(value: number): Promise<void>;
    getConcurrency(): Promise<number | undefined>;
    getWorkerConcurrency(): Promise<number | undefined>;
    getRateLimit(): Promise<QueueRateLimit | undefined>;
    getPriorityEnabled(): Promise<boolean>;
    getPartitionQueue(): Promise<boolean>;
    getMinPollingIntervalMs(): Promise<number | undefined>;
}
declare class WFQueueRunner {
    readonly wfQueuesByName: Map<string, WorkflowQueue>;
    /**
     * Queues fed by this process's own pollers (e.g. a Kafka consumer). Always dispatched,
     * regardless of any listenQueues filter, so this process executes what it enqueues.
     */
    readonly pollerQueueNames: Set<string>;
    private isRunning;
    private abortController?;
    private listenQueueNames;
    /** Per-queue scheduling state, keyed by queue name. */
    private readonly states;
    /** Names already warned about colliding with an in-memory queue (warn-once). */
    private readonly conflictWarned;
    private static readonly defaultMinPollingIntervalMs;
    private static readonly defaultMaxPollingIntervalMs;
    private static readonly reconcileIntervalMs;
    private static readonly transitionIntervalMs;
    private readonly backoffFactor;
    private readonly scalebackFactor;
    private readonly jitterMin;
    private readonly jitterMax;
    stop(): void;
    clearRegistrations(): void;
    dispatchLoop(exec: DBOSExecutor, listenQueuesArg: (WorkflowQueue | string)[] | null, maxConcurrentQueueDispatches?: number): Promise<void>;
    /** Resolve the listenQueues argument to the set of in-memory queues to dispatch for. */
    private resolveInMemoryQueues;
    /** Begin tracking a queue if it isn't already, scheduling its first poll one interval out. */
    private ensureState;
    /** Reconcile DB-backed queues against the queues table in one query: refresh, add, or drop them. */
    private refreshDbQueues;
    /** Log every queue this process will dispatch for, once at startup after discovery. */
    private logRunningQueues;
    /** Reconcile queues and schedule due polls across a bounded number of independent lanes. */
    private schedulerLoop;
    /** Start due queue polls up to the lane limit, in nextPollAt order so the longest-overdue queue goes first. */
    private scheduleDueQueues;
    /** Run one queue's poll while reserving that queue's lane until its backoff state is updated. */
    private runQueuePoll;
    /** Poll one queue once, starting ready workflows; returns true if DB contention was detected. */
    private pollQueue;
    /** After a poll, grow the interval on contention or shrink it toward the minimum, then schedule the next poll with jitter. */
    private adjustInterval;
}
export declare const wfQueueRunner: WFQueueRunner;
export {};
//# sourceMappingURL=wfqueue.d.ts.map