"use strict";
var __decorate = (this && this.__decorate) || function (decorators, target, key, desc) {
    var c = arguments.length, r = c < 3 ? target : desc === null ? desc = Object.getOwnPropertyDescriptor(target, key) : desc, d;
    if (typeof Reflect === "object" && typeof Reflect.decorate === "function") r = Reflect.decorate(decorators, target, key, desc);
    else for (var i = decorators.length - 1; i >= 0; i--) if (d = decorators[i]) r = (c < 3 ? d(r) : c > 3 ? d(target, key, r) : d(target, key)) || r;
    return c > 3 && r && Object.defineProperty(target, key, r), r;
};
var __metadata = (this && this.__metadata) || function (k, v) {
    if (typeof Reflect === "object" && typeof Reflect.metadata === "function") return Reflect.metadata(k, v);
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.SystemDatabase = exports.ensureSystemDatabase = exports.grantDbosSchemaPermissions = exports.DEFAULT_NOTIFICATION_COALESCE_MS = exports.QUEUE_WAKEUP_KEY = exports.DBOS_WORKFLOW_COMPLETION_CHANNEL = exports.DBOS_QUEUE_WAKEUP_CHANNEL = exports.DBOS_STREAMS_CHANNEL = exports.DBOS_WORKFLOW_EVENTS_CHANNEL = exports.DBOS_NOTIFICATIONS_CHANNEL = exports.DBOS_STREAM_CLOSED_SENTINEL = exports.DEFAULT_POOL_SIZE = exports.DBOS_FUNCNAME_CLOSESTREAM = exports.DBOS_FUNCNAME_WRITESTREAM = exports.DBOS_FUNCNAME_GETSTATUS = exports.DBOS_FUNCNAME_SLEEP = exports.DBOS_FUNCNAME_GETEVENT = exports.DBOS_FUNCNAME_SETEVENT = exports.DBOS_FUNCNAME_RECV = exports.DBOS_FUNCNAME_SEND = void 0;
const dbos_executor_1 = require("./dbos-executor");
const pg_1 = require("pg");
const error_1 = require("./error");
const workflow_1 = require("./workflow");
const utils_1 = require("./utils");
const crypto_1 = require("crypto");
const utils_2 = require("./utils");
const database_utils_1 = require("./database_utils");
const migration_runner_1 = require("./sysdb_migrations/migration_runner");
const migrations_1 = require("./sysdb_migrations/internal/migrations");
const debugpoint_1 = require("./debugpoint");
const serialization_1 = require("./serialization");
exports.DBOS_FUNCNAME_SEND = 'DBOS.send';
exports.DBOS_FUNCNAME_RECV = 'DBOS.recv';
exports.DBOS_FUNCNAME_SETEVENT = 'DBOS.setEvent';
exports.DBOS_FUNCNAME_GETEVENT = 'DBOS.getEvent';
exports.DBOS_FUNCNAME_SLEEP = 'DBOS.sleep';
exports.DBOS_FUNCNAME_GETSTATUS = 'getStatus';
exports.DBOS_FUNCNAME_WRITESTREAM = 'DBOS.writeStream';
exports.DBOS_FUNCNAME_CLOSESTREAM = 'DBOS.closeStream';
exports.DEFAULT_POOL_SIZE = 10;
exports.DBOS_STREAM_CLOSED_SENTINEL = '__DBOS_STREAM_CLOSED__';
// LISTEN/NOTIFY channels. Streams and workflow_events are pushed by the notifier loop off the write path; notifications fires from an in-transaction DB trigger so recv is never woken before its row commits.
exports.DBOS_NOTIFICATIONS_CHANNEL = 'dbos_notifications_channel';
exports.DBOS_WORKFLOW_EVENTS_CHANNEL = 'dbos_workflow_events_channel';
exports.DBOS_STREAMS_CHANNEL = 'dbos_streams_channel';
// Wake channels: hint-only wakes for the queue scheduler (enqueue) and getResult
// waiters (completion). CRITICALLY, and unlike the three channels above, these carry
// NO DB trigger on workflow_status. That table is the hottest in the system — kraftp
// benchmarked >40K updates/s (>10K/s seen in production), orders of magnitude above
// the notifications/events/streams tables — so a per-row trigger+NOTIFY on it is
// prohibitive. Instead these wakes are emitted APP-SIDE and ONLY at ENQUEUED- and
// terminal-transitions (a tiny fraction of status-table traffic: ~one per workflow
// start and one per workflow finish, never per update) and are COALESCED through the
// existing notifier (see DEFAULT_NOTIFICATION_COALESCE_MS / #signalNotification), so
// the worst-case NOTIFY rate is bounded by the coalesce window regardless of workflow
// throughput. The poll loop remains the untouched correctness floor: a dropped or
// disabled wake costs at most one poll interval, never a lost workflow.
exports.DBOS_QUEUE_WAKEUP_CHANNEL = 'dbos_queue_wakeup';
exports.DBOS_WORKFLOW_COMPLETION_CHANNEL = 'dbos_workflow_completion';
// The single well-known key the queue scheduler registers its wake under. The woken
// queue's name rides as the callback event payload (not the map key), so one
// registration serves every queue and a wake can target a single queue.
exports.QUEUE_WAKEUP_KEY = 'dbos_queue_wakeup';
// Interval for coalescing LISTEN/NOTIFY notifications off the write path; caps the rate of notifying commits regardless of write throughput.
exports.DEFAULT_NOTIFICATION_COALESCE_MS = 10;
// Workflow statuses that are terminal — reaching one wakes a getResult() waiter on the completion channel.
const TERMINAL_WORKFLOW_STATUSES = new Set([
    workflow_1.StatusString.SUCCESS,
    workflow_1.StatusString.ERROR,
    workflow_1.StatusString.CANCELLED,
    workflow_1.StatusString.MAX_RECOVERY_ATTEMPTS_EXCEEDED,
]);
const QUEUE_COLUMN_BY_FIELD = {
    concurrency: 'concurrency',
    workerConcurrency: 'worker_concurrency',
    rateLimitMax: 'rate_limit_max',
    rateLimitPeriodSec: 'rate_limit_period_sec',
    priorityEnabled: 'priority_enabled',
    partitionQueue: 'partition_queue',
    pollingIntervalSec: 'polling_interval_sec',
};
function queueRecordFromRow(row) {
    return {
        name: row.name,
        concurrency: row.concurrency,
        workerConcurrency: row.worker_concurrency,
        rateLimitMax: row.rate_limit_max,
        rateLimitPeriodSec: row.rate_limit_period_sec,
        priorityEnabled: row.priority_enabled,
        partitionQueue: row.partition_queue,
        pollingIntervalSec: row.polling_interval_sec,
    };
}
async function grantDbosSchemaPermissions(databaseUrl, roleName, logger, schemaName = 'dbos') {
    logger.info(`Granting permissions for ${schemaName} schema to ${roleName}`);
    const client = new pg_1.Client((0, utils_2.getClientConfig)(databaseUrl));
    await client.connect();
    try {
        // Grant usage on the schema
        const grantUsageSql = `GRANT USAGE ON SCHEMA "${schemaName}" TO "${roleName}"`;
        logger.info(grantUsageSql);
        await client.query(grantUsageSql);
        // Grant all privileges on all existing tables in schema (includes views)
        const grantTablesSql = `GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA "${schemaName}" TO "${roleName}"`;
        logger.info(grantTablesSql);
        await client.query(grantTablesSql);
        // Grant all privileges on all sequences in schema
        const grantSequencesSql = `GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA "${schemaName}" TO "${roleName}"`;
        logger.info(grantSequencesSql);
        await client.query(grantSequencesSql);
        // Grant execute on all functions and procedures in schema
        const grantFunctionsSql = `GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA "${schemaName}" TO "${roleName}"`;
        logger.info(grantFunctionsSql);
        await client.query(grantFunctionsSql);
        // Grant default privileges for future objects in schema
        const alterTablesSql = `ALTER DEFAULT PRIVILEGES IN SCHEMA "${schemaName}" GRANT ALL ON TABLES TO "${roleName}"`;
        logger.info(alterTablesSql);
        await client.query(alterTablesSql);
        const alterSequencesSql = `ALTER DEFAULT PRIVILEGES IN SCHEMA "${schemaName}" GRANT ALL ON SEQUENCES TO "${roleName}"`;
        logger.info(alterSequencesSql);
        await client.query(alterSequencesSql);
        const alterFunctionsSql = `ALTER DEFAULT PRIVILEGES IN SCHEMA "${schemaName}" GRANT EXECUTE ON FUNCTIONS TO "${roleName}"`;
        logger.info(alterFunctionsSql);
        await client.query(alterFunctionsSql);
    }
    catch (e) {
        logger.error(`Failed to grant permissions to role ${roleName}: ${e.message}`);
        throw e;
    }
    finally {
        await client.end();
    }
}
exports.grantDbosSchemaPermissions = grantDbosSchemaPermissions;
async function ensureSystemDatabase(sysDbUrl, logger, customPool, schemaName = 'dbos', useListenNotify = true) {
    let client = null;
    if (customPool) {
        // If a custom pool is passed in, assume the database already exists and create
        // a client to run migrations.
        client = await customPool.connect();
    }
    else {
        // Otherwise, create the system database if it does not exist.
        const res = await (0, database_utils_1.ensurePGDatabase)({
            urlToEnsure: sysDbUrl,
            logger: (msg) => logger.debug(msg),
        });
        if (res.status === 'failed') {
            logger.warn(`Database could not be verified / created: ${(0, database_utils_1.maskDatabaseUrl)(sysDbUrl)}: ${res.message} ${res.hint ?? ''}\n  ${res.notes.join('\n')}`);
        }
        const cconnect = await (0, database_utils_1.connectToPGAndReportOutcome)(sysDbUrl, () => { }, 'System Database');
        if (cconnect.result !== 'ok') {
            logger.warn(`Unable to connect to system database at ${(0, database_utils_1.maskDatabaseUrl)(sysDbUrl)}
        ${cconnect.message}: (${cconnect.code ? cconnect.code : 'connectivity problem'})`);
            throw new error_1.DBOSInitializationError(`Unable to connect to system database at ${(0, database_utils_1.maskDatabaseUrl)(sysDbUrl)}`);
        }
        client = cconnect.client;
    }
    try {
        const versionRes = await client.query('SELECT version() AS version');
        const isCockroach = /cockroachdb/i.test(versionRes.rows[0]?.version ?? '');
        await (0, migration_runner_1.runSysMigrationsPg)(client, (0, migrations_1.allMigrations)(schemaName, { useListenNotify, isCockroach }), schemaName, {
            onWarn: (e) => logger.info(e),
            isCockroach,
        });
    }
    finally {
        try {
            if (customPool) {
                client.release();
            }
            else {
                await client.end();
            }
        }
        catch (e) { }
    }
}
exports.ensureSystemDatabase = ensureSystemDatabase;
class NotificationMap {
    map = new Map();
    curCK = 0;
    registerCallback(key, cb) {
        if (!this.map.has(key)) {
            this.map.set(key, new Map());
        }
        const ck = this.curCK++;
        this.map.get(key).set(ck, cb);
        return { key, ck };
    }
    deregisterCallback(k) {
        if (!this.map.has(k.key))
            return;
        const sm = this.map.get(k.key);
        if (!sm.has(k.ck))
            return;
        sm.delete(k.ck);
        if (sm.size === 0) {
            this.map.delete(k.key);
        }
    }
    callCallbacks(key, event) {
        if (!this.map.has(key))
            return;
        const sm = this.map.get(key);
        for (const cb of sm.values()) {
            cb(event);
        }
    }
}
function mapWorkflowStatus(row) {
    return {
        workflowUUID: row.workflow_uuid,
        status: row.status,
        workflowName: row.name,
        output: row.output ? row.output : null,
        error: row.error ? row.error : null,
        workflowClassName: row.class_name ?? '',
        workflowConfigName: row.config_name ?? '',
        queueName: row.queue_name ?? undefined,
        authenticatedUser: row.authenticated_user,
        assumedRole: row.assumed_role,
        authenticatedRoles: JSON.parse(row.authenticated_roles),
        request: row.request ? JSON.parse(row.request) : {},
        executorId: row.executor_id,
        createdAt: Number(row.created_at),
        updatedAt: Number(row.updated_at),
        applicationVersion: row.application_version,
        applicationID: row.application_id,
        recoveryAttempts: Number(row.recovery_attempts),
        input: row.inputs ? row.inputs : null,
        timeoutMS: row.workflow_timeout_ms ? Number(row.workflow_timeout_ms) : undefined,
        deadlineEpochMS: row.workflow_deadline_epoch_ms ? Number(row.workflow_deadline_epoch_ms) : undefined,
        deduplicationID: row.deduplication_id ?? undefined,
        priority: row.priority ?? 0,
        queuePartitionKey: row.queue_partition_key ?? undefined,
        startedAtEpochMs: row.started_at_epoch_ms ? Number(row.started_at_epoch_ms) : undefined,
        forkedFrom: row.forked_from ?? undefined,
        wasForkedFrom: row.was_forked_from ?? false,
        parentWorkflowID: row.parent_workflow_id ?? undefined,
        serialization: row.serialization,
        delayUntilEpochMS: row.delay_until_epoch_ms ? Number(row.delay_until_epoch_ms) : undefined,
        completedAt: row.completed_at ? Number(row.completed_at) : undefined,
        attributes: row.attributes ?? undefined,
        scheduleName: row.schedule_name ?? undefined,
        debounceDeadlineEpochMS: row.debounce_deadline_epoch_ms ? Number(row.debounce_deadline_epoch_ms) : undefined,
        isDebounced: row.is_debounced ?? false,
    };
}
// SQLSTATE classes/codes that are generally safe to retry
// https://www.postgresql.org/docs/current/errcodes-appendix.html
const RETRY_SQLSTATE_PREFIXES = new Set([
    '08', // Connection Exception
    '53', // Insufficient Resources
    '57', // Operator Intervention (e.g. admin_shutdown, cannot_connect_now)
]);
const RETRY_SQLSTATE_CODES = new Set([
    '40003', // statement_completion_unknown
]);
// Node.js transient network error codes (system call level)
const RETRY_NODE_ERRNOS = new Set([
    'ECONNRESET',
    'ECONNREFUSED',
    'EHOSTUNREACH',
    'ENETUNREACH',
    'ETIMEDOUT',
    'ECONNABORTED',
]);
function isPgDatabaseError(e) {
    // DatabaseError has 'code' (SQLSTATE)
    return !!e && typeof e === 'object' && typeof e.code === 'string' && e.code.length === 5;
}
function sqlStateLooksRetryable(sqlstate) {
    if (!sqlstate)
        return false;
    if (RETRY_SQLSTATE_CODES.has(sqlstate))
        return true;
    const prefix = sqlstate.toString().slice(0, 2);
    return RETRY_SQLSTATE_PREFIXES.has(prefix);
}
function nodeErrnoLooksRetryable(e) {
    const code = e.code;
    return !!code && RETRY_NODE_ERRNOS.has(code);
}
function messageLooksRetryable(msg) {
    const m = msg.toLowerCase();
    return (msg.includes('ECONNREFUSED') ||
        msg.includes('ECONNRESET') ||
        m.includes('connection timeout') ||
        m.includes('server closed the connection') ||
        m.includes('connection terminated unexpectedly') ||
        m.includes('client has encountered a connection error') ||
        m.includes('timeout exceeded when trying to connect') ||
        m.includes('could not connect to server'));
}
function* unwrapErrors(e) {
    // Walk through AggregateError.errors and cause chains
    const queue = [e];
    const seen = new Set();
    while (queue.length) {
        const cur = queue.shift();
        if (cur && typeof cur === 'object') {
            if (seen.has(cur))
                continue;
            seen.add(cur);
            // AggregateError (native and some libs)
            const ae = cur;
            if (Array.isArray(ae.errors))
                queue.push(...ae.errors);
            // cause chain
            const withCause = cur;
            if (withCause.cause)
                queue.push(withCause.cause);
            // some libs wrap in { error }
            const wrapped = cur;
            if (wrapped.error)
                queue.push(wrapped.error);
        }
        yield cur;
    }
}
// "What could possibly go wrong?"
function retriablePostgresException(err) {
    // Dig into AggregateErrors of various types
    for (const e of unwrapErrors(err)) {
        const anyErr = e;
        // For Postgres errors, check the code
        if (isPgDatabaseError(anyErr) && sqlStateLooksRetryable(anyErr.code)) {
            return true;
        }
        // Look for node-like retriable errors
        if (nodeErrnoLooksRetryable(anyErr)) {
            return true;
        }
        // Also, check for network issues in the string
        if (e instanceof Error) {
            if (e.stack && messageLooksRetryable(e.stack))
                return true;
            if (e.message && messageLooksRetryable(e.message))
                return true;
        }
        else {
            if (messageLooksRetryable(String(e)))
                return true;
        }
    }
    return false;
}
/**
 * If a workflow encounters a database connection issue while performing an operation,
 * block the workflow and retry the operation until it reconnects and succeeds.
 * In other words, if DBOS loses its database connection, everything pauses until the connection is recovered,
 * trading off availability for correctness.
 */
function dbRetry(options = {}) {
    const { initialBackoff = 1.0, maxBackoff = 60.0 } = options;
    return function (target, propertyName, descriptor) {
        const method = descriptor.value;
        descriptor.value = async function (...args) {
            let retries = 0;
            let backoff = initialBackoff;
            while (true) {
                try {
                    return await method.apply(this, args);
                }
                catch (e) {
                    if (retriablePostgresException(e)) {
                        retries++;
                        // Calculate backoff with jitter
                        const actualBackoff = backoff * (0.5 + Math.random());
                        dbos_executor_1.DBOSExecutor.globalInstance?.logger.warn(`Database connection failed: ${e instanceof Error ? e.message : String(e)}. ` +
                            `Retrying in ${actualBackoff.toFixed(2)}s (attempt ${retries})`);
                        // Sleep with backoff
                        await (0, utils_1.sleepms)(actualBackoff * 1000); // Convert to milliseconds
                        // Increase backoff for next attempt (exponential)
                        backoff = Math.min(backoff * 2, maxBackoff);
                    }
                    else {
                        throw e;
                    }
                }
            }
        };
        return descriptor;
    };
}
/**
 * General notes:
 *   The responsibilities of the `SystemDatabase` are to store data for workflows, and
 *     associated steps, transactions, messages, and events.  The system DB is
 *     also the IPC mechanism that performs notifications when things change, for
 *     example a receive is unblocked when a send occurs, or a cancel interrupts
 *     the receive.
 *   The `SystemDatabase` expects values in inputs/outputs/errors to be JSON.  However,
 *     the serialization process of turning data into JSON or converting it back, should
 *     be done elsewhere (executor), as it may require application-specific logic or extensions.
 */
class SystemDatabase {
    systemDatabaseUrl;
    logger;
    serializer;
    // ==================== Lifecycle ====================
    pool;
    schemaName;
    /*
     * Generally, notifications are asynchronous.  One should:
     *  Subscribe to updates
     *  Read the database item in question
     *  In response to updates, re-read the database item
     *  Unsubscribe at the end
     * The notification mechanism is reliable in the sense that it will eventually deliver updates
     *  or the DB connection will get dropped.  The right thing to do if you lose connectivity to
     *  the system DB is to exit the process and go through recovery... system DB writes, notifications,
     *  etc may not have completed correctly, and recovery is the way to rebuild in-memory state.
     *
     * NOTE:
     * PG Notifications are not fully reliable.
     *   Dropped connections are recoverable - you just need to restart and scan everything.
     *      (The whole VM being the logical choice, so workflows can recover from any write failures.)
     *   The real problem is, if the pipes out of the server are full... then notifications can be
     *     dropped, and only the PG server log may note it.  For those reasons, we do occasional polling
     */
    notificationsClient = null;
    dbPollingIntervalResultMs = 1000;
    dbPollingIntervalEventMs = 10000;
    shouldUseDBNotifications = true;
    // Wake-on-enqueue (fork): gates the app-side ENQUEUED/terminal wakes independently of
    // shouldUseDBNotifications, so the hottest workloads (see the wake-channel note above)
    // can pay literally nothing by turning wakes off while streams/events/recv NOTIFY stays on.
    wakeNotificationsEnabled = true;
    notificationsMap = new NotificationMap();
    workflowEventsMap = new NotificationMap();
    streamsMap = new NotificationMap();
    // Queue-scheduler wakes: the woken queue name rides as the callback event; keyed by QUEUE_WAKEUP_KEY.
    queueWakeMap = new NotificationMap();
    // getResult() completion wakes: keyed by workflow id.
    completionMap = new NotificationMap();
    customPool = false;
    // Interval for coalescing LISTEN/NOTIFY notifications pushed off the write path (Postgres + L/N only).
    notificationCoalesceMs = exports.DEFAULT_NOTIFICATION_COALESCE_MS;
    // Coalesced NOTIFY payloads keyed by channel, flushed by the notifier loop; soft-private so tests can drive a flush.
    pendingNotifications = new Map();
    #notifierActive = false;
    // Wakes the notifier out of its coalescing sleep so shutdown flushes promptly.
    #notifierWake = null;
    // The notifier loop's completion, awaited on destroy so a final flush precedes closing the pool.
    #notifierLoop = undefined;
    /**
     * Caps how many DB-backed polling reads (from wait operations) may run
     * concurrently against the pool, so a polling storm cannot check out every
     * client and starve control-plane operations. See {@link #pollWithLimiter}.
     */
    pollLimiter;
    runningWorkflowMap = new Map(); // Map from workflowID to workflow promise, queue name and partition key
    // Per-partition-key created_at cursors: keep per-key queue order monotonic across batches
    #batchCreatedAtCursors = new Map();
    constructor(systemDatabaseUrl, logger, serializer, sysDbPoolSize = exports.DEFAULT_POOL_SIZE, systemDatabasePool, schemaName = 'dbos', useListenNotify = true, pollingConcurrency, notificationCoalesceMs = exports.DEFAULT_NOTIFICATION_COALESCE_MS, wakeNotificationsEnabled = true) {
        this.systemDatabaseUrl = systemDatabaseUrl;
        this.logger = logger;
        this.serializer = serializer;
        this.schemaName = schemaName;
        this.shouldUseDBNotifications = useListenNotify;
        this.notificationCoalesceMs = notificationCoalesceMs;
        // Wakes need LISTEN/NOTIFY to reach other processes, so they are additionally gated on
        // shouldUseDBNotifications; the flag lets a deployment keep NOTIFY on but wakes off.
        this.wakeNotificationsEnabled = useListenNotify && wakeNotificationsEnabled;
        if (systemDatabasePool) {
            this.pool = systemDatabasePool;
            this.customPool = true;
        }
        else {
            const systemPoolConfig = {
                ...(0, utils_2.getClientConfig)(systemDatabaseUrl),
                // This sets the application_name column in pg_stat_activity
                application_name: `dbos_transact_${utils_1.globalParams.executorID}_${utils_1.globalParams.appVersion}`,
                max: sysDbPoolSize,
            };
            this.pool = new pg_1.Pool(systemPoolConfig);
        }
        // Default the polling limit to half the pool (minimum 1), reserving the rest
        // of the pool for control-plane operations.
        const effectivePoolSize = this.pool.options.max ?? sysDbPoolSize;
        const pollingLimit = pollingConcurrency ?? Math.max(1, Math.floor(effectivePoolSize / 2));
        this.pollLimiter = new utils_1.Semaphore(pollingLimit);
        this.pool.on('error', (err) => {
            this.logger.warn(`Unexpected error in pool: ${err}`);
        });
        this.pool.on('connect', (client) => {
            client.on('error', (err) => {
                this.logger.warn(`Unexpected error in idle client: ${err}`);
            });
        });
    }
    getSerializer() {
        return this.serializer;
    }
    async init() {
        await ensureSystemDatabase(this.systemDatabaseUrl, this.logger, this.customPool ? this.pool : undefined, this.schemaName, this.shouldUseDBNotifications);
        if (this.shouldUseDBNotifications) {
            await this.#listenForNotifications();
            // Push coalesced stream and event notifications off the write path.
            this.#notifierActive = true;
            this.#notifierLoop = this.#runNotifier();
        }
    }
    async destroy() {
        if (this.reconnectTimeout) {
            clearTimeout(this.reconnectTimeout);
        }
        // Stop the notifier and await its final flush before the pool closes.
        this.#notifierActive = false;
        this.#notifierWake?.();
        if (this.#notifierLoop) {
            await this.#notifierLoop;
            this.#notifierLoop = undefined;
        }
        if (this.notificationsClient) {
            try {
                this.notificationsClient.release(true);
            }
            catch (e) {
                this.logger.warn(`Error ending notifications client: ${String(e)}`);
            }
        }
        await this.pool.end();
    }
    // ==================== Workflow Status ====================
    async initWorkflowStatus(initStatus, ownerXid, options) {
        const client = await this.pool.connect();
        let shouldCommit = false;
        // Set on the committed enqueue path (fresh ENQUEUED row with a queue); emitted after COMMIT
        // so the scheduler wakes for a real, visible enqueue rather than a rolled-back or dequeued one.
        let enqueueWakeQueue = undefined;
        try {
            await client.query('BEGIN ISOLATION LEVEL READ COMMITTED');
            // Moving from enqueued to pending asks to increment recovery attempts... rather than in the recovery process
            //  where it moves from pending back to enqueued.
            const resRow = await this.insertWorkflowStatus(client, initStatus, ownerXid, !!options?.isRecoveryRequest || !!options?.isDequeuedRequest);
            if (resRow.name !== initStatus.workflowName) {
                const msg = `Workflow already exists with a different function name: ${resRow.name}, but the provided function name is: ${initStatus.workflowName}`;
                throw new error_1.DBOSConflictingWorkflowError(initStatus.workflowUUID, msg);
            }
            else if (resRow.class_name !== initStatus.workflowClassName) {
                const msg = `Workflow already exists with a different class name: ${resRow.class_name}, but the provided class name is: ${initStatus.workflowClassName}`;
                throw new error_1.DBOSConflictingWorkflowError(initStatus.workflowUUID, msg);
            }
            else if ((resRow.config_name || '') !== (initStatus.workflowConfigName || '')) {
                const msg = `Workflow already exists with a different class configuration: ${resRow.config_name}, but the provided class configuration is: ${initStatus.workflowConfigName}`;
                throw new error_1.DBOSConflictingWorkflowError(initStatus.workflowUUID, msg);
            }
            else if ((resRow.queue_name ?? undefined) !== (initStatus.queueName ?? undefined)) {
                // This is a warning because a different queue name is not necessarily an error.
                this.logger.warn(`Workflow (${initStatus.workflowUUID}) already exists in queue: ${resRow.queue_name}, but the provided queue name is: ${initStatus.queueName}. The queue is not updated. ${new Error().stack}`);
            }
            const status = resRow.status;
            const deadlineEpochMS = resRow.workflow_deadline_epoch_ms ?? undefined;
            // If there is an existing DB record and we aren't here to recover it,
            //  leave it be.  Roll back the change to max recovery attempts.
            if (ownerXid !== resRow.owner_xid && !options?.isRecoveryRequest && !options?.isDequeuedRequest) {
                // It is not clear if getting the handle should throw the error, or getting the result from the handle should error.
                //  Current precedent is the former.
                if (status === workflow_1.StatusString.MAX_RECOVERY_ATTEMPTS_EXCEEDED) {
                    throw new error_1.DBOSMaxRecoveryAttemptsExceededError(initStatus.workflowUUID, options?.maxRetries ?? -1);
                }
                return { status, deadlineEpochMS, shouldExecuteOnThisExecutor: false, serialization: resRow.serialization };
            }
            // Upsert above already set executor assignment and incremented the recovery attempt
            shouldCommit = true;
            // recovery_attempt means "attempts" (we kept the name for backward compatibility). It's default value is 1.
            // Every time we init the status, we increment `recovery_attempts` by 1.
            // Thus, when this number becomes equal to `maxRetries + 1`, we should mark the workflow as `MAX_RECOVERY_ATTEMPTS_EXCEEDED`.
            const attempts = resRow.recovery_attempts;
            if (options?.maxRetries && attempts > options?.maxRetries + 1) {
                await this.updateWorkflowStatus(client, initStatus.workflowUUID, workflow_1.StatusString.MAX_RECOVERY_ATTEMPTS_EXCEEDED, {
                    where: { status: workflow_1.StatusString.PENDING },
                    throwOnFailure: false,
                    update: { resetDeduplicationID: true },
                });
                throw new error_1.DBOSMaxRecoveryAttemptsExceededError(initStatus.workflowUUID, options.maxRetries);
            }
            this.logger.debug(`Workflow ${initStatus.workflowUUID} attempt number: ${attempts}.`);
            // Enqueue-transition wake: only for a fresh ENQUEUED row on a queue (not DELAYED, which
            // transitionDelayedWorkflows wakes when due, nor a PENDING dequeue). One wake per start.
            if (initStatus.status === workflow_1.StatusString.ENQUEUED && initStatus.queueName) {
                enqueueWakeQueue = initStatus.queueName;
            }
            return {
                status,
                deadlineEpochMS,
                shouldExecuteOnThisExecutor: true,
                serialization: resRow.serialization,
            };
        }
        finally {
            try {
                if (shouldCommit) {
                    await client.query('COMMIT');
                    // Wake the queue scheduler now that the ENQUEUED row is committed and visible.
                    if (enqueueWakeQueue !== undefined) {
                        this.#signalWake(exports.DBOS_QUEUE_WAKEUP_CHANNEL, enqueueWakeQueue);
                    }
                    await (0, debugpoint_1.debugTriggerPoint)(debugpoint_1.DEBUG_TRIGGER_INITWF_COMMIT);
                }
                else {
                    await client.query('ROLLBACK');
                }
            }
            finally {
                client.release();
            }
        }
    }
    /** Highest created_at among still-active rows per partition key, used to seed the in-memory cursor. */
    async #maxPartitionKeyCreatedAt(keys) {
        const maxima = new Map();
        if (keys.length === 0)
            return maxima;
        const { rows } = await this.pool.query(`SELECT queue_partition_key, MAX(created_at) AS max_created_at
       FROM "${this.schemaName}".workflow_status
       WHERE queue_partition_key = ANY($1) AND status = ANY($2)
       GROUP BY queue_partition_key`, [keys, [workflow_1.StatusString.ENQUEUED, workflow_1.StatusString.PENDING]]);
        for (const row of rows) {
            if (row.max_created_at !== null) {
                maxima.set(row.queue_partition_key, Number(row.max_created_at));
            }
        }
        return maxima;
    }
    /**
     * Stamp created_at monotonic within each partition key so per-key order holds across batches.
     * Unordered rows (no partition key) get wall-clock time and never touch the cursors.
     */
    async #assignBatchCreatedAt(statuses) {
        const nowMS = Date.now();
        const batchKeys = new Set();
        for (const status of statuses) {
            if (status.queuePartitionKey !== undefined) {
                batchKeys.add(status.queuePartitionKey);
            }
        }
        // On first sight of a key, seed its cursor from the DB high-water mark so per-key order
        // survives a restart or rebalance instead of resetting to wall-clock.
        const unseen = Array.from(batchKeys).filter((key) => !this.#batchCreatedAtCursors.has(key));
        const seeds = await this.#maxPartitionKeyCreatedAt(unseen);
        // Synchronous from here, so a concurrent batch cannot interleave with these cursor updates.
        for (const [key, seededMax] of seeds) {
            // max() guards against a concurrent batch that already advanced this key.
            this.#batchCreatedAtCursors.set(key, Math.max(this.#batchCreatedAtCursors.get(key) ?? 0, seededMax + 1));
        }
        const createdAts = [];
        const nextForKey = new Map();
        for (const status of statuses) {
            const key = status.queuePartitionKey;
            if (key === undefined) {
                createdAts.push(nowMS);
                continue;
            }
            const value = nextForKey.get(key) ?? Math.max(nowMS, this.#batchCreatedAtCursors.get(key) ?? 0);
            createdAts.push(value);
            nextForKey.set(key, value + 1);
        }
        for (const [key, next] of nextForKey) {
            this.#batchCreatedAtCursors.set(key, next);
        }
        return createdAts;
    }
    /**
     * Batch-insert ENQUEUED workflow status rows in a single transaction.
     *
     * Rows whose workflow_uuid already exists are skipped rather than updated, making this
     * idempotent under redelivery (e.g. Kafka). Returns the IDs of the rows actually inserted.
     *
     * Deliberately not `@dbRetry()`-decorated, unlike its neighbours: that loop is unabortable, so a
     * connection outage would trap the caller in it rather than let it back off and observe a
     * shutdown. Callers retry this themselves.
     */
    async enqueueWorkflows(statuses) {
        const inserted = new Set();
        if (statuses.length === 0)
            return inserted;
        for (const status of statuses) {
            if (status.status !== workflow_1.StatusString.ENQUEUED) {
                throw new error_1.DBOSError(`enqueueWorkflows only accepts ${workflow_1.StatusString.ENQUEUED} workflows, but ${status.workflowUUID} is ${status.status}`);
            }
            if (status.deduplicationID !== undefined) {
                throw new error_1.DBOSError(`enqueueWorkflows does not support deduplication IDs, but ${status.workflowUUID} has one`);
            }
        }
        const createdAts = await this.#assignBatchCreatedAt(statuses);
        const columns = [
            'workflow_uuid',
            'status',
            'name',
            'class_name',
            'config_name',
            'queue_name',
            'authenticated_user',
            'assumed_role',
            'authenticated_roles',
            'request',
            'executor_id',
            'application_version',
            'application_id',
            'created_at',
            'recovery_attempts',
            'updated_at',
            'workflow_timeout_ms',
            'workflow_deadline_epoch_ms',
            'inputs',
            'deduplication_id',
            'priority',
            'queue_partition_key',
            'parent_workflow_id',
            'serialization',
            'owner_xid',
            'delay_until_epoch_ms',
            'attributes',
            'schedule_name',
        ];
        const client = await this.pool.connect();
        try {
            await client.query('BEGIN ISOLATION LEVEL READ COMMITTED');
            // Chunk to stay well under the bind-parameter limit.
            const chunkSize = 500;
            for (let start = 0; start < statuses.length; start += chunkSize) {
                const chunk = statuses.slice(start, start + chunkSize);
                const tuples = [];
                const params = [];
                let paramIdx = 1;
                for (let i = 0; i < chunk.length; i++) {
                    const status = chunk[i];
                    const createdAt = createdAts[start + i];
                    tuples.push(`(${columns.map(() => `$${paramIdx++}`).join(', ')})`);
                    params.push(status.workflowUUID, status.status, status.workflowName, 
                    // For cross-language compatibility, these MUST be NULL in the database when not set
                    status.workflowClassName === '' ? null : status.workflowClassName, status.workflowConfigName === '' ? null : status.workflowConfigName, status.queueName ?? null, status.authenticatedUser, status.assumedRole, JSON.stringify(status.authenticatedRoles), JSON.stringify(status.request), status.executorId, status.applicationVersion ?? null, status.applicationID, createdAt, 0, createdAt, status.timeoutMS ?? null, status.deadlineEpochMS ?? null, status.input, null, status.priority, status.queuePartitionKey ?? null, status.parentWorkflowID ?? null, status.serialization, null, status.delayUntilEpochMS ?? null, status.attributes ? JSON.stringify(status.attributes) : null, status.scheduleName ?? null);
                }
                const { rows } = await client.query(`INSERT INTO "${this.schemaName}".workflow_status (${columns.join(', ')})
           VALUES ${tuples.join(', ')}
           ON CONFLICT (workflow_uuid) DO NOTHING
           RETURNING workflow_uuid`, params);
                for (const row of rows) {
                    inserted.add(row.workflow_uuid);
                }
            }
            await client.query('COMMIT');
        }
        catch (e) {
            await client.query('ROLLBACK');
            throw e;
        }
        finally {
            client.release();
        }
        // Enqueue-transition wake, one per distinct queue actually inserted (coalesced downstream), so a
        // batch of N onto one queue collapses to a single NOTIFY rather than N.
        const wokenQueues = new Set();
        for (const status of statuses) {
            if (inserted.has(status.workflowUUID) && status.queueName) {
                wokenQueues.add(status.queueName);
            }
        }
        for (const queueName of wokenQueues) {
            this.#signalWake(exports.DBOS_QUEUE_WAKEUP_CHANNEL, queueName);
        }
        return inserted;
    }
    async recordWorkflowOutput(workflowID, status) {
        const client = await this.pool.connect();
        try {
            await this.#recordWorkflowOutcome(client, workflowID, workflow_1.StatusString.SUCCESS, { output: status.output });
        }
        finally {
            client.release();
        }
    }
    async recordWorkflowError(workflowID, status) {
        const client = await this.pool.connect();
        try {
            await this.#recordWorkflowOutcome(client, workflowID, workflow_1.StatusString.ERROR, { error: status.error });
        }
        finally {
            client.release();
        }
    }
    // Record a workflow's terminal outcome (SUCCESS or ERROR), but never overwrite
    // the terminal CANCELLED status: a workflow can be cancelled during its final
    // step, and if so it must not be able to subsequently complete. If the
    // workflow is cancelled, abort the function so it does not complete. This
    // mirrors the cancellation check done before each step.
    async #recordWorkflowOutcome(client, workflowID, status, outcome) {
        let cancelled = false;
        try {
            await client.query('BEGIN');
            await this.updateWorkflowStatus(client, workflowID, status, {
                update: { ...outcome, resetDeduplicationID: true, setCompletedAt: true },
                where: { notStatus: workflow_1.StatusString.CANCELLED },
                throwOnFailure: false,
            });
            cancelled = (await this.getWorkflowStatusValue(client, workflowID)) === workflow_1.StatusString.CANCELLED;
            await client.query('COMMIT');
        }
        catch (e) {
            await client.query('ROLLBACK');
            throw e;
        }
        if (cancelled) {
            throw new error_1.DBOSWorkflowCancelledError(workflowID);
        }
    }
    async getPendingWorkflows(executorID, appVersion) {
        const getWorkflows = await this.pool.query(`SELECT workflow_uuid, queue_name 
       FROM "${this.schemaName}".workflow_status 
       WHERE status=$1 AND executor_id=$2 AND application_version=$3`, [workflow_1.StatusString.PENDING, executorID, appVersion]);
        return getWorkflows.rows.map((i) => ({
            workflowUUID: i.workflow_uuid,
            queueName: i.queue_name,
        }));
    }
    async reenqueuePendingQueuedWorkflows(executorID, appVersion) {
        const result = await this.pool.query(`UPDATE "${this.schemaName}".workflow_status
       SET started_at_epoch_ms = NULL,
           status = $1
       WHERE status = $2
         AND executor_id = $3
         AND application_version = $4
         AND queue_name IS NOT NULL
       RETURNING workflow_uuid`, [workflow_1.StatusString.ENQUEUED, workflow_1.StatusString.PENDING, executorID, appVersion]);
        return result.rows.map((row) => row.workflow_uuid);
    }
    async getWorkflowStatus(workflowID, callerID, callerFN) {
        const funcGetStatus = async () => {
            const statuses = await this.listWorkflows({ workflowIDs: [workflowID] });
            const status = statuses.find((s) => s.workflowUUID === workflowID);
            return status ? JSON.stringify(status) : null;
        };
        if (callerID && callerFN) {
            const client = await this.pool.connect();
            try {
                // Check if the operation has been done before for OAOO (only do this inside a workflow).
                const json = await this.#runAndRecordResult(client, exports.DBOS_FUNCNAME_GETSTATUS, callerID, callerFN, funcGetStatus);
                return parseStatus(json);
            }
            finally {
                client.release();
            }
        }
        else {
            const json = await funcGetStatus();
            return parseStatus(json);
        }
        function parseStatus(json) {
            return json ? JSON.parse(json) : null;
        }
    }
    // Only used in tests
    async setWorkflowStatus(workflowID, status, resetRecoveryAttempts, internalOptions) {
        const client = await this.pool.connect();
        try {
            await this.updateWorkflowStatus(client, workflowID, status, {
                update: { resetRecoveryAttempts, resetNameTo: internalOptions?.updateName },
            });
        }
        finally {
            client.release();
        }
    }
    // ==================== Step Results ====================
    async getOperationResultAndThrowIfCancelled(workflowID, functionID) {
        const client = await this.pool.connect();
        try {
            return await this.#getOperationResultAndThrowIfCancelled(client, workflowID, functionID);
        }
        finally {
            client.release();
        }
    }
    async getAllOperationResults(workflowID, limit, offset) {
        let query = `SELECT * FROM "${this.schemaName}".operation_outputs WHERE workflow_uuid=$1 ORDER BY function_id`;
        const params = [workflowID];
        if (limit !== undefined) {
            params.push(limit);
            query += ` LIMIT $${params.length}`;
        }
        if (offset !== undefined) {
            params.push(offset);
            query += ` OFFSET $${params.length}`;
        }
        const { rows } = await this.pool.query(query, params);
        return rows;
    }
    async recordOperationResult(workflowID, functionID, functionName, checkConflict, startTimeEpochMs, endTimeEpochMs, options = {}) {
        const client = await this.pool.connect();
        try {
            await this.recordOperationResultInternal(client, workflowID, functionID, functionName, checkConflict, startTimeEpochMs, endTimeEpochMs, options);
        }
        finally {
            client.release();
            await (0, debugpoint_1.debugTriggerPoint)(debugpoint_1.DEBUG_TRIGGER_STEP_COMMIT);
        }
    }
    async runTransactionalStep(workflowID, functionID, functionName, callback) {
        const client = await this.pool.connect();
        try {
            await client.query('BEGIN ISOLATION LEVEL READ COMMITTED');
            const existing = await this.#getOperationResultAndThrowIfCancelled(client, workflowID, functionID);
            if (existing !== undefined) {
                await client.query('ROLLBACK');
                return existing;
            }
            const startTime = Date.now();
            const output = await callback(client);
            await this.recordOperationResultInternal(client, workflowID, functionID, functionName, true, startTime, Date.now(), {
                output,
            });
            await client.query('COMMIT');
            await (0, debugpoint_1.debugTriggerPoint)(debugpoint_1.DEBUG_TRIGGER_STEP_COMMIT);
            return undefined;
        }
        catch (e) {
            await client.query('ROLLBACK');
            throw e;
        }
        finally {
            client.release();
        }
    }
    async checkPatch(workflowID, functionID, patchName, deprecated) {
        // Not doing a cancel check at this point.
        if (functionID === undefined)
            throw new TypeError('functionID must be defined');
        patchName = `DBOS.patch-${patchName}`;
        const { rows } = await this.pool.query(`SELECT function_name
       FROM "${this.schemaName}".operation_outputs
      WHERE workflow_uuid=$1 AND function_id=$2`, [workflowID, functionID]);
        if (deprecated) {
            // Deprecated does not write anything.  We skip any existing matching patch marker if it matches
            if (rows.length === 0) {
                return { isPatched: true, hasEntry: false };
            }
            return { isPatched: true, hasEntry: rows[0].function_name === patchName };
        }
        // Nondeprecated - skip matching entry, unpatched if nonmatching entry,
        //  If there is no entry, we insert one that indicates it is patched.
        if (rows.length !== 0) {
            if (rows[0].function_name === patchName) {
                return { isPatched: true, hasEntry: true };
            }
            return { isPatched: false, hasEntry: false };
        }
        // Insert a patchmarker
        const dn = Date.now();
        await this.pool.query(`INSERT INTO ${this.schemaName}.operation_outputs
       (workflow_uuid, function_id, output, error, function_name, child_workflow_id, started_at_epoch_ms, completed_at_epoch_ms)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
       ON CONFLICT DO NOTHING;`, [workflowID, functionID, null, null, patchName, null, dn, dn]);
        return { isPatched: true, hasEntry: true };
    }
    // ==================== Workflow Management ====================
    async cancelWorkflows(workflowIDs, cancelChildren = false) {
        if (!cancelChildren) {
            await this.#cancelWorkflows(workflowIDs);
            return;
        }
        // Cascade cancellation to child workflows level by level.
        const visited = new Set(workflowIDs);
        let frontier = workflowIDs;
        while (frontier.length > 0) {
            await this.#cancelWorkflows(frontier);
            const children = await this.#getDirectChildren(frontier);
            frontier = children.filter((id) => !visited.has(id));
            for (const id of frontier) {
                visited.add(id);
            }
        }
    }
    async #cancelWorkflows(workflowIDs) {
        const res = await this.pool.query(`UPDATE "${this.schemaName}".workflow_status
       SET status = $1, queue_name = NULL, deduplication_id = NULL, started_at_epoch_ms = NULL,
           updated_at = (EXTRACT(EPOCH FROM now()) * 1000)::bigint,
           completed_at = (EXTRACT(EPOCH FROM now()) * 1000)::bigint
       WHERE workflow_uuid = ANY($2)
         AND status NOT IN ($3, $4)
       RETURNING workflow_uuid`, [workflow_1.StatusString.CANCELLED, workflowIDs, workflow_1.StatusString.SUCCESS, workflow_1.StatusString.ERROR]);
        // Completion wake: CANCELLED is terminal, and this bulk path bypasses updateWorkflowStatus,
        // so it wakes getResult() waiters itself — one per row that actually transitioned.
        for (const row of res.rows) {
            this.#signalWake(exports.DBOS_WORKFLOW_COMPLETION_CHANNEL, row.workflow_uuid);
        }
    }
    async checkIfCanceled(workflowID) {
        await this.#checkIfCanceled(this.pool, workflowID);
    }
    async resumeWorkflows(workflowIDs, queueName) {
        const targetQueue = queueName ?? utils_1.INTERNAL_QUEUE_NAME;
        const res = await this.pool.query(`UPDATE "${this.schemaName}".workflow_status
       SET status = $1, queue_name = $2, recovery_attempts = 0,
           workflow_deadline_epoch_ms = NULL, deduplication_id = NULL,
           started_at_epoch_ms = NULL,
           updated_at = (EXTRACT(EPOCH FROM now()) * 1000)::bigint,
           completed_at = NULL
       WHERE workflow_uuid = ANY($3)
         AND status NOT IN ($4, $5)
       RETURNING workflow_uuid`, [workflow_1.StatusString.ENQUEUED, targetQueue, workflowIDs, workflow_1.StatusString.SUCCESS, workflow_1.StatusString.ERROR]);
        // Resume re-enqueues onto a single queue; one wake covers the whole batch.
        if ((res.rowCount ?? 0) > 0) {
            this.#signalWake(exports.DBOS_QUEUE_WAKEUP_CHANNEL, targetQueue);
        }
    }
    async setWorkflowPriority(workflowID, priority) {
        await this.pool.query(`UPDATE "${this.schemaName}".workflow_status
       SET priority = $1, updated_at = $2
       WHERE workflow_uuid = $3
         AND status IN ($4, $5)`, [priority, Date.now(), workflowID, workflow_1.StatusString.ENQUEUED, workflow_1.StatusString.DELAYED]);
    }
    async setWorkflowDelay(workflowID, delayUntilEpochMS) {
        await this.pool.query(`UPDATE "${this.schemaName}".workflow_status
       SET delay_until_epoch_ms = $1, updated_at = $2
       WHERE workflow_uuid = $3
         AND status = $4`, [delayUntilEpochMS, Date.now(), workflowID, workflow_1.StatusString.DELAYED]);
    }
    /**
     * Extend an existing debounced DELAYED workflow's delay and update its inputs, atomically.
     * The new delay is capped at the workflow's debounce_deadline_epoch_ms, if one is set.
     * Matching on workflow name and class ensures a debounce-key collision between different
     * workflows never overwrites another workflow's inputs. If nothing matched, returns the
     * current holder (or that the key is unheld) so the caller can start fresh or surface a conflict.
     * Runs on `client` if given, joining its transaction (e.g. a transactional step's);
     * otherwise in its own retried transaction.
     */
    async debounceDelayedWorkflow(params, client) {
        if (client !== undefined) {
            return await this.#debounceDelayedWorkflowInternal(client, params);
        }
        return await this.debounceDelayedWorkflowStandalone(params);
    }
    async debounceDelayedWorkflowStandalone(params) {
        const client = await this.pool.connect();
        try {
            await client.query('BEGIN ISOLATION LEVEL READ COMMITTED');
            const result = await this.#debounceDelayedWorkflowInternal(client, params);
            await client.query('COMMIT');
            return result;
        }
        catch (e) {
            await client.query('ROLLBACK');
            throw e;
        }
        finally {
            client.release();
        }
    }
    async #debounceDelayedWorkflowInternal(client, params) {
        const classNameOrNull = params.workflowClassName === '' ? null : params.workflowClassName;
        const updated = await client.query(`UPDATE "${this.schemaName}".workflow_status
       SET delay_until_epoch_ms = CASE
             WHEN debounce_deadline_epoch_ms IS NOT NULL AND debounce_deadline_epoch_ms < $1
             THEN debounce_deadline_epoch_ms
             ELSE $1
           END,
           inputs = $2, serialization = $3, updated_at = $4
       WHERE name = $5 AND class_name IS NOT DISTINCT FROM $6
         AND queue_name = $7 AND deduplication_id = $8
         AND status = $9 AND is_debounced = TRUE
       RETURNING workflow_uuid`, [
            params.delayUntilEpochMS,
            params.input,
            params.serialization,
            Date.now(),
            params.workflowName,
            classNameOrNull,
            params.queueName,
            params.deduplicationID,
            workflow_1.StatusString.DELAYED,
        ]);
        if (updated.rows.length > 0) {
            return {
                bouncedWorkflowID: updated.rows[0].workflow_uuid,
                holderWorkflowID: null,
                holderIsDebounced: false,
                holderWorkflowName: null,
                holderWorkflowClassName: null,
            };
        }
        // No match: the key is unheld, or held by a non-debounced or name-colliding workflow.
        const holder = await client.query(`SELECT workflow_uuid, is_debounced, name, class_name
       FROM "${this.schemaName}".workflow_status
       WHERE queue_name = $1 AND deduplication_id = $2`, [params.queueName, params.deduplicationID]);
        if (holder.rows.length === 0) {
            return {
                bouncedWorkflowID: null,
                holderWorkflowID: null,
                holderIsDebounced: false,
                holderWorkflowName: null,
                holderWorkflowClassName: null,
            };
        }
        return {
            bouncedWorkflowID: null,
            holderWorkflowID: holder.rows[0].workflow_uuid,
            holderIsDebounced: holder.rows[0].is_debounced ?? false,
            holderWorkflowName: holder.rows[0].name,
            holderWorkflowClassName: holder.rows[0].class_name ?? null,
        };
    }
    // Get the immediate (one-level) child workflow IDs for a set of workflows.
    async #getDirectChildren(workflowIDs) {
        if (workflowIDs.length === 0) {
            return [];
        }
        const result = await this.pool.query(`SELECT workflow_uuid
       FROM "${this.schemaName}".workflow_status
       WHERE parent_workflow_id = ANY($1)`, [workflowIDs]);
        return result.rows.map((row) => row.workflow_uuid);
    }
    async getWorkflowChildren(workflowID) {
        // BFS to find all descendant workflows
        const descendants = new Set();
        let frontier = [workflowID];
        while (frontier.length > 0) {
            const children = await this.#getDirectChildren(frontier);
            frontier = children.filter((id) => !descendants.has(id));
            for (const id of frontier) {
                descendants.add(id);
            }
        }
        return [...descendants];
    }
    async deleteWorkflows(workflowIDs, deleteChildren = false) {
        const allIds = [...workflowIDs];
        if (deleteChildren) {
            for (const wfid of workflowIDs) {
                allIds.push(...(await this.getWorkflowChildren(wfid)));
            }
        }
        await this.pool.query(`DELETE FROM "${this.schemaName}".workflow_status WHERE workflow_uuid = ANY($1)`, [allIds]);
        for (const wfid of allIds) {
            this.runningWorkflowMap.delete(wfid);
        }
    }
    async forkWorkflow(workflowID, startStep, options = {}) {
        const newWorkflowID = options.newWorkflowID ?? (0, crypto_1.randomUUID)();
        const result = await this.bulkForkWorkflows([workflowID], [newWorkflowID], [startStep], options);
        return result[0];
    }
    async forkFromFailure(workflowIDs, options = {}) {
        const modes = [
            options.fromLastFailure ?? false,
            options.fromLastStep ?? false,
            options.fromStep !== undefined,
            options.fromStepName !== undefined,
        ].filter(Boolean).length;
        if (modes !== 1) {
            throw new Error('Exactly one of fromLastFailure, fromLastStep, fromStep, or fromStepName must be specified');
        }
        let startSteps;
        if (options.fromStep !== undefined) {
            startSteps = Array(workflowIDs.length).fill(options.fromStep);
        }
        else {
            let query;
            const params = [workflowIDs];
            if (options.fromLastFailure) {
                query = `SELECT workflow_uuid,
                        COALESCE(
                          MAX(function_id) FILTER (WHERE error IS NOT NULL),
                          MAX(function_id)
                        ) AS start_step
                 FROM "${this.schemaName}".operation_outputs
                 WHERE workflow_uuid = ANY($1)
                 GROUP BY workflow_uuid`;
            }
            else if (options.fromLastStep) {
                query = `SELECT workflow_uuid, MAX(function_id) AS start_step
                 FROM "${this.schemaName}".operation_outputs
                 WHERE workflow_uuid = ANY($1)
                 GROUP BY workflow_uuid`;
            }
            else {
                // fromStepName
                query = `SELECT workflow_uuid, MAX(function_id) AS start_step
                 FROM "${this.schemaName}".operation_outputs
                 WHERE workflow_uuid = ANY($1) AND function_name = $2
                 GROUP BY workflow_uuid`;
                params.push(options.fromStepName);
            }
            const result = await this.pool.query(query, params);
            const startStepByID = new Map(result.rows.map((r) => [r.workflow_uuid, Number(r.start_step)]));
            for (const wid of workflowIDs) {
                if (!startStepByID.has(wid)) {
                    if (options.fromStepName !== undefined) {
                        throw new Error(`Workflow ${wid} has no step named '${options.fromStepName}'`);
                    }
                    throw new Error(`Workflow ${wid} has no steps`);
                }
            }
            startSteps = workflowIDs.map((wid) => startStepByID.get(wid));
        }
        const forkedIDs = workflowIDs.map(() => (0, crypto_1.randomUUID)());
        return this.bulkForkWorkflows(workflowIDs, forkedIDs, startSteps, options);
    }
    async bulkForkWorkflows(originalWorkflowIDs, forkedWorkflowIDs, startSteps, options = {}) {
        if (originalWorkflowIDs.length === 0) {
            return [];
        }
        if (originalWorkflowIDs.length !== forkedWorkflowIDs.length || originalWorkflowIDs.length !== startSteps.length) {
            throw new Error('originalWorkflowIDs, forkedWorkflowIDs, and startSteps must have the same length');
        }
        const client = await this.pool.connect();
        try {
            await client.query('BEGIN ISOLATION LEVEL READ COMMITTED');
            // Fetch the status of all original workflows inside the transaction.
            const { rows: statusRows } = await client.query(`SELECT workflow_uuid, name, class_name, config_name, application_id,
                authenticated_user, authenticated_roles, assumed_role, inputs, serialization,
                request, application_version, attributes
         FROM "${this.schemaName}".workflow_status
         WHERE workflow_uuid = ANY($1)`, [originalWorkflowIDs]);
            const statusByID = new Map(statusRows.map((r) => [r.workflow_uuid, r]));
            for (const wid of originalWorkflowIDs) {
                if (!statusByID.has(wid)) {
                    throw new error_1.DBOSNonExistentWorkflowError(`Workflow ${wid} does not exist`);
                }
            }
            const queueName = options.queueName ?? utils_1.INTERNAL_QUEUE_NAME;
            // Bulk insert all forked workflow status rows.
            const insertCols = [
                'workflow_uuid',
                'status',
                'name',
                'class_name',
                'config_name',
                'queue_name',
                'authenticated_user',
                'assumed_role',
                'authenticated_roles',
                'request',
                'application_version',
                'application_id',
                'inputs',
                'queue_partition_key',
                'forked_from',
                'serialization',
                'attributes',
            ];
            if (options.timeoutMS !== undefined) {
                insertCols.push('workflow_timeout_ms');
            }
            const valuesPlaceholders = [];
            const params = [];
            let paramIdx = 1;
            for (let i = 0; i < originalWorkflowIDs.length; i++) {
                const origID = originalWorkflowIDs[i];
                const forkID = forkedWorkflowIDs[i];
                const ws = statusByID.get(origID);
                const placeholders = insertCols.map(() => `$${paramIdx++}`).join(', ');
                valuesPlaceholders.push(`(${placeholders})`);
                params.push(forkID, workflow_1.StatusString.ENQUEUED, ws.name, ws.class_name ?? null, ws.config_name ?? null, queueName, ws.authenticated_user, ws.assumed_role, ws.authenticated_roles, ws.request, options.applicationVersion ?? ws.application_version ?? null, ws.application_id, ws.inputs, options.queuePartitionKey ?? null, origID, ws.serialization, ws.attributes ? JSON.stringify(ws.attributes) : null);
                if (options.timeoutMS !== undefined) {
                    params.push(options.timeoutMS);
                }
            }
            await client.query(`INSERT INTO "${this.schemaName}".workflow_status (${insertCols.join(', ')})
         VALUES ${valuesPlaceholders.join(', ')}`, params);
            // Mark all original workflows as having been forked from.
            await client.query(`UPDATE "${this.schemaName}".workflow_status SET was_forked_from = TRUE WHERE workflow_uuid = ANY($1)`, [originalWorkflowIDs]);
            // For workflows with start_step > 0, copy checkpoints/events/streams.
            // Build a mapping CTE so each copy is a single SQL statement.
            const forkMappings = originalWorkflowIDs
                .map((origID, i) => ({ origID, forkID: forkedWorkflowIDs[i], startStep: startSteps[i] }))
                .filter((m) => m.startStep > 0);
            if (forkMappings.length > 0) {
                const mappingValues = [];
                const mappingParams = [];
                let mIdx = 1;
                for (const m of forkMappings) {
                    mappingValues.push(`($${mIdx}::text, $${mIdx + 1}::text, $${mIdx + 2}::int)`);
                    mappingParams.push(m.origID, m.forkID, m.startStep);
                    mIdx += 3;
                }
                const mappingCTE = `WITH mapping(orig_id, fork_id, start_step) AS (VALUES ${mappingValues.join(', ')})`;
                // Build the child_workflow_id expression, applying replacements if provided.
                let childWfExpr = 'oo.child_workflow_id';
                const ooParams = [...mappingParams];
                if (options.replacementChildren && Object.keys(options.replacementChildren).length > 0) {
                    const whenClauses = [];
                    for (const [oldId, newId] of Object.entries(options.replacementChildren)) {
                        whenClauses.push(`WHEN oo.child_workflow_id = $${mIdx} THEN $${mIdx + 1}::text`);
                        ooParams.push(oldId, newId);
                        mIdx += 2;
                    }
                    childWfExpr = `CASE ${whenClauses.join(' ')} ELSE oo.child_workflow_id END`;
                }
                // Copy operation outputs
                await client.query(`${mappingCTE}
           INSERT INTO "${this.schemaName}".operation_outputs
             (workflow_uuid, function_id, output, error, serialization, function_name, child_workflow_id, started_at_epoch_ms, completed_at_epoch_ms)
           SELECT m.fork_id, oo.function_id, oo.output, oo.error, oo.serialization, oo.function_name, ${childWfExpr}, oo.started_at_epoch_ms, oo.completed_at_epoch_ms
           FROM mapping m
           JOIN "${this.schemaName}".operation_outputs oo
             ON oo.workflow_uuid = m.orig_id AND oo.function_id < m.start_step`, ooParams);
                // Copy streams
                await client.query(`${mappingCTE}
           INSERT INTO "${this.schemaName}".streams
             (workflow_uuid, key, value, serialization, "offset", function_id)
           SELECT m.fork_id, s.key, s.value, s.serialization, s."offset", s.function_id
           FROM mapping m
           JOIN "${this.schemaName}".streams s
             ON s.workflow_uuid = m.orig_id AND s.function_id < m.start_step`, mappingParams);
                // Copy events history
                await client.query(`${mappingCTE}
           INSERT INTO "${this.schemaName}".workflow_events_history
             (workflow_uuid, function_id, key, value, serialization)
           SELECT m.fork_id, weh.function_id, weh.key, weh.value, weh.serialization
           FROM mapping m
           JOIN "${this.schemaName}".workflow_events_history weh
             ON weh.workflow_uuid = m.orig_id AND weh.function_id < m.start_step`, mappingParams);
                // Copy only the latest version of each event using a window function
                await client.query(`${mappingCTE}
           INSERT INTO "${this.schemaName}".workflow_events
             (workflow_uuid, key, value, serialization)
           SELECT ranked.workflow_uuid, ranked.key, ranked.value, ranked.serialization
           FROM (
             SELECT m.fork_id AS workflow_uuid, weh.key, weh.value, weh.serialization,
                    ROW_NUMBER() OVER (PARTITION BY weh.workflow_uuid, weh.key ORDER BY weh.function_id DESC) AS rn
             FROM mapping m
             JOIN "${this.schemaName}".workflow_events_history weh
               ON weh.workflow_uuid = m.orig_id AND weh.function_id < m.start_step
           ) ranked
           WHERE ranked.rn = 1`, mappingParams);
            }
            await client.query('COMMIT');
            return forkedWorkflowIDs;
        }
        catch (error) {
            await client.query('ROLLBACK');
            throw error;
        }
        finally {
            client.release();
        }
    }
    async exportWorkflow(workflowID, exportChildren = false) {
        const workflowIDs = [workflowID];
        if (exportChildren) {
            workflowIDs.push(...(await this.getWorkflowChildren(workflowID)));
        }
        const exportedWorkflows = [];
        const client = await this.pool.connect();
        try {
            for (const wfID of workflowIDs) {
                // Export workflow_status
                const statusResult = await client.query(
                // owner_xid is intentionally omitted: it is a transient transaction-ownership
                // token, not logical workflow state, and a source database's xid is
                // meaningless in the target.
                `SELECT
            workflow_uuid, status, name, authenticated_user, assumed_role,
            authenticated_roles, request, output, error, executor_id,
            created_at, updated_at, application_version, application_id,
            class_name, config_name, recovery_attempts, queue_name,
            workflow_timeout_ms, workflow_deadline_epoch_ms, started_at_epoch_ms,
            deduplication_id, inputs, priority, queue_partition_key, forked_from,
            parent_workflow_id, serialization, delay_until_epoch_ms,
            was_forked_from, rate_limited, completed_at, attributes, schedule_name,
            debounce_deadline_epoch_ms, is_debounced
          FROM "${this.schemaName}".workflow_status
          WHERE workflow_uuid = $1`, [wfID]);
                if (statusResult.rows.length === 0) {
                    throw new error_1.DBOSNonExistentWorkflowError(`Workflow ${wfID} does not exist`);
                }
                const workflowStatus = statusResult.rows[0];
                // Export operation_outputs
                const outputsResult = await client.query(`SELECT
            workflow_uuid, function_id, function_name, output, error,
            child_workflow_id, started_at_epoch_ms, completed_at_epoch_ms,
            serialization
          FROM "${this.schemaName}".operation_outputs
          WHERE workflow_uuid = $1`, [wfID]);
                // Export workflow_events
                const eventsResult = await client.query(`SELECT workflow_uuid, key, value, serialization
          FROM "${this.schemaName}".workflow_events
          WHERE workflow_uuid = $1`, [wfID]);
                // Export workflow_events_history
                const historyResult = await client.query(`SELECT workflow_uuid, function_id, key, value, serialization
          FROM "${this.schemaName}".workflow_events_history
          WHERE workflow_uuid = $1`, [wfID]);
                // Export streams
                const streamsResult = await client.query(`SELECT workflow_uuid, key, value, "offset", function_id, serialization
          FROM "${this.schemaName}".streams
          WHERE workflow_uuid = $1`, [wfID]);
                exportedWorkflows.push({
                    workflow_status: workflowStatus,
                    operation_outputs: outputsResult.rows,
                    workflow_events: eventsResult.rows,
                    workflow_events_history: historyResult.rows,
                    streams: streamsResult.rows,
                });
            }
        }
        finally {
            client.release();
        }
        return exportedWorkflows;
    }
    async importWorkflow(workflows) {
        const client = await this.pool.connect();
        try {
            await client.query('BEGIN');
            for (const workflow of workflows) {
                const status = workflow.workflow_status;
                // Import workflow_status
                await client.query(`INSERT INTO "${this.schemaName}".workflow_status (
            workflow_uuid, status, name, authenticated_user, assumed_role,
            authenticated_roles, request, output, error, executor_id,
            created_at, updated_at, application_version, application_id,
            class_name, config_name, recovery_attempts, queue_name,
            workflow_timeout_ms, workflow_deadline_epoch_ms, started_at_epoch_ms,
            deduplication_id, inputs, priority, queue_partition_key, forked_from,
            parent_workflow_id, serialization, delay_until_epoch_ms,
            was_forked_from, rate_limited, completed_at, attributes, schedule_name,
            debounce_deadline_epoch_ms, is_debounced
          ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28, $29, $30, $31, $32, $33, $34, $35, $36)`, [
                    status.workflow_uuid,
                    status.status,
                    status.name,
                    status.authenticated_user,
                    status.assumed_role,
                    status.authenticated_roles,
                    status.request,
                    status.output,
                    status.error,
                    status.executor_id,
                    status.created_at,
                    status.updated_at,
                    status.application_version,
                    status.application_id,
                    status.class_name,
                    status.config_name,
                    status.recovery_attempts,
                    status.queue_name,
                    status.workflow_timeout_ms,
                    status.workflow_deadline_epoch_ms,
                    status.started_at_epoch_ms,
                    status.deduplication_id,
                    status.inputs,
                    status.priority,
                    status.queue_partition_key,
                    status.forked_from,
                    status.parent_workflow_id,
                    status.serialization,
                    status.delay_until_epoch_ms ?? null,
                    // NOT NULL columns: fall back to FALSE for payloads exported before
                    // these fields were included.
                    status.was_forked_from ?? false,
                    status.rate_limited ?? false,
                    status.completed_at ?? null,
                    status.attributes ? JSON.stringify(status.attributes) : null,
                    status.schedule_name ?? null,
                    status.debounce_deadline_epoch_ms ?? null,
                    status.is_debounced ?? false,
                ]);
                // Import operation_outputs
                for (const output of workflow.operation_outputs) {
                    await client.query(`INSERT INTO "${this.schemaName}".operation_outputs (
              workflow_uuid, function_id, function_name, output, error,
              child_workflow_id, started_at_epoch_ms, completed_at_epoch_ms,
              serialization
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`, [
                        output.workflow_uuid,
                        output.function_id,
                        output.function_name,
                        output.output,
                        output.error,
                        output.child_workflow_id,
                        output.started_at_epoch_ms,
                        output.completed_at_epoch_ms,
                        output.serialization,
                    ]);
                }
                // Import workflow_events
                for (const event of workflow.workflow_events) {
                    await client.query(`INSERT INTO "${this.schemaName}".workflow_events (
              workflow_uuid, key, value, serialization
            ) VALUES ($1, $2, $3, $4)`, [event.workflow_uuid, event.key, event.value, event.serialization]);
                }
                // Import workflow_events_history
                for (const history of workflow.workflow_events_history) {
                    await client.query(`INSERT INTO "${this.schemaName}".workflow_events_history (
              workflow_uuid, function_id, key, value, serialization
            ) VALUES ($1, $2, $3, $4, $5)`, [history.workflow_uuid, history.function_id, history.key, history.value, history.serialization]);
                }
                // Import streams
                for (const stream of workflow.streams) {
                    await client.query(`INSERT INTO "${this.schemaName}".streams (
              workflow_uuid, key, value, "offset", function_id, serialization
            ) VALUES ($1, $2, $3, $4, $5, $6)`, [stream.workflow_uuid, stream.key, stream.value, stream.offset, stream.function_id, stream.serialization]);
                }
            }
            await client.query('COMMIT');
        }
        catch (error) {
            await client.query('ROLLBACK');
            throw error;
        }
        finally {
            client.release();
        }
    }
    // ==================== Awaiting Workflows ====================
    registerRunningWorkflow(workflowID, workflowPromise, onSettled, queueName, queuePartitionKey) {
        // Need to await for the workflow and capture errors.
        const awaitWorkflowPromise = workflowPromise
            .catch((error) => {
            this.logger.debug('Captured error in awaitWorkflowPromise: ' + error);
        })
            .finally(() => {
            onSettled();
        });
        this.runningWorkflowMap.set(workflowID, {
            promise: awaitWorkflowPromise,
            queueName,
            queuePartitionKey,
        });
    }
    checkForRunningWorkflow(workflowID) {
        return this.runningWorkflowMap.has(workflowID);
    }
    clearRunningWorkflow(workflowID) {
        this.runningWorkflowMap.delete(workflowID);
    }
    countRunningWorkflowsForQueue(queueName, queuePartitionKey) {
        let count = 0;
        for (const entry of this.runningWorkflowMap.values()) {
            if (entry.queueName === queueName && entry.queuePartitionKey === queuePartitionKey)
                count++;
        }
        return count;
    }
    async awaitRunningWorkflows() {
        if (this.runningWorkflowMap.size > 0) {
            this.logger.info('Waiting for pending workflows to finish.');
            await Promise.allSettled(Array.from(this.runningWorkflowMap.values(), (entry) => entry.promise));
        }
        if (this.workflowEventsMap.map.size > 0) {
            this.logger.warn('Workflow events map is not empty - shutdown is not clean.');
            //throw new Error('Workflow events map is not empty - shutdown is not clean.');
        }
        if (this.notificationsMap.map.size > 0) {
            this.logger.warn('Message notification map is not empty - shutdown is not clean.');
            //throw new Error('Message notification map is not empty - shutdown is not clean.');
        }
    }
    /**
     * Run a DB-backed polling read under the polling concurrency limiter, so that
     * high-fan-out wait loops cannot check out every pool client and starve
     * control-plane operations. Only polling reads should go through here;
     * control-plane work hits the pool directly and bypasses the limiter.
     */
    #pollWithLimiter(query) {
        return this.pollLimiter.runExclusive(query);
    }
    /**
     * Cancellation check for use inside polling wait loops: the status read runs
     * under the polling limiter so it counts against the same concurrency budget
     * as the rest of the loop's reads.
     */
    async #checkIfCanceledLimited(workflowID) {
        await this.#pollWithLimiter(() => this.#checkIfCanceled(this.pool, workflowID));
    }
    async awaitWorkflowResult(workflowID, timeoutSeconds, callerID, timerFuncID, pollingIntervalMs) {
        const timeoutms = timeoutSeconds !== undefined ? timeoutSeconds * 1000 : undefined;
        let finishTime = timeoutms !== undefined ? Date.now() + timeoutms : undefined;
        const pollIntervalMs = pollingIntervalMs ?? this.dbPollingIntervalResultMs;
        // Record the durable timeout deadline once before polling. #durableSleep persists it on the
        // first call and reads back the same value on recovery, so it never changes across iterations.
        if (timerFuncID !== undefined && callerID !== undefined && timeoutms !== undefined) {
            finishTime = await this.#durableSleep(callerID, timerFuncID, timeoutms);
        }
        while (true) {
            // Register the completion wait BEFORE reading (mirroring recv/getEvent) so a terminal
            // transition landing between the SELECT and the wait still wakes this iteration. Undefined
            // when wakes are disabled — the loop then behaves exactly as the pre-existing poll loop.
            let resolveNotification;
            const completionPromise = new Promise((resolve) => {
                resolveNotification = resolve;
            });
            const cbr = this.wakeNotificationsEnabled
                ? this.completionMap.registerCallback(workflowID, resolveNotification)
                : undefined;
            try {
                if (callerID)
                    await this.#checkIfCanceledLimited(callerID);
                try {
                    const { rows } = await this.#pollWithLimiter(() => this.pool.query(`SELECT status, output, error, serialization FROM "${this.schemaName}".workflow_status
               WHERE workflow_uuid=$1`, [workflowID]));
                    if (rows.length > 0) {
                        const status = rows[0].status;
                        if (status === workflow_1.StatusString.SUCCESS) {
                            return { output: rows[0].output, serialization: rows[0].serialization };
                        }
                        else if (status === workflow_1.StatusString.ERROR) {
                            return { error: rows[0].error, serialization: rows[0].serialization };
                        }
                        else if (status === workflow_1.StatusString.CANCELLED) {
                            return { cancelled: true };
                        }
                        else if (status === workflow_1.StatusString.MAX_RECOVERY_ATTEMPTS_EXCEEDED) {
                            return { maxRecoveryAttemptsExceeded: true };
                        }
                        else {
                            // Status is not actionable
                        }
                    }
                }
                catch (e) {
                    const err = e;
                    this.logger.error(`Exception from system database: ${err}`, err);
                    throw err;
                }
                const ct = Date.now();
                if (finishTime && ct > finishTime)
                    return undefined; // Time's up
                let poll = finishTime ? finishTime - Date.now() : pollIntervalMs;
                poll = Math.min(pollIntervalMs, poll);
                // Race the terminal-status notification against the poll: whichever fires first ends the
                // wait. pollIntervalMs (default dbPollingIntervalResultMs = 1000) is the untouched floor,
                // so a dropped NOTIFY or disabled wake costs at most one interval, never a hang.
                const { promise, cancel } = (0, utils_1.cancellableSleep)(poll);
                try {
                    await Promise.race([completionPromise, promise]);
                }
                finally {
                    cancel();
                }
            }
            finally {
                if (cbr)
                    this.completionMap.deregisterCallback(cbr);
            }
        }
    }
    async awaitFirstWorkflowId(workflowIds, callerID, pollingIntervalMs) {
        const placeholders = workflowIds.map((_, i) => `$${i + 1}`).join(', ');
        const pollIntervalMs = pollingIntervalMs ?? this.dbPollingIntervalResultMs;
        while (true) {
            if (callerID)
                await this.#checkIfCanceledLimited(callerID);
            const { rows } = await this.#pollWithLimiter(() => this.pool.query(`SELECT workflow_uuid FROM "${this.schemaName}".workflow_status
           WHERE workflow_uuid IN (${placeholders})
             AND status NOT IN ('${workflow_1.StatusString.PENDING}', '${workflow_1.StatusString.ENQUEUED}', '${workflow_1.StatusString.DELAYED}')
           LIMIT 1`, workflowIds));
            if (rows.length > 0) {
                return rows[0].workflow_uuid;
            }
            await (0, utils_1.sleepms)(pollIntervalMs);
        }
    }
    async awaitWorkflowIds(workflowIds, callerID, pollingIntervalMs) {
        const remainingWorkflowIds = new Set(workflowIds);
        const pollIntervalMs = pollingIntervalMs ?? this.dbPollingIntervalResultMs;
        while (remainingWorkflowIds.size > 0) {
            const currentWorkflowIds = [...remainingWorkflowIds];
            if (callerID)
                await this.#checkIfCanceledLimited(callerID);
            const { rows } = await this.#pollWithLimiter(() => this.pool.query(`SELECT workflow_uuid FROM "${this.schemaName}".workflow_status
           WHERE workflow_uuid = ANY($1::text[])
             AND status NOT IN ('${workflow_1.StatusString.PENDING}', '${workflow_1.StatusString.ENQUEUED}', '${workflow_1.StatusString.DELAYED}')`, [currentWorkflowIds]));
            for (const row of rows) {
                remainingWorkflowIds.delete(row.workflow_uuid);
            }
            if (remainingWorkflowIds.size === 0) {
                return;
            }
            await (0, utils_1.sleepms)(pollIntervalMs);
        }
    }
    // ==================== Sleep ====================
    async durableSleepms(workflowID, functionID, durationMS) {
        const endTime = await this.#durableSleep(workflowID, functionID, durationMS);
        while (Date.now() < endTime) {
            await (0, utils_1.sleepms)(Math.min(endTime - Date.now(), utils_1.sleepConfig.maxTimeoutMS));
        }
        await this.checkIfCanceled(workflowID);
    }
    // ==================== Messaging ====================
    nullTopic = '__null__topic__';
    async send(workflowID, functionID, destinationID, message, topic, serialization, idempotencyKey) {
        topic = topic ?? this.nullTopic;
        const messageUUID = idempotencyKey ? `${idempotencyKey}::${destinationID}` : (0, crypto_1.randomUUID)();
        const client = await this.pool.connect();
        try {
            await client.query('BEGIN ISOLATION LEVEL READ COMMITTED');
            await this.#runAndRecordResult(client, exports.DBOS_FUNCNAME_SEND, workflowID, functionID, async () => {
                await client.query(`INSERT INTO "${this.schemaName}".notifications (destination_uuid, topic, message, serialization, message_uuid)
           VALUES ($1, $2, $3, $4, $5)
           ON CONFLICT (message_uuid) DO NOTHING;`, [destinationID, topic, message, serialization, messageUUID]);
                return undefined;
            });
            await client.query('COMMIT');
        }
        catch (error) {
            await client.query('ROLLBACK');
            const err = error;
            if (err.code === '23503') {
                // Foreign key constraint violation (only expected for the INSERT query)
                throw new error_1.DBOSNonExistentWorkflowError(`Sent to non-existent destination workflow UUID: ${destinationID}`);
            }
            else {
                throw err;
            }
        }
        finally {
            client.release();
        }
    }
    async sendDirect(destinationID, message, topic, serialization, idempotencyKey) {
        topic = topic ?? this.nullTopic;
        // Same per-destination scoping as send() above.
        const messageUUID = idempotencyKey ? `${idempotencyKey}::${destinationID}` : (0, crypto_1.randomUUID)();
        try {
            await this.pool.query(`INSERT INTO "${this.schemaName}".notifications (destination_uuid, topic, message, serialization, message_uuid)
         VALUES ($1, $2, $3, $4, $5)
         ON CONFLICT (message_uuid) DO NOTHING;`, [destinationID, topic, message, serialization, messageUUID]);
        }
        catch (error) {
            const err = error;
            if (err.code === '23503') {
                throw new error_1.DBOSNonExistentWorkflowError(`Sent to non-existent destination workflow UUID: ${destinationID}`);
            }
            throw err;
        }
    }
    async recv(workflowID, functionID, timeoutFunctionID, topic, timeoutSeconds = dbos_executor_1.DBOSExecutor.defaultNotificationTimeoutSec, pollingIntervalMs) {
        topic = topic ?? this.nullTopic;
        const startTime = Date.now();
        // First, check for previous executions.
        const res = await this.getOperationResultAndThrowIfCancelled(workflowID, functionID);
        if (res) {
            if (res.functionName !== exports.DBOS_FUNCNAME_RECV) {
                throw new error_1.DBOSUnexpectedStepError(workflowID, functionID, exports.DBOS_FUNCNAME_RECV, res.functionName);
            }
            return { serializedValue: res.output, serialization: res.serialization ?? null };
        }
        const timeoutms = timeoutSeconds !== undefined ? timeoutSeconds * 1000 : undefined;
        let finishTime = timeoutms !== undefined ? Date.now() + timeoutms : undefined;
        const pollIntervalMs = pollingIntervalMs ?? this.dbPollingIntervalEventMs;
        // Record the durable timeout deadline once before polling. #durableSleep persists it on the
        // first call and reads back the same value on recovery, so it never changes across iterations.
        if (timeoutms) {
            finishTime = await this.#durableSleep(workflowID, timeoutFunctionID, timeoutms);
        }
        while (true) {
            // register the key with the global notifications listener.
            let resolveNotification;
            const messagePromise = new Promise((resolve) => {
                resolveNotification = resolve;
            });
            const payload = `${workflowID}::${topic}`;
            const cbr = this.notificationsMap.registerCallback(payload, resolveNotification);
            try {
                await this.#checkIfCanceledLimited(workflowID);
                // Check if the key is already in the DB, then wait for the notification if it isn't.
                const initRecvRows = (await this.#pollWithLimiter(() => this.pool.query(`SELECT topic FROM "${this.schemaName}".notifications WHERE destination_uuid=$1 AND topic=$2 AND consumed = false;`, [workflowID, topic]))).rows;
                if (initRecvRows.length !== 0)
                    break;
                const ct = Date.now();
                if (finishTime && ct > finishTime)
                    break; // Time's up
                let poll = finishTime ? finishTime - Date.now() : pollIntervalMs;
                poll = Math.min(pollIntervalMs, poll);
                const { promise, cancel } = (0, utils_1.cancellableSleep)(poll);
                try {
                    await Promise.race([messagePromise, promise]);
                }
                finally {
                    cancel();
                }
            }
            finally {
                this.notificationsMap.deregisterCallback(cbr);
            }
        }
        await this.checkIfCanceled(workflowID);
        // Transactionally consume and return the message if it's in the DB, otherwise return null.
        let message = null;
        let serialization = null;
        const client = await this.pool.connect();
        try {
            await client.query(`BEGIN ISOLATION LEVEL READ COMMITTED`);
            const finalRecvRows = (await client.query(`UPDATE "${this.schemaName}".notifications
        SET consumed = true
        WHERE destination_uuid = $1
          AND topic = $2
          AND consumed = false
          AND message_uuid = (
            SELECT message_uuid
            FROM "${this.schemaName}".notifications
            WHERE destination_uuid = $1
              AND topic = $2
              AND consumed = false
            ORDER BY created_at_epoch_ms ASC
            LIMIT 1
          )
        RETURNING notifications.message, notifications.serialization;`, [workflowID, topic])).rows;
            if (finalRecvRows.length > 0) {
                message = finalRecvRows[0].message;
                serialization = finalRecvRows[0].serialization;
            }
            await this.recordOperationResultInternal(client, workflowID, functionID, exports.DBOS_FUNCNAME_RECV, true, startTime, Date.now(), {
                output: message,
                serialization,
            });
            await client.query(`COMMIT`);
        }
        catch (e) {
            this.logger.error(e);
            await client.query(`ROLLBACK`);
            throw e;
        }
        finally {
            client.release();
        }
        return { serializedValue: message, serialization };
    }
    // ==================== Events ====================
    async setEvent(workflowID, functionID, key, message, serialization) {
        const client = await this.pool.connect();
        try {
            await client.query('BEGIN ISOLATION LEVEL READ COMMITTED');
            // Only a real write (not a replay) should wake readers.
            let didWrite = false;
            await this.#runAndRecordResult(client, exports.DBOS_FUNCNAME_SETEVENT, workflowID, functionID, async () => {
                await client.query(`INSERT INTO "${this.schemaName}".workflow_events (workflow_uuid, key, value, serialization)
             VALUES ($1, $2, $3, $4)
             ON CONFLICT (workflow_uuid, key)
             DO UPDATE SET value = $3, serialization = $4
             RETURNING workflow_uuid;`, [workflowID, key, message, serialization]);
                // Also write to the immutable history table for fork support
                await client.query(`INSERT INTO "${this.schemaName}".workflow_events_history (workflow_uuid, function_id, key, value, serialization)
             VALUES ($1, $2, $3, $4, $5)
             ON CONFLICT (workflow_uuid, function_id, key)
             DO UPDATE SET value = $4, serialization = $5;`, [workflowID, functionID, key, message, serialization]);
                didWrite = true;
                return undefined;
            });
            await client.query('COMMIT');
            // Notify only after commit, so a woken getEvent sees the value.
            if (didWrite) {
                this.#signalNotification(exports.DBOS_WORKFLOW_EVENTS_CHANNEL, `${workflowID}::${key}`);
            }
        }
        catch (e) {
            this.logger.error(e);
            await client.query(`ROLLBACK`);
            throw e;
        }
        finally {
            client.release();
        }
    }
    async getEvent(workflowID, key, timeoutSeconds, callerWorkflow, pollingIntervalMs) {
        const startTime = Date.now();
        // Check if the operation has been done before for OAOO (only do this inside a workflow).
        if (callerWorkflow) {
            const res = await this.getOperationResultAndThrowIfCancelled(callerWorkflow.workflowID, callerWorkflow.functionID);
            if (res) {
                if (res.functionName !== exports.DBOS_FUNCNAME_GETEVENT) {
                    throw new error_1.DBOSUnexpectedStepError(callerWorkflow.workflowID, callerWorkflow.functionID, exports.DBOS_FUNCNAME_GETEVENT, res.functionName);
                }
                return { serializedValue: res.output, serialization: res.serialization ?? null };
            }
        }
        // Get the return the value. if it's in the DB, otherwise return null.
        let value = null;
        let valueSer = null;
        const payloadKey = `${workflowID}::${key}`;
        const timeoutms = timeoutSeconds !== undefined ? timeoutSeconds * 1000 : undefined;
        let finishTime = timeoutms !== undefined ? Date.now() + timeoutms : undefined;
        const pollIntervalMs = pollingIntervalMs ?? this.dbPollingIntervalEventMs;
        // If we have a callerWorkflow, we want a durable sleep, otherwise, not. Record the durable
        // timeout deadline once before polling. #durableSleep persists it on the first call and reads
        // back the same value on recovery, so it never changes across iterations.
        if (callerWorkflow && timeoutms) {
            finishTime = await this.#durableSleep(callerWorkflow.workflowID, callerWorkflow.timeoutFunctionID ?? -1, timeoutms);
        }
        // Register the key with the global notifications listener first... we do not want to look in the DB first
        //  or that would cause a timing hole.
        while (true) {
            let resolveNotification;
            const valuePromise = new Promise((resolve) => {
                resolveNotification = resolve;
            });
            const cbr = this.workflowEventsMap.registerCallback(payloadKey, resolveNotification);
            try {
                if (callerWorkflow?.workflowID)
                    await this.#checkIfCanceledLimited(callerWorkflow?.workflowID);
                // Check if the key is already in the DB, then wait for the notification if it isn't.
                const initRecvRows = (await this.#pollWithLimiter(() => this.pool.query(`SELECT key, value, serialization
             FROM "${this.schemaName}".workflow_events
             WHERE workflow_uuid=$1 AND key=$2;`, [workflowID, key]))).rows;
                if (initRecvRows.length > 0) {
                    value = initRecvRows[0].value;
                    valueSer = initRecvRows[0].serialization;
                    break;
                }
                const ct = Date.now();
                if (finishTime && ct > finishTime)
                    break; // Time's up
                let poll = finishTime ? finishTime - Date.now() : pollIntervalMs;
                poll = Math.min(pollIntervalMs, poll);
                const { promise, cancel } = (0, utils_1.cancellableSleep)(poll);
                try {
                    await Promise.race([valuePromise, promise]);
                }
                finally {
                    cancel();
                }
            }
            finally {
                this.workflowEventsMap.deregisterCallback(cbr);
            }
        }
        // Record the output if it is inside a workflow.
        if (callerWorkflow) {
            await this.recordOperationResult(callerWorkflow.workflowID, callerWorkflow.functionID, exports.DBOS_FUNCNAME_GETEVENT, true, startTime, Date.now(), {
                output: value,
                serialization: valueSer,
            });
        }
        return { serializedValue: value, serialization: valueSer };
    }
    // Event dispatcher queries / updates
    async getEventDispatchState(service, workflowName, key) {
        const res = await this.pool.query(`SELECT * FROM "${this.schemaName}".event_dispatch_kv
       WHERE workflow_fn_name = $1 AND service_name = $2 AND key = $3;`, [workflowName, service, key]);
        if (res.rows.length === 0)
            return undefined;
        return {
            service: res.rows[0].service_name,
            workflowFnName: res.rows[0].workflow_fn_name,
            key: res.rows[0].key,
            value: res.rows[0].value,
            updateTime: res.rows[0].update_time,
            updateSeq: res.rows[0].update_seq !== null && res.rows[0].update_seq !== undefined
                ? BigInt(res.rows[0].update_seq)
                : undefined,
        };
    }
    async upsertEventDispatchState(state) {
        const res = await this.pool.query(`INSERT INTO "${this.schemaName}".event_dispatch_kv (
        service_name, workflow_fn_name, key, value, update_time, update_seq)
       VALUES ($1, $2, $3, $4, $5, $6)
       ON CONFLICT (service_name, workflow_fn_name, key)
       DO UPDATE SET
         update_time = GREATEST(EXCLUDED.update_time, event_dispatch_kv.update_time),
         update_seq =  GREATEST(EXCLUDED.update_seq,  event_dispatch_kv.update_seq),
         value = CASE WHEN (EXCLUDED.update_time > event_dispatch_kv.update_time 
            OR EXCLUDED.update_seq > event_dispatch_kv.update_seq 
            OR (event_dispatch_kv.update_time IS NULL and event_dispatch_kv.update_seq IS NULL)
         ) THEN EXCLUDED.value ELSE event_dispatch_kv.value END
       RETURNING value, update_time, update_seq;`, [state.service, state.workflowFnName, state.key, state.value, state.updateTime, state.updateSeq]);
        return {
            service: state.service,
            workflowFnName: state.workflowFnName,
            key: state.key,
            value: res.rows[0].value,
            updateTime: res.rows[0].update_time,
            updateSeq: res.rows[0].update_seq !== undefined && res.rows[0].update_seq !== null
                ? BigInt(res.rows[0].update_seq)
                : undefined,
        };
    }
    // ==================== Streams ====================
    async writeStreamFromStep(workflowID, functionID, key, serializedValue, serialization) {
        const client = await this.pool.connect();
        try {
            await client.query('BEGIN ISOLATION LEVEL READ COMMITTED');
            // Find the maximum offset for this workflow_uuid and key combination
            const maxOffsetResult = await client.query(`SELECT MAX("offset") FROM "${this.schemaName}".streams
         WHERE workflow_uuid = $1 AND key = $2`, [workflowID, key]);
            // Next offset is max + 1, or 0 if no records exist
            const maxOffset = maxOffsetResult.rows[0].max;
            const nextOffset = maxOffset !== null ? maxOffset + 1 : 0;
            // Insert the new stream entry
            await client.query(`INSERT INTO "${this.schemaName}".streams (workflow_uuid, key, value, "offset", function_id, serialization)
         VALUES ($1, $2, $3, $4, $5, $6)`, [workflowID, key, serializedValue, nextOffset, functionID, serialization]);
            await client.query('COMMIT');
            // Notify only after commit, so a woken reader sees the value.
            this.#signalNotification(exports.DBOS_STREAMS_CHANNEL, `${workflowID}::${key}`);
        }
        catch (e) {
            this.logger.error(e);
            await client.query('ROLLBACK');
            throw e;
        }
        finally {
            client.release();
        }
    }
    async writeStreamFromWorkflow(workflowID, functionID, key, serializedValue, serialization, functionName) {
        const client = await this.pool.connect();
        try {
            await client.query('BEGIN ISOLATION LEVEL READ COMMITTED');
            // Only a real insert (not a replay) should wake readers.
            let didWrite = false;
            await this.#runAndRecordResult(client, functionName, workflowID, functionID, async () => {
                // Find the maximum offset for this workflow_uuid and key combination
                const maxOffsetResult = await client.query(`SELECT MAX("offset") FROM "${this.schemaName}".streams
           WHERE workflow_uuid = $1 AND key = $2`, [workflowID, key]);
                // Next offset is max + 1, or 0 if no records exist
                const maxOffset = maxOffsetResult.rows[0].max;
                const nextOffset = maxOffset !== null ? maxOffset + 1 : 0;
                // Insert the new stream entry
                await client.query(`INSERT INTO "${this.schemaName}".streams (workflow_uuid, key, value, "offset", function_id, serialization)
           VALUES ($1, $2, $3, $4, $5, $6)`, [workflowID, key, serializedValue, nextOffset, functionID, serialization]);
                didWrite = true;
                return undefined;
            });
            await client.query('COMMIT');
            // Notify only after commit, so a woken reader sees the value.
            if (didWrite) {
                this.#signalNotification(exports.DBOS_STREAMS_CHANNEL, `${workflowID}::${key}`);
            }
        }
        catch (e) {
            this.logger.error(e);
            await client.query('ROLLBACK');
            throw e;
        }
        finally {
            client.release();
        }
    }
    async closeStream(workflowID, functionID, key) {
        await this.writeStreamFromWorkflow(workflowID, functionID, key, exports.DBOS_STREAM_CLOSED_SENTINEL, 'portable_json', exports.DBOS_FUNCNAME_CLOSESTREAM);
    }
    // Read the value at `offset` and the workflow's status in one query: status null = no such workflow, value undefined = nothing at that offset.
    async readStreamValue(workflowID, key, offset) {
        // LEFT JOIN so a workflow with nothing at offset still reports its status (single PK lookup); under the poll limiter, inside @dbRetry so the permit frees across backoff.
        const result = await this.#pollWithLimiter(() => this.pool.query(
        // "offset" is a reserved word, so alias it (stream_offset) to read it back plainly.
        `SELECT ws.status AS status, s.value AS value, s.serialization AS serialization, s."offset" AS stream_offset
         FROM "${this.schemaName}".workflow_status ws
         LEFT OUTER JOIN "${this.schemaName}".streams s
           ON s.workflow_uuid = ws.workflow_uuid AND s.key = $2 AND s."offset" = $3
         WHERE ws.workflow_uuid = $1`, [workflowID, key, offset]));
        if (result.rows.length === 0) {
            return { status: null, value: undefined };
        }
        const row = result.rows[0];
        // streams.offset is non-nullable, so a NULL here means the join matched nothing at offset.
        if (row.stream_offset === null) {
            return { status: row.status, value: undefined };
        }
        return { status: row.status, value: { serializedValue: row.value, serialization: row.serialization } };
    }
    #signalNotification(channel, payload) {
        // Coalesce a wakeup on `channel` for `payload`; no-op without LISTEN/NOTIFY (clients, CockroachDB), which poll.
        if (!this.shouldUseDBNotifications) {
            return;
        }
        let batch = this.pendingNotifications.get(channel);
        if (batch === undefined) {
            batch = new Set();
            this.pendingNotifications.set(channel, batch);
        }
        batch.add(payload);
    }
    // Coalesce a hint-only queue/completion wake. Fires ONLY from ENQUEUED/terminal transition
    // sites, never per-update, and rides the same coalescing notifier as streams/events, so its
    // NOTIFY rate is bounded by notificationCoalesceMs regardless of workflow_status write volume
    // (see the wake-channel note above). A no-op when wakes are disabled, so the hottest workloads
    // pay nothing. The poll loop stays the correctness floor, so a dropped wake only costs latency.
    #signalWake(channel, payload) {
        if (!this.wakeNotificationsEnabled) {
            return;
        }
        this.#signalNotification(channel, payload);
    }
    /**
     * Register the queue scheduler's wake callback so an ENQUEUED-transition NOTIFY can trip its
     * dispatch loop before the next poll. The callback receives the woken queue's name. Returns a
     * deregistration handle, or `undefined` when wakes are disabled (the caller then relies on the
     * poll floor). Symmetric to how recv/getEvent register on their notification maps.
     */
    registerQueueWake(cb) {
        if (!this.wakeNotificationsEnabled) {
            return undefined;
        }
        return this.queueWakeMap.registerCallback(exports.QUEUE_WAKEUP_KEY, cb);
    }
    deregisterQueueWake(handle) {
        if (handle) {
            this.queueWakeMap.deregisterCallback(handle);
        }
    }
    // Periodically flush coalesced notifications across all channels, keeping the notifying commit off the write path.
    async #runNotifier() {
        while (this.#notifierActive) {
            const { promise, cancel } = (0, utils_1.cancellableSleep)(this.notificationCoalesceMs);
            this.#notifierWake = cancel;
            await promise;
            this.#notifierWake = null;
            if (!this.#notifierActive) {
                break;
            }
            try {
                await this.flushNotifications();
            }
            catch (e) {
                // Last resort: the flush drops its own failed batch, so this catches only unexpected errors that must not kill the push path.
                if (this.#notifierActive) {
                    this.logger.warn(`Notifier error: ${String(e)}`);
                    const { promise: backoff } = (0, utils_1.cancellableSleep)(1000);
                    await backoff;
                }
            }
        }
        // Final flush so values written just before shutdown still wake readers promptly.
        try {
            await this.flushNotifications();
        }
        catch (e) {
            this.logger.warn(`Notifier final flush error: ${String(e)}`);
        }
    }
    // Emit one notifying transaction per channel for all pending payloads; drop a channel's batch on failure. Soft-private so tests can drive it.
    async flushNotifications() {
        let hasPending = false;
        for (const batch of this.pendingNotifications.values()) {
            if (batch.size > 0) {
                hasPending = true;
                break;
            }
        }
        if (!hasPending) {
            return;
        }
        // Grab and clear atomically (no await between), so writes during the flush start the next batch.
        const batches = this.pendingNotifications;
        this.pendingNotifications = new Map();
        // One transaction per channel so an unsendable payload on one channel drops only its own batch.
        for (const [channel, batch] of batches) {
            if (batch.size === 0) {
                continue;
            }
            try {
                // One statement: one round trip, one async-notify queue-lock acquisition; unnest emits one notification per payload.
                const client = await this.pool.connect();
                try {
                    await client.query(`SELECT pg_notify($1, p) FROM unnest($2::text[]) AS p`, [channel, Array.from(batch)]);
                }
                finally {
                    client.release();
                }
            }
            catch (e) {
                // Drop the batch (don't requeue) on failure, e.g. a payload over pg_notify's 8000-byte limit; polling still delivers those values.
                this.logger.warn(`Notifier flush error on ${channel}: ${String(e)}`);
            }
        }
    }
    // ==================== Observability: Workflow Communications ====================
    async getAllEvents(workflowID) {
        const client = await this.pool.connect();
        try {
            const result = await client.query(`SELECT key, value, serialization FROM "${this.schemaName}".workflow_events
         WHERE workflow_uuid = $1`, [workflowID]);
            const events = {};
            for (const row of result.rows) {
                events[row.key] = await (0, serialization_1.safeParse)(this.serializer, row.value, row.serialization);
            }
            return events;
        }
        finally {
            client.release();
        }
    }
    async getAllNotifications(workflowID) {
        const client = await this.pool.connect();
        try {
            const result = await client.query(`SELECT topic, message, serialization, created_at_epoch_ms, consumed
         FROM "${this.schemaName}".notifications
         WHERE destination_uuid = $1
         ORDER BY created_at_epoch_ms`, [workflowID]);
            return await Promise.all(result.rows.map(async (row) => ({
                topic: row.topic === this.nullTopic ? null : row.topic,
                message: await (0, serialization_1.safeParse)(this.serializer, row.message, row.serialization),
                createdAtEpochMs: Number(row.created_at_epoch_ms),
                consumed: row.consumed,
            })));
        }
        finally {
            client.release();
        }
    }
    async getAllStreamEntries(workflowID) {
        const client = await this.pool.connect();
        try {
            const result = await client.query(`SELECT key, value, serialization FROM "${this.schemaName}".streams
         WHERE workflow_uuid = $1
         ORDER BY key, "offset"`, [workflowID]);
            const streams = {};
            for (const row of result.rows) {
                const value = await (0, serialization_1.safeParse)(this.serializer, row.value, row.serialization);
                if (value === exports.DBOS_STREAM_CLOSED_SENTINEL) {
                    continue;
                }
                if (!streams[row.key]) {
                    streams[row.key] = [];
                }
                streams[row.key].push(value);
            }
            return streams;
        }
        finally {
            client.release();
        }
    }
    // ==================== Queues ====================
    async transitionDelayedWorkflows() {
        // Transition workflows from DELAYED to ENQUEUED when their delay has expired.
        // For debounced workflows, clear the deduplication ID in the same atomic update: it is a
        // debounce key held only while DELAYED, so a later same-key debounce starts a fresh workflow.
        const res = await this.pool.query(`UPDATE "${this.schemaName}".workflow_status
       SET status = $1, updated_at = $2,
           deduplication_id = CASE WHEN is_debounced THEN NULL ELSE deduplication_id END
       WHERE status = $3 AND delay_until_epoch_ms <= $2
       RETURNING queue_name`, [workflow_1.StatusString.ENQUEUED, Date.now(), workflow_1.StatusString.DELAYED]);
        // Enqueue-transition wake per distinct queue that actually had a row become due. This is the
        // same NOTIFY path other processes' schedulers hear, so a delayed workflow whose queue is
        // dispatched elsewhere no longer waits a full poll interval past its delay.
        const wokenQueues = new Set();
        for (const row of res.rows) {
            if (row.queue_name) {
                wokenQueues.add(row.queue_name);
            }
        }
        for (const queueName of wokenQueues) {
            this.#signalWake(exports.DBOS_QUEUE_WAKEUP_CHANNEL, queueName);
        }
    }
    async clearQueueAssignment(workflowID) {
        // Reset the status of the task from "PENDING" to "ENQUEUED"
        const wqRes = await this.pool.query(`UPDATE "${this.schemaName}".workflow_status
        SET started_at_epoch_ms = NULL, status = $2
        WHERE workflow_uuid = $1 AND queue_name is NOT NULL AND status = $3
        RETURNING queue_name`, [workflowID, workflow_1.StatusString.ENQUEUED, workflow_1.StatusString.PENDING]);
        // If no rows were affected, the workflow is not anymore in the queue or was already completed
        const cleared = (wqRes.rowCount ?? 0) > 0;
        // Enqueue-transition wake: this row went back to ENQUEUED and is dispatchable again now.
        if (cleared && wqRes.rows[0]?.queue_name) {
            this.#signalWake(exports.DBOS_QUEUE_WAKEUP_CHANNEL, wqRes.rows[0].queue_name);
        }
        return cleared;
    }
    async getDeduplicatedWorkflow(queueName, deduplicationID) {
        const { rows } = await this.pool.query(`SELECT workflow_uuid FROM "${this.schemaName}".workflow_status
       WHERE queue_name = $1 AND deduplication_id = $2`, [queueName, deduplicationID]);
        if (rows.length === 0) {
            return null;
        }
        return rows[0].workflow_uuid;
    }
    async getQueuePartitions(queueName) {
        const { rows } = await this.pool.query(`SELECT DISTINCT queue_partition_key FROM "${this.schemaName}".workflow_status
       WHERE queue_name = $1
         AND status = $2
         AND queue_partition_key IS NOT NULL`, [queueName, workflow_1.StatusString.ENQUEUED]);
        return rows.map((row) => row.queue_partition_key);
    }
    async findAndMarkStartableWorkflows(queue, executorID, appVersion, queuePartitionKey) {
        const startTimeMs = Date.now();
        const limiterPeriodMS = queue.rateLimit ? queue.rateLimit.periodSec * 1000 : 0;
        const claimedIDs = [];
        const localRunningForQueue = this.countRunningWorkflowsForQueue(queue.name, queuePartitionKey);
        // Build partition key filter
        let partitionFilter = '';
        const partitionParams = [];
        if (queuePartitionKey !== undefined) {
            partitionFilter = `AND queue_partition_key = $PARTITION`;
            partitionParams.push(queuePartitionKey);
        }
        const client = await this.pool.connect();
        try {
            // Default to READ COMMITTED except with global concurrency limits or rate limits
            if (queue.concurrency !== undefined || queue.rateLimit !== undefined) {
                await client.query('BEGIN ISOLATION LEVEL REPEATABLE READ');
            }
            else {
                await client.query('BEGIN');
            }
            // If there is a rate limit, compute how many functions have started in its period.
            let numRecentQueries = 0;
            if (queue.rateLimit) {
                const params = [
                    queue.name,
                    workflow_1.StatusString.ENQUEUED,
                    workflow_1.StatusString.DELAYED,
                    startTimeMs - limiterPeriodMS,
                    ...partitionParams,
                ];
                const countResult = await client.query(`SELECT COUNT(*) FROM "${this.schemaName}".workflow_status
           WHERE queue_name = $1
             AND rate_limited = TRUE
             AND status NOT IN ($2, $3)
             AND started_at_epoch_ms > $4
             ${partitionFilter.replace('$PARTITION', '$5')}`, params);
                numRecentQueries = Number(countResult.rows[0].count);
                if (numRecentQueries >= queue.rateLimit.limitPerPeriod) {
                    await client.query('COMMIT');
                    return claimedIDs;
                }
            }
            // Dequeue functions eligible for this worker and ordered by the time at which they were enqueued.
            // If there is a global or local concurrency limit N, select only the N oldest enqueued
            // functions, else select all of them.
            let maxTasks = Infinity;
            if (queue.workerConcurrency !== undefined) {
                // Use the in-memory registry for this worker's running count — avoids a DB round trip.
                maxTasks = Math.max(0, queue.workerConcurrency - localRunningForQueue);
            }
            if (queue.concurrency !== undefined) {
                // Global concurrency still requires a DB query since other workers may be running workflows too.
                const params = [queue.name, workflow_1.StatusString.PENDING, ...partitionParams];
                const runningTasksResult = await client.query(`SELECT COUNT(*) as task_count
           FROM "${this.schemaName}".workflow_status
           WHERE queue_name = $1 AND status = $2
             ${partitionFilter.replace('$PARTITION', '$3')}`, params);
                const totalRunningTasks = Number(runningTasksResult.rows[0]?.task_count ?? 0);
                if (totalRunningTasks > queue.concurrency) {
                    this.logger.warn(`Total running tasks (${totalRunningTasks}) exceeds the global concurrency limit (${queue.concurrency})`);
                }
                const availableTasks = Math.max(0, queue.concurrency - totalRunningTasks);
                maxTasks = Math.min(maxTasks, availableTasks);
            }
            // Return immediately if there are no available tasks due to flow control limits
            if (maxTasks <= 0) {
                await client.query('COMMIT');
                return claimedIDs;
            }
            // Retrieve the first max_tasks workflows in the queue.
            const latestVersionResult = await client.query(`SELECT version_name
         FROM "${this.schemaName}".application_versions
         ORDER BY version_timestamp DESC LIMIT 1`);
            const latestVersion = latestVersionResult.rows[0]?.version_name;
            const isLatestVersion = latestVersion === undefined || latestVersion === appVersion;
            const versionClause = isLatestVersion
                ? '(application_version = $3 OR application_version IS NULL)'
                : 'application_version = $3';
            const lockMode = queue.concurrency ? 'FOR UPDATE NOWAIT' : 'FOR UPDATE SKIP LOCKED';
            const limitClause = maxTasks !== Infinity ? `LIMIT ${maxTasks}` : '';
            const selectParams = [workflow_1.StatusString.ENQUEUED, queue.name, appVersion, ...partitionParams];
            const selectQuery = `
        SELECT workflow_uuid
        FROM "${this.schemaName}".workflow_status
        WHERE status = $1
          AND queue_name = $2
          AND ${versionClause}
          ${partitionFilter.replace('$PARTITION', '$4')}
        ORDER BY priority ASC, created_at ASC
        ${limitClause}
        ${lockMode}
      `;
            const { rows } = await client.query(selectQuery, selectParams);
            // Fires while the SELECT FOR UPDATE lock is held — tests can throw a
            // synthetic 55P03 here to simulate a concurrent executor winning the race.
            await (0, debugpoint_1.debugTriggerPoint)(debugpoint_1.DEBUG_TRIGGER_FIND_AND_MARK_AFTER_SELECT);
            // Start the workflows
            const workflowIDs = rows.map((row) => row.workflow_uuid);
            for (const id of workflowIDs) {
                // If we have a rate limit, stop starting functions when the number
                //   of functions started this period exceeds the limit.
                if (queue.rateLimit && claimedIDs.length + numRecentQueries >= queue.rateLimit.limitPerPeriod) {
                    break;
                }
                // Start the functions by marking them as pending and updating their executor IDs.
                // Only claim the workflow if the UPDATE actually transitioned an ENQUEUED row —
                // otherwise another worker won the race and we must not re-dispatch it.
                const updateRes = await client.query(`UPDATE "${this.schemaName}".workflow_status
           SET status = $1,
               executor_id = $2,
               application_version = $3,
               started_at_epoch_ms = $4,
               rate_limited = $5,
               workflow_deadline_epoch_ms = CASE
                 WHEN workflow_timeout_ms IS NOT NULL AND workflow_deadline_epoch_ms IS NULL
                 THEN (EXTRACT(epoch FROM now()) * 1000)::bigint + workflow_timeout_ms
                 ELSE workflow_deadline_epoch_ms
               END
           WHERE workflow_uuid = $6 AND status = $7`, [
                    workflow_1.StatusString.PENDING,
                    executorID,
                    appVersion,
                    startTimeMs,
                    queue.rateLimit !== undefined,
                    id,
                    workflow_1.StatusString.ENQUEUED,
                ]);
                if ((updateRes.rowCount ?? 0) > 0) {
                    claimedIDs.push(id);
                }
            }
            await client.query('COMMIT');
        }
        catch (error) {
            await client.query('ROLLBACK');
            throw error;
        }
        finally {
            client.release();
        }
        // Return the IDs of all functions we marked started
        return claimedIDs;
    }
    // ==================== Queries & Maintenance ====================
    async listWorkflows(input) {
        const schemaName = this.schemaName;
        const selectColumns = [
            'workflow_uuid',
            'status',
            'name',
            'recovery_attempts',
            'config_name',
            'class_name',
            'authenticated_user',
            'authenticated_roles',
            'assumed_role',
            'queue_name',
            'executor_id',
            'created_at',
            'updated_at',
            'application_version',
            'application_id',
            'workflow_deadline_epoch_ms',
            'workflow_timeout_ms',
            'deduplication_id',
            'priority',
            'queue_partition_key',
            'started_at_epoch_ms',
            'forked_from',
            'was_forked_from',
            'parent_workflow_id',
            'delay_until_epoch_ms',
            'completed_at',
            'attributes',
            'schedule_name',
            'debounce_deadline_epoch_ms',
            'is_debounced',
        ];
        input.loadInput = input.loadInput ?? true;
        input.loadOutput = input.loadOutput ?? true;
        if (input.loadInput) {
            selectColumns.push('inputs', 'request');
        }
        if (input.loadOutput) {
            selectColumns.push('output', 'error');
        }
        if (input.loadInput || input.loadOutput) {
            selectColumns.push('serialization');
        }
        input.sortDesc = input.sortDesc ?? false; // By default, sort in ascending order
        // Build WHERE clauses
        const whereClauses = [];
        const params = [];
        let paramCounter = 1;
        // Helper: add a filter for a field that may be a single value or an array.
        // Uses = for a single value, IN (...) for an array.
        const addFilter = (column, value) => {
            if (!value)
                return;
            if (Array.isArray(value)) {
                const placeholders = value.map((_, i) => `$${paramCounter + i}`).join(', ');
                whereClauses.push(`${column} IN (${placeholders})`);
                params.push(...value);
                paramCounter += value.length;
            }
            else {
                whereClauses.push(`${column} = $${paramCounter}`);
                params.push(value);
                paramCounter++;
            }
        };
        // If queuesOnly, filter for queued workflows
        if (input.queuesOnly) {
            whereClauses.push(`queue_name IS NOT NULL`);
            whereClauses.push(`status IN ($${paramCounter}, $${paramCounter + 1}, $${paramCounter + 2})`);
            params.push(workflow_1.StatusString.ENQUEUED, workflow_1.StatusString.PENDING, workflow_1.StatusString.DELAYED);
            paramCounter += 3;
        }
        addFilter('name', input.workflowName);
        addFilter('queue_name', input.queueName);
        addFilter('schedule_name', input.scheduleName);
        if (input.workflow_id_prefix) {
            if (Array.isArray(input.workflow_id_prefix)) {
                const likeClauses = input.workflow_id_prefix.map((_, i) => `workflow_uuid LIKE $${paramCounter + i}`);
                whereClauses.push(`(${likeClauses.join(' OR ')})`);
                params.push(...input.workflow_id_prefix.map((p) => `${p}%`));
                paramCounter += input.workflow_id_prefix.length;
            }
            else {
                whereClauses.push(`workflow_uuid LIKE $${paramCounter}`);
                params.push(`${input.workflow_id_prefix}%`);
                paramCounter++;
            }
        }
        if (input.workflowIDs) {
            const placeholders = input.workflowIDs.map((_, i) => `$${paramCounter + i}`).join(', ');
            whereClauses.push(`workflow_uuid IN (${placeholders})`);
            params.push(...input.workflowIDs);
            paramCounter += input.workflowIDs.length;
        }
        addFilter('authenticated_user', input.authenticatedUser);
        addFilter('forked_from', input.forkedFrom);
        addFilter('parent_workflow_id', input.parentWorkflowID);
        if (input.wasForkedFrom !== undefined) {
            whereClauses.push(`was_forked_from = $${paramCounter}`);
            params.push(input.wasForkedFrom);
            paramCounter++;
        }
        if (input.hasParent !== undefined) {
            if (input.hasParent) {
                whereClauses.push(`parent_workflow_id IS NOT NULL`);
            }
            else {
                whereClauses.push(`parent_workflow_id IS NULL`);
            }
        }
        // Match workflows whose attributes JSONB contains all the given key-value pairs.
        // The `@>` containment operator is served by the GIN index on the attributes column.
        if (input.attributes && Object.keys(input.attributes).length > 0) {
            whereClauses.push(`attributes @> $${paramCounter}::jsonb`);
            params.push(JSON.stringify(input.attributes));
            paramCounter++;
        }
        if (input.startTime) {
            whereClauses.push(`created_at >= $${paramCounter}`);
            params.push(new Date(input.startTime).getTime());
            paramCounter++;
        }
        if (input.endTime) {
            whereClauses.push(`created_at <= $${paramCounter}`);
            params.push(new Date(input.endTime).getTime());
            paramCounter++;
        }
        if (input.completedAfter) {
            whereClauses.push(`completed_at >= $${paramCounter}`);
            params.push(new Date(input.completedAfter).getTime());
            paramCounter++;
        }
        if (input.completedBefore) {
            whereClauses.push(`completed_at <= $${paramCounter}`);
            params.push(new Date(input.completedBefore).getTime());
            paramCounter++;
        }
        // dequeuedAfter/Before filter on started_at_epoch_ms: that column is
        // populated on dequeue and surfaced as WorkflowStatus.dequeuedAt.
        if (input.dequeuedAfter) {
            whereClauses.push(`started_at_epoch_ms >= $${paramCounter}`);
            params.push(new Date(input.dequeuedAfter).getTime());
            paramCounter++;
        }
        if (input.dequeuedBefore) {
            whereClauses.push(`started_at_epoch_ms <= $${paramCounter}`);
            params.push(new Date(input.dequeuedBefore).getTime());
            paramCounter++;
        }
        addFilter('status', input.status);
        addFilter('application_version', input.applicationVersion);
        addFilter('executor_id', input.executorId);
        const whereClause = whereClauses.length > 0 ? `WHERE ${whereClauses.join(' AND ')}` : '';
        const orderClause = `ORDER BY created_at ${input.sortDesc ? 'DESC' : 'ASC'}`;
        const limitClause = input.limit ? `LIMIT ${input.limit}` : '';
        const offsetClause = input.offset ? `OFFSET ${input.offset}` : '';
        const query = `
      SELECT ${selectColumns.join(', ')}
      FROM "${schemaName}".workflow_status
      ${whereClause}
      ${orderClause}
      ${limitClause}
      ${offsetClause}
    `;
        const result = await this.pool.query(query, params);
        return result.rows.map(mapWorkflowStatus);
    }
    async getWorkflowAggregates(input) {
        if (input.timeBucketSizeMs !== undefined && input.timeBucketSizeMs <= 0) {
            throw new Error('time_bucket_size_ms must be > 0');
        }
        const groupByFlags = [
            ['status', input.groupByStatus ?? false, 'status'],
            ['name', input.groupByName ?? false, 'name'],
            ['queue_name', input.groupByQueueName ?? false, 'queue_name'],
            ['executor_id', input.groupByExecutorId ?? false, 'executor_id'],
            ['application_version', input.groupByApplicationVersion ?? false, 'application_version'],
        ];
        const groupNames = [];
        const groupColumns = [];
        const groupSelectColumns = [];
        for (const [colName, enabled, col] of groupByFlags) {
            if (enabled) {
                groupNames.push(colName);
                groupColumns.push(col);
                groupSelectColumns.push(col);
            }
        }
        if (input.timeBucketSizeMs !== undefined) {
            // Bucket on created_at — the indexed wall-clock timestamp on workflow_status.
            const bucket = input.timeBucketSizeMs;
            const bucketExpr = `(CAST(FLOOR(created_at / ${bucket}) AS BIGINT) * ${bucket})`;
            groupNames.push('time_bucket');
            groupColumns.push(bucketExpr);
            groupSelectColumns.push(`${bucketExpr} AS time_bucket`);
        }
        if (groupColumns.length === 0) {
            throw new Error('At least one group_by flag must be set to True');
        }
        // Build select columns from boolean flags. MAX ignores NULLs, so rows
        // missing started_at_epoch_ms or completed_at naturally drop out of the
        // latency maxes.
        const selectFlags = [
            ['count', input.selectCount ?? false, 'COUNT(*)'],
            ['min_created_at', input.selectMinCreatedAt ?? false, 'MIN(created_at)'],
            ['max_queue_wait_ms', input.selectMaxQueueWaitMs ?? false, 'MAX(started_at_epoch_ms - created_at)'],
            ['max_total_latency_ms', input.selectMaxTotalLatencyMs ?? false, 'MAX(completed_at - created_at)'],
        ];
        const selectNames = [];
        const selectColumns = [];
        for (const [name, enabled, expr] of selectFlags) {
            if (enabled) {
                selectNames.push(name);
                selectColumns.push(`${expr} AS ${name}`);
            }
        }
        if (selectColumns.length === 0) {
            throw new Error('At least one select_ flag must be set to True');
        }
        const whereClauses = [];
        const params = [];
        let paramIdx = 1;
        const addFilter = (column, values) => {
            if (!values || values.length === 0)
                return;
            const placeholders = values.map((_, i) => `$${paramIdx + i}`).join(', ');
            whereClauses.push(`${column} IN (${placeholders})`);
            params.push(...values);
            paramIdx += values.length;
        };
        addFilter('status', input.status);
        addFilter('name', input.name);
        addFilter('application_version', input.appVersion);
        addFilter('executor_id', input.executorId);
        addFilter('queue_name', input.queueName);
        if (input.workflowIdPrefix && input.workflowIdPrefix.length > 0) {
            const likeClauses = input.workflowIdPrefix.map((p) => {
                params.push(`${p}%`);
                return `workflow_uuid LIKE $${paramIdx++}`;
            });
            whereClauses.push(`(${likeClauses.join(' OR ')})`);
        }
        addFilter('workflow_uuid', input.workflowIDs);
        addFilter('authenticated_user', input.authenticatedUser);
        addFilter('forked_from', input.forkedFrom);
        addFilter('parent_workflow_id', input.parentWorkflowID);
        addFilter('schedule_name', input.scheduleName);
        // Only workflows that are actively enqueued.
        if (input.queuesOnly) {
            whereClauses.push(`queue_name IS NOT NULL`);
            whereClauses.push(`status IN ($${paramIdx}, $${paramIdx + 1}, $${paramIdx + 2})`);
            params.push(workflow_1.StatusString.ENQUEUED, workflow_1.StatusString.PENDING, workflow_1.StatusString.DELAYED);
            paramIdx += 3;
        }
        if (input.wasForkedFrom !== undefined) {
            whereClauses.push(`was_forked_from = $${paramIdx}`);
            params.push(input.wasForkedFrom);
            paramIdx++;
        }
        if (input.hasParent !== undefined) {
            whereClauses.push(input.hasParent ? `parent_workflow_id IS NOT NULL` : `parent_workflow_id IS NULL`);
        }
        // Match workflows whose attributes JSONB contains all the given key-value pairs.
        if (input.attributes && Object.keys(input.attributes).length > 0) {
            whereClauses.push(`attributes @> $${paramIdx}::jsonb`);
            params.push(JSON.stringify(input.attributes));
            paramIdx++;
        }
        if (input.startTime) {
            whereClauses.push(`created_at >= $${paramIdx}`);
            params.push(new Date(input.startTime).getTime());
            paramIdx++;
        }
        if (input.endTime) {
            whereClauses.push(`created_at <= $${paramIdx}`);
            params.push(new Date(input.endTime).getTime());
            paramIdx++;
        }
        if (input.completedAfter) {
            whereClauses.push(`completed_at >= $${paramIdx}`);
            params.push(new Date(input.completedAfter).getTime());
            paramIdx++;
        }
        if (input.completedBefore) {
            whereClauses.push(`completed_at <= $${paramIdx}`);
            params.push(new Date(input.completedBefore).getTime());
            paramIdx++;
        }
        // dequeuedAfter/Before filter on started_at_epoch_ms: that column is
        // populated on dequeue and surfaced as WorkflowStatus.dequeuedAt.
        if (input.dequeuedAfter) {
            whereClauses.push(`started_at_epoch_ms >= $${paramIdx}`);
            params.push(new Date(input.dequeuedAfter).getTime());
            paramIdx++;
        }
        if (input.dequeuedBefore) {
            whereClauses.push(`started_at_epoch_ms <= $${paramIdx}`);
            params.push(new Date(input.dequeuedBefore).getTime());
            paramIdx++;
        }
        const whereClause = whereClauses.length > 0 ? `WHERE ${whereClauses.join(' AND ')}` : '';
        const groupByClause = groupColumns.join(', ');
        const selectClause = [...groupSelectColumns, ...selectColumns].join(', ');
        const query = `
      SELECT ${selectClause}
      FROM "${this.schemaName}".workflow_status
      ${whereClause}
      GROUP BY ${groupByClause}
    `;
        const result = await this.pool.query(query, params);
        const toIntOrNull = (v) => (v === null || v === undefined ? null : Number(v));
        return result.rows.map((row) => {
            const group = {};
            for (const name of groupNames) {
                const v = row[name];
                group[name] = v === null || v === undefined ? null : String(v);
            }
            return {
                group,
                count: selectNames.includes('count') ? toIntOrNull(row.count) : null,
                minCreatedAt: selectNames.includes('min_created_at') ? toIntOrNull(row.min_created_at) : null,
                maxQueueWaitMs: selectNames.includes('max_queue_wait_ms') ? toIntOrNull(row.max_queue_wait_ms) : null,
                maxTotalLatencyMs: selectNames.includes('max_total_latency_ms') ? toIntOrNull(row.max_total_latency_ms) : null,
            };
        });
    }
    async getStepAggregates(input) {
        if (input.timeBucketSizeMs !== undefined && input.timeBucketSizeMs <= 0) {
            throw new Error('time_bucket_size_ms must be > 0');
        }
        // operation_outputs has no explicit status column; derive it from
        // whether `error` is populated. Bookkeeping rows from recordChildWorkflow
        // and DBOS.getResult have NULL error and NULL output, so they appear
        // as SUCCESS here — callers can filter them by function_name.
        const statusExpr = `CASE WHEN error IS NULL THEN 'SUCCESS' ELSE 'ERROR' END`;
        const groupByFlags = [
            ['function_name', input.groupByFunctionName ?? false, 'function_name'],
            ['status', input.groupByStatus ?? false, statusExpr],
        ];
        const groupNames = [];
        const groupColumns = [];
        const groupSelectColumns = [];
        for (const [colName, enabled, expr] of groupByFlags) {
            if (enabled) {
                groupNames.push(colName);
                groupColumns.push(expr);
                groupSelectColumns.push(`${expr} AS ${colName}`);
            }
        }
        if (input.timeBucketSizeMs !== undefined) {
            // Bucket on completed_at_epoch_ms — it's the indexed timestamp on
            // this table.
            const bucket = input.timeBucketSizeMs;
            const bucketExpr = `(CAST(FLOOR(completed_at_epoch_ms / ${bucket}) AS BIGINT) * ${bucket})`;
            groupNames.push('time_bucket');
            groupColumns.push(bucketExpr);
            groupSelectColumns.push(`${bucketExpr} AS time_bucket`);
        }
        if (groupColumns.length === 0) {
            throw new Error('At least one group_by flag must be set to True');
        }
        // Build select columns from boolean flags. MAX ignores NULLs, so rows
        // without start/complete timestamps (child-workflow and getResult
        // markers) drop out of the duration max.
        const selectFlags = [
            ['count', input.selectCount ?? false, 'COUNT(*)'],
            ['max_duration_ms', input.selectMaxDurationMs ?? false, 'MAX(completed_at_epoch_ms - started_at_epoch_ms)'],
        ];
        const selectNames = [];
        const selectColumns = [];
        for (const [name, enabled, expr] of selectFlags) {
            if (enabled) {
                selectNames.push(name);
                selectColumns.push(`${expr} AS ${name}`);
            }
        }
        if (selectColumns.length === 0) {
            throw new Error('At least one select_ flag must be set to True');
        }
        const whereClauses = [];
        const params = [];
        let paramIdx = 1;
        if (input.status && input.status.length > 0) {
            const placeholders = input.status.map((_, i) => `$${paramIdx + i}`).join(', ');
            whereClauses.push(`(${statusExpr}) IN (${placeholders})`);
            params.push(...input.status);
            paramIdx += input.status.length;
        }
        if (input.functionName && input.functionName.length > 0) {
            const placeholders = input.functionName.map((_, i) => `$${paramIdx + i}`).join(', ');
            whereClauses.push(`function_name IN (${placeholders})`);
            params.push(...input.functionName);
            paramIdx += input.functionName.length;
        }
        if (input.workflowIdPrefix && input.workflowIdPrefix.length > 0) {
            const likeClauses = input.workflowIdPrefix.map((p) => {
                params.push(`${p}%`);
                return `workflow_uuid LIKE $${paramIdx++}`;
            });
            whereClauses.push(`(${likeClauses.join(' OR ')})`);
        }
        if (input.completedAfter) {
            whereClauses.push(`completed_at_epoch_ms >= $${paramIdx}`);
            params.push(new Date(input.completedAfter).getTime());
            paramIdx++;
        }
        if (input.completedBefore) {
            whereClauses.push(`completed_at_epoch_ms <= $${paramIdx}`);
            params.push(new Date(input.completedBefore).getTime());
            paramIdx++;
        }
        const whereClause = whereClauses.length > 0 ? `WHERE ${whereClauses.join(' AND ')}` : '';
        const groupByClause = groupColumns.join(', ');
        const selectClause = [...groupSelectColumns, ...selectColumns].join(', ');
        const query = `
      SELECT ${selectClause}
      FROM "${this.schemaName}".operation_outputs
      ${whereClause}
      GROUP BY ${groupByClause}
    `;
        const result = await this.pool.query(query, params);
        const toIntOrNull = (v) => (v === null || v === undefined ? null : Number(v));
        return result.rows.map((row) => {
            const group = {};
            for (const name of groupNames) {
                const v = row[name];
                group[name] = v === null || v === undefined ? null : String(v);
            }
            return {
                group,
                count: selectNames.includes('count') ? toIntOrNull(row.count) : null,
                maxDurationMs: selectNames.includes('max_duration_ms') ? toIntOrNull(row.max_duration_ms) : null,
            };
        });
    }
    async garbageCollect(cutoffEpochTimestampMs, rowsThreshold) {
        if (rowsThreshold !== undefined) {
            // Get the created_at timestamp of the rows_threshold newest row
            const result = await this.pool.query(`SELECT created_at
         FROM "${this.schemaName}".workflow_status
         ORDER BY created_at DESC
         LIMIT 1 OFFSET $1`, [rowsThreshold - 1]);
            if (result.rows.length > 0) {
                const rowsBasedCutoff = result.rows[0].created_at;
                // Use the more restrictive cutoff (higher timestamp = more recent = more deletion)
                if (cutoffEpochTimestampMs === undefined || rowsBasedCutoff > cutoffEpochTimestampMs) {
                    cutoffEpochTimestampMs = rowsBasedCutoff;
                }
            }
        }
        if (cutoffEpochTimestampMs === undefined) {
            return;
        }
        // Delete all workflows older than cutoff that are NOT PENDING, ENQUEUED, or DELAYED
        await this.pool.query(`DELETE FROM "${this.schemaName}".workflow_status
       WHERE created_at < $1
         AND status NOT IN ($2, $3, $4)`, [cutoffEpochTimestampMs, workflow_1.StatusString.PENDING, workflow_1.StatusString.ENQUEUED, workflow_1.StatusString.DELAYED]);
        return;
    }
    async getMetrics(startTime, endTime) {
        const startEpochMs = new Date(startTime).getTime();
        const endEpochMs = new Date(endTime).getTime();
        const metrics = [];
        // Query workflow metrics
        const workflowResult = await this.pool.query(`SELECT name, COUNT(workflow_uuid) as count
       FROM "${this.schemaName}".workflow_status
       WHERE created_at >= $1 AND created_at < $2
       GROUP BY name`, [startEpochMs, endEpochMs]);
        for (const row of workflowResult.rows) {
            metrics.push({
                metricType: 'workflow_count',
                metricName: row.name,
                value: Number(row.count),
            });
        }
        // Query step metrics
        const stepResult = await this.pool.query(`SELECT function_name, COUNT(*) as count
       FROM "${this.schemaName}".operation_outputs
       WHERE completed_at_epoch_ms >= $1 AND completed_at_epoch_ms < $2
       GROUP BY function_name`, [startEpochMs, endEpochMs]);
        for (const row of stepResult.rows) {
            metrics.push({
                metricType: 'step_count',
                metricName: row.function_name,
                value: Number(row.count),
            });
        }
        return metrics;
    }
    // ==================== Scheduling ====================
    async createSchedule(schedule, client) {
        const q = client ?? this.pool;
        try {
            await q.query(`INSERT INTO "${this.schemaName}".workflow_schedules
         (schedule_id, schedule_name, workflow_name, workflow_class_name, schedule, status, context, last_fired_at, automatic_backfill, cron_timezone, queue_name)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)`, [
                schedule.scheduleId,
                schedule.scheduleName,
                schedule.workflowName,
                schedule.workflowClassName,
                schedule.schedule,
                schedule.status,
                schedule.context,
                schedule.lastFiredAt,
                schedule.automaticBackfill,
                schedule.cronTimezone,
                schedule.queueName,
            ]);
        }
        catch (e) {
            if (e instanceof pg_1.DatabaseError && e.code === '23505') {
                throw new Error(`Schedule '${schedule.scheduleName}' already exists`);
            }
            throw e;
        }
    }
    async listSchedules(filters, client) {
        const q = client ?? this.pool;
        const conditions = [];
        const params = [];
        let paramIdx = 1;
        if (filters?.status) {
            const vals = Array.isArray(filters.status) ? filters.status : [filters.status];
            const placeholders = vals.map((v) => {
                params.push(v);
                return `$${paramIdx++}`;
            });
            conditions.push(`status IN (${placeholders.join(', ')})`);
        }
        if (filters?.workflowName) {
            const vals = Array.isArray(filters.workflowName) ? filters.workflowName : [filters.workflowName];
            const placeholders = vals.map((v) => {
                params.push(v);
                return `$${paramIdx++}`;
            });
            conditions.push(`workflow_name IN (${placeholders.join(', ')})`);
        }
        if (filters?.scheduleNamePrefix) {
            const prefixes = Array.isArray(filters.scheduleNamePrefix)
                ? filters.scheduleNamePrefix
                : [filters.scheduleNamePrefix];
            const likeClauses = prefixes.map((p) => {
                params.push(`${p}%`);
                return `schedule_name LIKE $${paramIdx++}`;
            });
            conditions.push(`(${likeClauses.join(' OR ')})`);
        }
        const where = conditions.length > 0 ? ` WHERE ${conditions.join(' AND ')}` : '';
        const result = await q.query(`SELECT schedule_id, schedule_name, workflow_name, workflow_class_name, schedule, status, context, last_fired_at, automatic_backfill, cron_timezone, queue_name
       FROM "${this.schemaName}".workflow_schedules${where}
       ORDER BY schedule_name`, params);
        return result.rows.map((row) => ({
            scheduleId: row.schedule_id,
            scheduleName: row.schedule_name,
            workflowName: row.workflow_name,
            workflowClassName: row.workflow_class_name,
            schedule: row.schedule,
            status: row.status,
            context: row.context,
            lastFiredAt: row.last_fired_at ?? null,
            automaticBackfill: !!row.automatic_backfill,
            cronTimezone: row.cron_timezone ?? null,
            queueName: row.queue_name ?? null,
        }));
    }
    async getSchedule(name, client) {
        const q = client ?? this.pool;
        const result = await q.query(`SELECT schedule_id, schedule_name, workflow_name, workflow_class_name, schedule, status, context, last_fired_at, automatic_backfill, cron_timezone, queue_name
       FROM "${this.schemaName}".workflow_schedules
       WHERE schedule_name = $1`, [name]);
        if (result.rows.length === 0)
            return null;
        const row = result.rows[0];
        return {
            scheduleId: row.schedule_id,
            scheduleName: row.schedule_name,
            workflowName: row.workflow_name,
            workflowClassName: row.workflow_class_name,
            schedule: row.schedule,
            status: row.status,
            context: row.context,
            lastFiredAt: row.last_fired_at ?? null,
            automaticBackfill: !!row.automatic_backfill,
            cronTimezone: row.cron_timezone ?? null,
            queueName: row.queue_name ?? null,
        };
    }
    async deleteSchedule(name, client) {
        const q = client ?? this.pool;
        await q.query(`DELETE FROM "${this.schemaName}".workflow_schedules WHERE schedule_name = $1`, [name]);
    }
    async setScheduleStatus(name, status, client) {
        const q = client ?? this.pool;
        await q.query(`UPDATE "${this.schemaName}".workflow_schedules SET status = $1 WHERE schedule_name = $2`, [
            status,
            name,
        ]);
    }
    async updateSchedule(name, updates, client) {
        const q = client ?? this.pool;
        // Only update the definition fields the caller provided, leaving runtime state (schedule_id, status, last_fired_at) untouched.
        const columns = [
            ['schedule', 'schedule'],
            ['context', 'context'],
            ['automaticBackfill', 'automatic_backfill'],
            ['cronTimezone', 'cron_timezone'],
            ['queueName', 'queue_name'],
        ];
        const setClauses = [];
        const params = [];
        let paramIdx = 1;
        for (const [key, column] of columns) {
            if (key in updates) {
                setClauses.push(`${column} = $${paramIdx++}`);
                params.push(updates[key] ?? null);
            }
        }
        if (setClauses.length === 0) {
            // Nothing to change, but still surface a missing schedule as an error.
            const existing = await q.query(`SELECT 1 FROM "${this.schemaName}".workflow_schedules WHERE schedule_name = $1`, [
                name,
            ]);
            if (existing.rows.length === 0) {
                throw new error_1.DBOSError(`Schedule '${name}' not found`);
            }
            return;
        }
        params.push(name);
        const result = await q.query(`UPDATE "${this.schemaName}".workflow_schedules SET ${setClauses.join(', ')} WHERE schedule_name = $${paramIdx}`, params);
        if (result.rowCount === 0) {
            throw new error_1.DBOSError(`Schedule '${name}' not found`);
        }
    }
    async updateLastFiredAt(name, lastFiredAt) {
        await this.pool.query(`UPDATE "${this.schemaName}".workflow_schedules SET last_fired_at = $1 WHERE schedule_name = $2`, [lastFiredAt, name]);
    }
    async applySchedules(schedules) {
        const client = await this.pool.connect();
        try {
            await client.query('BEGIN');
            for (const sched of schedules) {
                // Upsert on schedule_name; on conflict, preserve schedule_id and runtime state (status, last_fired_at) and update only the declared definition fields, so an unchanged re-apply is a no-op.
                await client.query(`INSERT INTO "${this.schemaName}".workflow_schedules
           (schedule_id, schedule_name, workflow_name, workflow_class_name, schedule, status, context, last_fired_at, automatic_backfill, cron_timezone, queue_name)
           VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
           ON CONFLICT (schedule_name) DO UPDATE SET
             workflow_name = EXCLUDED.workflow_name,
             workflow_class_name = EXCLUDED.workflow_class_name,
             schedule = EXCLUDED.schedule,
             context = EXCLUDED.context,
             automatic_backfill = EXCLUDED.automatic_backfill,
             cron_timezone = EXCLUDED.cron_timezone,
             queue_name = EXCLUDED.queue_name`, [
                    sched.scheduleId,
                    sched.scheduleName,
                    sched.workflowName,
                    sched.workflowClassName,
                    sched.schedule,
                    sched.status,
                    sched.context,
                    sched.lastFiredAt,
                    sched.automaticBackfill,
                    sched.cronTimezone,
                    sched.queueName,
                ]);
            }
            await client.query('COMMIT');
        }
        catch (e) {
            await client.query('ROLLBACK');
            throw e;
        }
        finally {
            client.release();
        }
    }
    // ==================== Application Versions ====================
    async createApplicationVersion(versionName) {
        const versionId = (0, crypto_1.randomUUID)();
        await this.pool.query(`INSERT INTO "${this.schemaName}".application_versions (version_id, version_name)
       VALUES ($1, $2)
       ON CONFLICT (version_name) DO NOTHING`, [versionId, versionName]);
    }
    async updateApplicationVersionTimestamp(versionName, newTimestamp) {
        await this.pool.query(`UPDATE "${this.schemaName}".application_versions
       SET version_timestamp = $1
       WHERE version_name = $2`, [newTimestamp, versionName]);
    }
    async listApplicationVersions() {
        const { rows } = await this.pool.query(`SELECT version_id, version_name, version_timestamp, created_at
       FROM "${this.schemaName}".application_versions
       ORDER BY version_timestamp DESC`);
        return rows.map((r) => ({
            versionId: r.version_id,
            versionName: r.version_name,
            versionTimestamp: Number(r.version_timestamp),
            createdAt: Number(r.created_at),
        }));
    }
    async getLatestApplicationVersion() {
        const { rows } = await this.pool.query(`SELECT version_id, version_name, version_timestamp, created_at
       FROM "${this.schemaName}".application_versions
       ORDER BY version_timestamp DESC
       LIMIT 1`);
        if (rows.length === 0) {
            throw new error_1.DBOSInitializationError('No application versions found');
        }
        const r = rows[0];
        return {
            versionId: r.version_id,
            versionName: r.version_name,
            versionTimestamp: Number(r.version_timestamp),
            createdAt: Number(r.created_at),
        };
    }
    // ==================== Queues ====================
    async getQueue(name) {
        const { rows } = await this.pool.query(`SELECT name, concurrency, worker_concurrency, rate_limit_max, rate_limit_period_sec,
              priority_enabled, partition_queue, polling_interval_sec
         FROM "${this.schemaName}".queues
        WHERE name = $1`, [name]);
        return rows.length === 0 ? null : queueRecordFromRow(rows[0]);
    }
    async listQueues() {
        const { rows } = await this.pool.query(`SELECT name, concurrency, worker_concurrency, rate_limit_max, rate_limit_period_sec,
              priority_enabled, partition_queue, polling_interval_sec
         FROM "${this.schemaName}".queues`);
        return rows.map(queueRecordFromRow);
    }
    async deleteQueue(name) {
        await this.pool.query(`DELETE FROM "${this.schemaName}".queues WHERE name = $1`, [name]);
    }
    async updateQueue(name, fields) {
        const setClauses = [];
        const params = [];
        let idx = 1;
        for (const [key, value] of Object.entries(fields)) {
            const column = QUEUE_COLUMN_BY_FIELD[key];
            setClauses.push(`"${column}" = $${idx++}`);
            params.push(value);
        }
        if (setClauses.length === 0)
            return;
        setClauses.push(`"updated_at" = $${idx++}`);
        params.push(Date.now());
        params.push(name);
        await this.pool.query(`UPDATE "${this.schemaName}".queues SET ${setClauses.join(', ')} WHERE name = $${idx}`, params);
    }
    /** Returns true iff this call inserted a new row (i.e. the queue did not
     * previously exist). False if the row already existed, regardless of
     * whether it was updated. */
    async upsertQueue(record, updateExisting) {
        const now = Date.now();
        const onConflict = updateExisting
            ? `ON CONFLICT (name) DO UPDATE SET
          concurrency = EXCLUDED.concurrency,
          worker_concurrency = EXCLUDED.worker_concurrency,
          rate_limit_max = EXCLUDED.rate_limit_max,
          rate_limit_period_sec = EXCLUDED.rate_limit_period_sec,
          priority_enabled = EXCLUDED.priority_enabled,
          partition_queue = EXCLUDED.partition_queue,
          polling_interval_sec = EXCLUDED.polling_interval_sec,
          updated_at = EXCLUDED.updated_at`
            : `ON CONFLICT (name) DO NOTHING`;
        const client = await this.pool.connect();
        try {
            await client.query('BEGIN');
            const existed = await client.query(`SELECT name FROM "${this.schemaName}".queues WHERE name = $1`, [record.name]);
            await client.query(`INSERT INTO "${this.schemaName}".queues
          (name, concurrency, worker_concurrency, rate_limit_max, rate_limit_period_sec,
           priority_enabled, partition_queue, polling_interval_sec, updated_at)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
         ${onConflict}`, [
                record.name,
                record.concurrency,
                record.workerConcurrency,
                record.rateLimitMax,
                record.rateLimitPeriodSec,
                record.priorityEnabled,
                record.partitionQueue,
                record.pollingIntervalSec,
                now,
            ]);
            await client.query('COMMIT');
            return existed.rowCount === 0;
        }
        catch (e) {
            await client.query('ROLLBACK');
            throw e;
        }
        finally {
            client.release();
        }
    }
    // ==================== Internal ====================
    async insertWorkflowStatus(client, initStatus, ownerXid, incrementAttempts = false) {
        try {
            const { rows } = await client.query(`INSERT INTO "${this.schemaName}".workflow_status (
          workflow_uuid,
          status,
          name,
          class_name,
          config_name,
          queue_name,
          authenticated_user,
          assumed_role,
          authenticated_roles,
          request,
          executor_id,
          application_version,
          application_id,
          created_at,
          recovery_attempts,
          updated_at,
          workflow_timeout_ms,
          workflow_deadline_epoch_ms,
          inputs,
          deduplication_id,
          priority,
          queue_partition_key,
          forked_from,
          parent_workflow_id,
          serialization,
          owner_xid,
          delay_until_epoch_ms,
          attributes,
          schedule_name,
          debounce_deadline_epoch_ms,
          is_debounced
        ) VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $26, $27, $28, $29, $30, $31, $32)
        ON CONFLICT (workflow_uuid)
          DO UPDATE SET
            recovery_attempts = CASE
              WHEN workflow_status.status != '${workflow_1.StatusString.ENQUEUED}' AND workflow_status.status != '${workflow_1.StatusString.DELAYED}'
              THEN workflow_status.recovery_attempts + $25
              ELSE workflow_status.recovery_attempts
            END,
            updated_at = EXCLUDED.updated_at,
            executor_id = CASE
              WHEN EXCLUDED.status != '${workflow_1.StatusString.ENQUEUED}' AND EXCLUDED.status != '${workflow_1.StatusString.DELAYED}'
              THEN EXCLUDED.executor_id
              ELSE workflow_status.executor_id
            END
          RETURNING recovery_attempts, status, name, class_name, config_name, queue_name, workflow_deadline_epoch_ms, executor_id, owner_xid, serialization`, [
                initStatus.workflowUUID,
                initStatus.status,
                initStatus.workflowName,
                // For cross-language compatibility, these variables MUST be NULL in the database when not set
                initStatus.workflowClassName === '' ? null : initStatus.workflowClassName,
                initStatus.workflowConfigName === '' ? null : initStatus.workflowConfigName,
                initStatus.queueName ?? null,
                initStatus.authenticatedUser,
                initStatus.assumedRole,
                JSON.stringify(initStatus.authenticatedRoles),
                JSON.stringify(initStatus.request),
                initStatus.executorId,
                initStatus.applicationVersion ?? null,
                initStatus.applicationID,
                initStatus.createdAt,
                initStatus.status === workflow_1.StatusString.ENQUEUED || initStatus.status === workflow_1.StatusString.DELAYED ? 0 : 1,
                initStatus.updatedAt ?? Date.now(),
                initStatus.timeoutMS ?? null,
                initStatus.deadlineEpochMS ?? null,
                initStatus.input ?? null,
                initStatus.deduplicationID ?? null,
                initStatus.priority,
                initStatus.queuePartitionKey ?? null,
                initStatus.forkedFrom ?? null,
                initStatus.parentWorkflowID ?? null,
                (incrementAttempts ?? false) ? 1 : 0,
                initStatus.serialization,
                ownerXid,
                initStatus.delayUntilEpochMS ?? null,
                initStatus.attributes ? JSON.stringify(initStatus.attributes) : null,
                initStatus.scheduleName ?? null,
                initStatus.debounceDeadlineEpochMS ?? null,
                initStatus.isDebounced ?? false,
            ]);
            if (rows.length === 0) {
                throw new Error(`Attempt to insert workflow ${initStatus.workflowUUID} failed`);
            }
            const ret = rows[0];
            ret.class_name = ret.class_name ?? '';
            ret.config_name = ret.config_name ?? '';
            initStatus.serialization = ret.serialization;
            return ret;
        }
        catch (error) {
            const err = error;
            if (err.code === '23505') {
                throw new error_1.DBOSQueueDuplicatedError(initStatus.workflowUUID, initStatus.queueName ?? '', initStatus.deduplicationID ?? '');
            }
            throw error;
        }
    }
    async getWorkflowStatusValue(client, workflowID) {
        const { rows } = await client.query(`SELECT status FROM "${this.schemaName}".workflow_status WHERE workflow_uuid=$1`, [workflowID]);
        return rows.length === 0 ? undefined : rows[0].status;
    }
    async updateWorkflowStatus(client, workflowID, status, options = {}) {
        // Use SQL now() so updated_at and completed_at (when set together) are
        // computed in the same statement against the same clock.
        const nowMsExpr = `(EXTRACT(EPOCH FROM now()) * 1000)::bigint`;
        let setClause = `SET status=$2, updated_at=${nowMsExpr}`;
        let whereClause = `WHERE workflow_uuid=$1`;
        const args = [workflowID, status];
        const update = options.update ?? {};
        if (update.output) {
            const param = args.push(update.output);
            setClause += `, output=$${param}`;
        }
        if (update.error) {
            const param = args.push(update.error);
            setClause += `, error=$${param}`;
        }
        if (update.resetRecoveryAttempts) {
            setClause += `, recovery_attempts = 0`;
        }
        if (update.resetDeadline) {
            setClause += `, workflow_deadline_epoch_ms = NULL`;
        }
        if (update.queueName !== undefined) {
            const param = args.push(update.queueName ?? undefined);
            setClause += `, queue_name=$${param}`;
        }
        if (update.resetDeduplicationID) {
            setClause += `, deduplication_id = NULL`;
        }
        if (update.resetStartedAtEpochMs) {
            setClause += `, started_at_epoch_ms = NULL`;
        }
        if (update.executorId !== undefined) {
            const param = args.push(update.executorId ?? undefined);
            setClause += `, executor_id=$${param}`;
        }
        if (update.resetNameTo !== undefined) {
            const param = args.push(update.resetNameTo ?? undefined);
            setClause += `, name=$${param}`;
        }
        if (update.setCompletedAt) {
            setClause += `, completed_at=${nowMsExpr}`;
        }
        else if (update.clearCompletedAt) {
            setClause += `, completed_at = NULL`;
        }
        const where = options.where ?? {};
        if (where.status) {
            const param = args.push(where.status);
            whereClause += ` AND status=$${param}`;
        }
        if (where.notStatus) {
            const param = args.push(where.notStatus);
            whereClause += ` AND status!=$${param}`;
        }
        const result = await client.query(`UPDATE "${this.schemaName}".workflow_status ${setClause} ${whereClause}`, args);
        // Completion wake: one row actually reached a terminal status here — wake any getResult() waiter.
        // This is the central status writer for SUCCESS/ERROR/MAX_RECOVERY (bulk CANCELLED wakes in
        // #cancelWorkflows), so terminal transitions are covered without a per-update trigger. The signal
        // is in-memory and coalesced; the notifier flushes it off the write path, after this txn commits.
        if (result.rowCount === 1 && TERMINAL_WORKFLOW_STATUSES.has(status)) {
            this.#signalWake(exports.DBOS_WORKFLOW_COMPLETION_CHANNEL, workflowID);
        }
        const throwOnFailure = options.throwOnFailure ?? true;
        if (throwOnFailure && result.rowCount !== 1) {
            throw new error_1.DBOSWorkflowConflictError(`Attempt to record transition of nonexistent workflow ${workflowID}`);
        }
    }
    async recordOperationResultInternal(client, workflowID, functionID, functionName, checkConflict, startTimeEpochMs, endTimeEpochMs, options = {}) {
        try {
            const out = await client.query(`INSERT INTO ${this.schemaName}.operation_outputs
         (workflow_uuid, function_id, output, error, function_name, child_workflow_id, started_at_epoch_ms, completed_at_epoch_ms, serialization)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
         ON CONFLICT (workflow_uuid, function_id) DO UPDATE
         SET completed_at_epoch_ms = operation_outputs.completed_at_epoch_ms
         RETURNING completed_at_epoch_ms;`, [
                workflowID,
                functionID,
                options.output ?? null,
                options.error ?? null,
                functionName,
                options.childWorkflowID ?? null,
                startTimeEpochMs,
                endTimeEpochMs,
                options.serialization ?? null,
            ]);
            if (checkConflict &&
                (out?.rowCount ?? 0) > 0 &&
                Number(out?.rows?.[0]?.completed_at_epoch_ms) !== endTimeEpochMs) {
                dbos_executor_1.DBOSExecutor.globalInstance?.logger.warn(`Step output for ${workflowID}(${functionID}):${functionName} already recorded`);
                throw new error_1.DBOSWorkflowConflictError(workflowID);
            }
        }
        catch (error) {
            const err = error;
            if (err.code === '40001' || err.code === '23505') {
                // Serialization and primary key conflict (Postgres).
                throw new error_1.DBOSWorkflowConflictError(workflowID);
            }
            else {
                throw err;
            }
        }
    }
    async #getOperationResultAndThrowIfCancelled(client, workflowID, functionID) {
        await this.#checkIfCanceled(client, workflowID);
        const { rows } = await client.query(`SELECT output, error, child_workflow_id, function_name, serialization
       FROM "${this.schemaName}".operation_outputs
      WHERE workflow_uuid=$1 AND function_id=$2`, [workflowID, functionID]);
        if (rows.length === 0) {
            return undefined;
        }
        else {
            return {
                output: rows[0].output,
                error: rows[0].error,
                childWorkflowID: rows[0].child_workflow_id,
                functionName: rows[0].function_name,
                // Return serialization so recv/getEvent replay deserializes with the stored format, not the default.
                serialization: rows[0].serialization,
            };
        }
    }
    async #runAndRecordResult(client, functionName, workflowID, functionID, func) {
        const startTime = Date.now();
        const result = await this.#getOperationResultAndThrowIfCancelled(client, workflowID, functionID);
        if (result !== undefined) {
            if (result.functionName !== functionName) {
                throw new error_1.DBOSUnexpectedStepError(workflowID, functionID, functionName, result.functionName);
            }
            return result.output;
        }
        const output = await func();
        await this.recordOperationResultInternal(client, workflowID, functionID, functionName, true, startTime, Date.now(), {
            output,
        });
        return output;
    }
    async #checkIfCanceled(client, workflowID) {
        const statusValue = await this.getWorkflowStatusValue(client, workflowID);
        if (statusValue === workflow_1.StatusString.CANCELLED) {
            throw new error_1.DBOSWorkflowCancelledError(workflowID);
        }
    }
    // Durably records (or, on recovery, reads back) the wakeup deadline for a sleep or
    // timeout so it survives recovery. Returns the absolute end time in epoch ms; the
    // caller is responsible for actually waiting until then. Throws if the workflow has
    // been cancelled.
    async #durableSleep(workflowID, functionID, durationMS) {
        const endTimeMs = Date.now() + durationMS;
        const client = await this.pool.connect();
        try {
            const res = await this.#getOperationResultAndThrowIfCancelled(client, workflowID, functionID);
            if (res) {
                if (res.functionName !== exports.DBOS_FUNCNAME_SLEEP) {
                    throw new error_1.DBOSUnexpectedStepError(workflowID, functionID, exports.DBOS_FUNCNAME_SLEEP, res.functionName);
                }
                return JSON.parse(res.output);
            }
            await this.recordOperationResultInternal(client, workflowID, functionID, exports.DBOS_FUNCNAME_SLEEP, false, Date.now(), Date.now(), {
                output: serialization_1.DBOSPortableJSON.stringify(endTimeMs),
                serialization: serialization_1.DBOSPortableJSON.name(),
            });
            return endTimeMs;
        }
        finally {
            client.release();
        }
    }
    /* BACKGROUND PROCESSES */
    /**
     * A background process that listens for notifications from Postgres then signals the appropriate
     * workflow listener by resolving its promise.
     */
    reconnectTimeout = null;
    async #listenForNotifications() {
        const connect = async () => {
            const reconnect = () => {
                if (this.reconnectTimeout) {
                    return;
                }
                this.reconnectTimeout = setTimeout(async () => {
                    this.reconnectTimeout = null;
                    await connect();
                }, 1000);
            };
            let client = null;
            try {
                client = await this.pool.connect();
                await client.query(`LISTEN ${exports.DBOS_NOTIFICATIONS_CHANNEL};`);
                await client.query(`LISTEN ${exports.DBOS_WORKFLOW_EVENTS_CHANNEL};`);
                await client.query(`LISTEN ${exports.DBOS_STREAMS_CHANNEL};`);
                // Wake channels are subscribed only when wakes are enabled, so a wakes-off deployment
                // opens no extra subscription. Both wakes remain gated on shouldUseDBNotifications above.
                if (this.wakeNotificationsEnabled) {
                    await client.query(`LISTEN ${exports.DBOS_QUEUE_WAKEUP_CHANNEL};`);
                    await client.query(`LISTEN ${exports.DBOS_WORKFLOW_COMPLETION_CHANNEL};`);
                }
                // Self-test: verify LISTEN actually works by sending a NOTIFY and checking it arrives.
                // If a transaction-mode pooler (e.g. PgBouncer pool_mode=transaction) is in the path,
                // LISTEN succeeds but the subscription is silently lost when the backend is released.
                let selfTestReceived = false;
                const onSelfTest = (msg) => {
                    if (msg.channel === 'dbos_notifications_channel' && msg.payload === 'dbos_listen_selftest') {
                        selfTestReceived = true;
                    }
                };
                client.on('notification', onSelfTest);
                await this.pool.query("NOTIFY dbos_notifications_channel, 'dbos_listen_selftest'");
                for (let i = 0; i < 30 && !selfTestReceived; i++) {
                    await new Promise((r) => setTimeout(r, 100));
                }
                client.removeListener('notification', onSelfTest);
                if (!selfTestReceived) {
                    this.logger.warn('LISTEN/NOTIFY self-test failed: notification was not received within 3 seconds. ' +
                        'This typically means the connection is going through a transaction-mode pooler ' +
                        '(e.g. PgBouncer with pool_mode=transaction), which silently breaks LISTEN/NOTIFY. ' +
                        'Workflow notifications will fall back to polling, which may increase latency.');
                }
                const handler = (msg) => {
                    if (!this.shouldUseDBNotifications)
                        return;
                    if (msg.channel === exports.DBOS_NOTIFICATIONS_CHANNEL && msg.payload) {
                        this.notificationsMap.callCallbacks(msg.payload);
                    }
                    else if (msg.channel === exports.DBOS_WORKFLOW_EVENTS_CHANNEL && msg.payload) {
                        this.workflowEventsMap.callCallbacks(msg.payload);
                    }
                    else if (msg.channel === exports.DBOS_STREAMS_CHANNEL && msg.payload) {
                        this.streamsMap.callCallbacks(msg.payload);
                    }
                    else if (msg.channel === exports.DBOS_QUEUE_WAKEUP_CHANNEL && msg.payload) {
                        // Payload is the woken queue name; deliver it as the callback event under the single key.
                        this.queueWakeMap.callCallbacks(exports.QUEUE_WAKEUP_KEY, msg.payload);
                    }
                    else if (msg.channel === exports.DBOS_WORKFLOW_COMPLETION_CHANNEL && msg.payload) {
                        // Payload is the completed workflow id; wake any getResult() waiter registered on it.
                        this.completionMap.callCallbacks(msg.payload);
                    }
                };
                client.on('notification', handler);
                client.on('error', (err) => {
                    this.logger.warn(`Error in notifications client: ${err}`);
                    if (client) {
                        client.removeAllListeners();
                        client.release(true);
                    }
                    reconnect();
                });
                this.notificationsClient = client;
            }
            catch (error) {
                this.logger.warn(`Error in notifications listener: ${String(error)}`);
                if (client) {
                    client.removeAllListeners();
                    client.release(true);
                }
                reconnect();
            }
        };
        await connect();
    }
}
exports.SystemDatabase = SystemDatabase;
__decorate([
    dbRetry(),
    __metadata("design:type", Function),
    __metadata("design:paramtypes", [Object, Object, Object]),
    __metadata("design:returntype", Promise)
], SystemDatabase.prototype, "initWorkflowStatus", null);
__decorate([
    dbRetry(),
    __metadata("design:type", Function),
    __metadata("design:paramtypes", [String, Object]),
    __metadata("design:returntype", Promise)
], SystemDatabase.prototype, "recordWorkflowOutput", null);
__decorate([
    dbRetry(),
    __metadata("design:type", Function),
    __metadata("design:paramtypes", [String, Object]),
    __metadata("design:returntype", Promise)
], SystemDatabase.prototype, "recordWorkflowError", null);
__decorate([
    dbRetry(),
    __metadata("design:type", Function),
    __metadata("design:paramtypes", [String, String, Number]),
    __metadata("design:returntype", Promise)
], SystemDatabase.prototype, "getWorkflowStatus", null);
__decorate([
    dbRetry(),
    __metadata("design:type", Function),
    __metadata("design:paramtypes", [String, Number]),
    __metadata("design:returntype", Promise)
], SystemDatabase.prototype, "getOperationResultAndThrowIfCancelled", null);
__decorate([
    dbRetry(),
    __metadata("design:type", Function),
    __metadata("design:paramtypes", [String, Number, String, Boolean, Number, Number, Object]),
    __metadata("design:returntype", Promise)
], SystemDatabase.prototype, "recordOperationResult", null);
__decorate([
    dbRetry(),
    __metadata("design:type", Function),
    __metadata("design:paramtypes", [String, Number, String, Boolean]),
    __metadata("design:returntype", Promise)
], SystemDatabase.prototype, "checkPatch", null);
__decorate([
    dbRetry(),
    __metadata("design:type", Function),
    __metadata("design:paramtypes", [String]),
    __metadata("design:returntype", Promise)
], SystemDatabase.prototype, "checkIfCanceled", null);
__decorate([
    dbRetry(),
    __metadata("design:type", Function),
    __metadata("design:paramtypes", [Object]),
    __metadata("design:returntype", Promise)
], SystemDatabase.prototype, "debounceDelayedWorkflowStandalone", null);
__decorate([
    dbRetry(),
    __metadata("design:type", Function),
    __metadata("design:paramtypes", [String, Number, String, Number, Number]),
    __metadata("design:returntype", Promise)
], SystemDatabase.prototype, "awaitWorkflowResult", null);
__decorate([
    dbRetry(),
    __metadata("design:type", Function),
    __metadata("design:paramtypes", [Array, String, Number]),
    __metadata("design:returntype", Promise)
], SystemDatabase.prototype, "awaitFirstWorkflowId", null);
__decorate([
    dbRetry(),
    __metadata("design:type", Function),
    __metadata("design:paramtypes", [Array, String, Number]),
    __metadata("design:returntype", Promise)
], SystemDatabase.prototype, "awaitWorkflowIds", null);
__decorate([
    dbRetry(),
    __metadata("design:type", Function),
    __metadata("design:paramtypes", [String, Number, Number]),
    __metadata("design:returntype", Promise)
], SystemDatabase.prototype, "durableSleepms", null);
__decorate([
    dbRetry(),
    __metadata("design:type", Function),
    __metadata("design:paramtypes", [String, Number, String, Object, Object, Object, String]),
    __metadata("design:returntype", Promise)
], SystemDatabase.prototype, "send", null);
__decorate([
    dbRetry(),
    __metadata("design:type", Function),
    __metadata("design:paramtypes", [String, Object, Object, Object, String]),
    __metadata("design:returntype", Promise)
], SystemDatabase.prototype, "sendDirect", null);
__decorate([
    dbRetry(),
    __metadata("design:type", Function),
    __metadata("design:paramtypes", [String, Number, Number, String, Number, Number]),
    __metadata("design:returntype", Promise)
], SystemDatabase.prototype, "recv", null);
__decorate([
    dbRetry(),
    __metadata("design:type", Function),
    __metadata("design:paramtypes", [String, Number, String, Object, Object]),
    __metadata("design:returntype", Promise)
], SystemDatabase.prototype, "setEvent", null);
__decorate([
    dbRetry(),
    __metadata("design:type", Function),
    __metadata("design:paramtypes", [String, String, Number, Object, Number]),
    __metadata("design:returntype", Promise)
], SystemDatabase.prototype, "getEvent", null);
__decorate([
    dbRetry(),
    __metadata("design:type", Function),
    __metadata("design:paramtypes", [String, String, String]),
    __metadata("design:returntype", Promise)
], SystemDatabase.prototype, "getEventDispatchState", null);
__decorate([
    dbRetry(),
    __metadata("design:type", Function),
    __metadata("design:paramtypes", [Object]),
    __metadata("design:returntype", Promise)
], SystemDatabase.prototype, "upsertEventDispatchState", null);
__decorate([
    dbRetry(),
    __metadata("design:type", Function),
    __metadata("design:paramtypes", [String, Number, String, String, Object]),
    __metadata("design:returntype", Promise)
], SystemDatabase.prototype, "writeStreamFromStep", null);
__decorate([
    dbRetry(),
    __metadata("design:type", Function),
    __metadata("design:paramtypes", [String, Number, String, String, Object, String]),
    __metadata("design:returntype", Promise)
], SystemDatabase.prototype, "writeStreamFromWorkflow", null);
__decorate([
    dbRetry(),
    __metadata("design:type", Function),
    __metadata("design:paramtypes", [String, String, Number]),
    __metadata("design:returntype", Promise)
], SystemDatabase.prototype, "readStreamValue", null);
__decorate([
    dbRetry(),
    __metadata("design:type", Function),
    __metadata("design:paramtypes", [String, String]),
    __metadata("design:returntype", Promise)
], SystemDatabase.prototype, "getDeduplicatedWorkflow", null);
__decorate([
    dbRetry(),
    __metadata("design:type", Function),
    __metadata("design:paramtypes", [String]),
    __metadata("design:returntype", Promise)
], SystemDatabase.prototype, "getQueuePartitions", null);
__decorate([
    dbRetry(),
    __metadata("design:type", Function),
    __metadata("design:paramtypes", [String, String]),
    __metadata("design:returntype", Promise)
], SystemDatabase.prototype, "getMetrics", null);
//# sourceMappingURL=system_database.js.map