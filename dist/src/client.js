"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.DBOSClient = exports.ClientHandle = void 0;
const system_database_1 = require("./system_database");
const logs_1 = require("./telemetry/logs");
const node_crypto_1 = require("node:crypto");
const workflow_1 = require("./workflow");
const utils_1 = require("./utils");
const dbos_1 = require("./dbos");
const serialization_1 = require("./serialization");
const workflow_management_1 = require("./workflow_management");
const dbos_executor_1 = require("./dbos-executor");
const error_1 = require("./error");
const scheduler_1 = require("./scheduler/scheduler");
const crontab_1 = require("./scheduler/crontab");
const wfqueue_1 = require("./wfqueue");
class ClientHandle {
    systemDatabase;
    workflowUUID;
    constructor(systemDatabase, workflowUUID) {
        this.systemDatabase = systemDatabase;
        this.workflowUUID = workflowUUID;
    }
    getWorkflowUUID() {
        return this.workflowUUID;
    }
    get workflowID() {
        return this.workflowUUID;
    }
    async getStatus() {
        const status = await this.systemDatabase.getWorkflowStatus(this.workflowUUID);
        return status ? (0, workflow_management_1.toWorkflowStatus)(status, this.systemDatabase.getSerializer()) : null;
    }
    async getResult(options) {
        const pollingIntervalMs = (0, dbos_1.resolvePollingIntervalMs)(options);
        const res = await this.systemDatabase.awaitWorkflowResult(this.workflowID, undefined, undefined, undefined, pollingIntervalMs);
        if (res?.cancelled) {
            throw new error_1.DBOSAwaitedWorkflowCancelledError(this.workflowID);
        }
        if (res?.maxRecoveryAttemptsExceeded) {
            throw new error_1.DBOSAwaitedWorkflowExceededMaxRecoveryAttempts(this.workflowID);
        }
        return await dbos_executor_1.DBOSExecutor.reviveResultOrError(res, this.systemDatabase.getSerializer());
    }
    async getWorkflowInputs() {
        const status = (await this.systemDatabase.getWorkflowStatus(this.workflowUUID));
        return (await (0, serialization_1.deserializePositionalArgs)(status.input, status.serialization, this.systemDatabase.getSerializer()));
    }
}
exports.ClientHandle = ClientHandle;
/**
 * DBOSClient is the main entry point for interacting with the DBOS system.
 */
class DBOSClient {
    serializer;
    logger;
    systemDatabase;
    constructor(systemDatabaseUrl, systemDatabasePool, serializer, systemDatabaseSchemaName, systemDatabasePoolSize, systemDatabasePollingConcurrency, logger) {
        this.serializer = serializer;
        this.logger = new logs_1.GlobalLogger(undefined, logger ? { logger } : undefined);
        this.systemDatabase = new system_database_1.SystemDatabase(systemDatabaseUrl, this.logger, serializer, systemDatabasePoolSize ?? system_database_1.DEFAULT_POOL_SIZE, systemDatabasePool, systemDatabaseSchemaName, 
        // The client does not run a background notifications listener
        false, systemDatabasePollingConcurrency);
    }
    /**
     * Creates a new instance of the DBOSClient.
     * @param systemDatabaseUrl - The connection string for the system database. This should include the hostname, port, username, password, and database name.
     * @param systemDatabasePool - An optional pre-configured connection pool to use for the system database. If provided, `systemDatabasePoolSize` is ignored.
     * @param serializer - An optional serializer for workflow inputs and outputs. Defaults to JSON serialization.
     * @param systemDatabaseSchemaName - An optional schema name for the system database. Defaults to `dbos`.
     * @param systemDatabasePoolSize - An optional maximum size for the system database connection pool. Defaults to {@link DEFAULT_POOL_SIZE}.
     * @param systemDatabasePollingConcurrency - An optional maximum number of concurrent polling operations. Defaults to half the pool size (minimum 1).
     * @param logger - An optional custom logger to which the client directs all its logging, replacing the built-in console logger.
     * @returns A Promise that resolves with the DBOSClient instance.
     */
    static async create({ systemDatabaseUrl, systemDatabasePool, serializer, systemDatabaseSchemaName, systemDatabasePoolSize, systemDatabasePollingConcurrency, logger, }) {
        const client = new DBOSClient(systemDatabaseUrl, systemDatabasePool, serializer ?? serialization_1.DBOSJSON, systemDatabaseSchemaName, systemDatabasePoolSize, systemDatabasePollingConcurrency, logger);
        return Promise.resolve(client);
    }
    /**
     * Destroys the underlying database connection.
     * This should be called when the client is no longer needed to clean up resources.
     * @returns A Promise that resolves when database connection is destroyed.
     */
    async destroy() {
        await this.systemDatabase.destroy();
    }
    /**
     * Enqueues a workflow for execution.
     * @param options - Options for the enqueue operation, including queue name, workflow name, and other parameters.
     * @param args - Arguments to pass to the workflow upon execution.
     * @returns A Promise that resolves when enqueue is complete, providing a handle to the enqueued workflow.
     */
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    async enqueue(options, ...args) {
        const internalStatus = await this.#buildEnqueueStatus(options, args);
        let finalID;
        if (options.duplicationPolicy === 'return-existing') {
            finalID = await this.#initSingletonWorkflow(internalStatus, options);
        }
        else {
            await this.systemDatabase.initWorkflowStatus(internalStatus, null);
            finalID = internalStatus.workflowUUID;
        }
        return new ClientHandle(this.systemDatabase, finalID);
    }
    async #buildEnqueueStatus(options, args) {
        (0, workflow_1.validateWorkflowAttributes)(options.attributes);
        const { workflowName, workflowClassName, workflowConfigName, queueName, appVersion } = options;
        const workflowUUID = options.workflowID ?? (0, node_crypto_1.randomUUID)();
        const serparam = await (0, serialization_1.serializeArgs)(args, undefined, this.serializer, options?.serializationType);
        const delayUntilEpochMS = options.delaySeconds !== undefined && options.delaySeconds > 0
            ? Date.now() + options.delaySeconds * 1000
            : undefined;
        return {
            workflowUUID: workflowUUID,
            status: delayUntilEpochMS !== undefined ? workflow_1.StatusString.DELAYED : workflow_1.StatusString.ENQUEUED,
            workflowName: workflowName,
            workflowClassName: workflowClassName ?? '',
            workflowConfigName: workflowConfigName ?? '',
            queueName: queueName,
            authenticatedUser: '',
            output: null,
            error: null,
            assumedRole: '',
            authenticatedRoles: [],
            request: {},
            executorId: '',
            applicationVersion: appVersion,
            applicationID: '',
            createdAt: Date.now(),
            timeoutMS: options.workflowTimeoutMS,
            deadlineEpochMS: undefined,
            input: serparam.serializedValue,
            deduplicationID: options.deduplicationID,
            priority: options.priority ?? 0,
            queuePartitionKey: options.queuePartitionKey,
            serialization: serparam.serialization,
            delayUntilEpochMS,
            attributes: options.attributes,
        };
    }
    /**
     * Enqueue a debounced workflow for `DebouncerClient`.
     * The debounce fields are stamped onto the built status here because they are
     * not part of the public enqueue API.
     * @internal
     */
    async enqueueDebounced(options, debounceDeadlineEpochMS, args) {
        const internalStatus = await this.#buildEnqueueStatus(options, args);
        internalStatus.debounceDeadlineEpochMS = debounceDeadlineEpochMS;
        internalStatus.isDebounced = true;
        if (internalStatus.delayUntilEpochMS !== undefined &&
            debounceDeadlineEpochMS !== undefined &&
            debounceDeadlineEpochMS < internalStatus.delayUntilEpochMS) {
            internalStatus.delayUntilEpochMS = debounceDeadlineEpochMS;
        }
        await this.systemDatabase.initWorkflowStatus(internalStatus, null);
        return internalStatus.workflowUUID;
    }
    /**
     * Extend an existing debounced DELAYED workflow, for `DebouncerClient`.
     * @internal
     */
    async debounceDelayedWorkflow(params) {
        return await this.systemDatabase.debounceDelayedWorkflow(params);
    }
    /**
     * Insert the workflow status row under the `'return-existing'` duplication policy: a retry
     * loop that catches `DBOSQueueDuplicatedError`, looks up the active workflow holding the
     * dedup slot, and returns its UUID. Falls back to retry if the slot was cleared between
     * INSERT and lookup (the prior workflow completed or was cancelled mid-flight).
     */
    async #initSingletonWorkflow(internalStatus, options) {
        if (!options.deduplicationID) {
            throw new error_1.DBOSInvalidWorkflowTransitionError("`duplicationPolicy: 'return-existing'` requires `deduplicationID`");
        }
        while (true) {
            try {
                await this.systemDatabase.initWorkflowStatus(internalStatus, null);
                return internalStatus.workflowUUID;
            }
            catch (e) {
                if (!(e instanceof error_1.DBOSQueueDuplicatedError))
                    throw e;
                const existingID = await this.systemDatabase.getDeduplicatedWorkflow(options.queueName, options.deduplicationID);
                if (existingID)
                    return existingID;
            }
        }
    }
    /**
     * Enqueues a workflow for execution, where the workflow function definition is not
     *   available and may be implemented in another language.
     * @param options - Options for the enqueue operation, including queue name, workflow name, and other parameters.
     * @param positionalArgs - Array of positional arguments to pass to the workflow upon execution.
     * @param namedArgs - Optional object containing named arguments for the target workflow (useful mainly for calling Python functions with kwargs)
     * @returns A Promise that resolves when enqueue is complete, providing a handle to the enqueued workflow.
     */
    async enqueuePortable(options, positionalArgs, namedArgs) {
        (0, workflow_1.validateWorkflowAttributes)(options.attributes);
        const { workflowName, workflowClassName, workflowConfigName, queueName, appVersion } = options;
        const workflowUUID = options.workflowID ?? (0, node_crypto_1.randomUUID)();
        const serparam = await (0, serialization_1.serializeArgs)(positionalArgs, namedArgs, this.serializer, options?.serializationType ?? 'portable');
        const delayUntilEpochMS = options.delaySeconds !== undefined && options.delaySeconds > 0
            ? Date.now() + options.delaySeconds * 1000
            : undefined;
        const internalStatus = {
            workflowUUID: workflowUUID,
            status: delayUntilEpochMS !== undefined ? workflow_1.StatusString.DELAYED : workflow_1.StatusString.ENQUEUED,
            workflowName: workflowName,
            workflowClassName: workflowClassName ?? '',
            workflowConfigName: workflowConfigName ?? '',
            queueName: queueName,
            authenticatedUser: '',
            output: null,
            error: null,
            assumedRole: '',
            authenticatedRoles: [],
            request: {},
            executorId: '',
            applicationVersion: appVersion,
            applicationID: '',
            createdAt: Date.now(),
            timeoutMS: options.workflowTimeoutMS,
            deadlineEpochMS: undefined,
            input: serparam.serializedValue,
            deduplicationID: options.deduplicationID,
            priority: options.priority ?? 0,
            queuePartitionKey: options.queuePartitionKey,
            serialization: serparam.serialization,
            delayUntilEpochMS,
            attributes: options.attributes,
        };
        let finalID;
        if (options.duplicationPolicy === 'return-existing') {
            finalID = await this.#initSingletonWorkflow(internalStatus, options);
        }
        else {
            await this.systemDatabase.initWorkflowStatus(internalStatus, null);
            finalID = internalStatus.workflowUUID;
        }
        return new ClientHandle(this.systemDatabase, finalID);
    }
    /**
     * Register a workflow queue and persist its configuration in the system
     * database. The returned queue's `set` methods write through this
     * client's database.
     *
     * Defaults `onConflict` to `'always_update'` because clients are not
     * associated with an application version.
     */
    async registerQueue(name, options = {}) {
        const { onConflict = 'always_update', ...params } = options;
        if (onConflict === 'update_if_latest_version') {
            throw new Error("DBOSClient.registerQueue does not support onConflict='update_if_latest_version' " +
                'because clients are not associated with an application version. ' +
                "Use 'always_update' or 'never_update'.");
        }
        wfqueue_1.WorkflowQueue.validateQueueParams(params);
        const updateExisting = onConflict === 'always_update';
        const record = wfqueue_1.WorkflowQueue.recordFromParams(name, params);
        const inserted = await this.systemDatabase.upsertQueue(record, updateExisting);
        const persisted = await this.systemDatabase.getQueue(name);
        if (persisted === null) {
            throw new Error(`Queue '${name}' missing from database after upsert`);
        }
        const queue = wfqueue_1.WorkflowQueue._fromRecord(persisted, this.systemDatabase);
        if (inserted) {
            this.logger.info(`Registered new queue:`);
            (0, wfqueue_1.logQueue)(this.logger, queue);
        }
        return queue;
    }
    /** Retrieve a database-backed queue by name, or `null` if no row exists. */
    async retrieveQueue(name) {
        const record = await this.systemDatabase.getQueue(name);
        return record === null ? null : wfqueue_1.WorkflowQueue._fromRecord(record, this.systemDatabase);
    }
    /** Delete a database-backed queue. Pending workflows on it are unrecoverable. */
    async deleteQueue(name) {
        await this.systemDatabase.deleteQueue(name);
    }
    /**
     * Sends a message to a workflow, identified by destinationID.
     * @param destinationID - The ID of the destination workflow.
     * @param message - The message to send. This can be any serializable object.
     * @param topic - An optional topic to send the message to. If not provided, the default topic will be used.
     * @param idempotencyKey - An optional idempotency key to ensure that the message is only sent once per destination.
     * @returns A Promise that resolves when the message has been sent.
     */
    async send(destinationID, message, topic, idempotencyKey, options) {
        const sermsg = await (0, serialization_1.serializeValue)(message, this.serializer, options?.serializationType);
        await this.systemDatabase.sendDirect(destinationID, sermsg.serializedValue, topic, sermsg.serialization, idempotencyKey);
    }
    /**
     * Retrieves an event published by workflowID for a given key.
     * @param workflowID - The ID of the workflow that published the event.
     * @param key - The key associated with the event you want to retrieve.
     * @param options - {@link GetEventOptions} controlling timeout or deadline; if neither is set, times out after 60 seconds
     * @returns A Promise that resolves with the event payload.
     */
    async getEvent(workflowID, key, options) {
        const timeoutSeconds = (0, dbos_1.resolveTimeoutSeconds)(options);
        const pollingIntervalMs = (0, dbos_1.resolvePollingIntervalMs)(options);
        const evt = await this.systemDatabase.getEvent(workflowID, key, timeoutSeconds ?? 60, undefined, pollingIntervalMs);
        return (await (0, serialization_1.deserializeValue)(evt.serializedValue, evt.serialization, this.serializer));
    }
    /**
     * Retrieves a single workflow by its id.
     * @param workflowID - The ID of the workflow to retrieve.
     * @returns a WorkflowHandle that represents the retrieved workflow.
     */
    retrieveWorkflow(workflowID) {
        return new ClientHandle(this.systemDatabase, workflowID);
    }
    cancelWorkflow(workflowID, options) {
        return this.systemDatabase.cancelWorkflows([workflowID], options?.cancelChildren);
    }
    cancelWorkflows(workflowIDs, options) {
        return this.systemDatabase.cancelWorkflows(workflowIDs, options?.cancelChildren);
    }
    resumeWorkflow(workflowID, options) {
        return this.systemDatabase.resumeWorkflows([workflowID], options?.queueName);
    }
    resumeWorkflows(workflowIDs, options) {
        return this.systemDatabase.resumeWorkflows(workflowIDs, options?.queueName);
    }
    setWorkflowPriority(workflowID, priority) {
        return this.systemDatabase.setWorkflowPriority(workflowID, priority);
    }
    setWorkflowDelay(workflowID, options) {
        const delayUntilEpochMS = (0, dbos_1.resolveDelayEpochMS)(options);
        return this.systemDatabase.setWorkflowDelay(workflowID, delayUntilEpochMS);
    }
    deleteWorkflow(workflowID, deleteChildren = false) {
        return this.systemDatabase.deleteWorkflows([workflowID], deleteChildren);
    }
    deleteWorkflows(workflowIDs, deleteChildren = false) {
        return this.systemDatabase.deleteWorkflows(workflowIDs, deleteChildren);
    }
    forkWorkflow(workflowID, startStep, options) {
        return (0, workflow_management_1.forkWorkflow)(this.systemDatabase, workflowID, startStep, options);
    }
    getWorkflow(workflowID) {
        return (0, workflow_management_1.getWorkflow)(this.systemDatabase, workflowID);
    }
    listWorkflows(input) {
        return (0, workflow_management_1.listWorkflows)(this.systemDatabase, input);
    }
    listQueuedWorkflows(input) {
        return (0, workflow_management_1.listQueuedWorkflows)(this.systemDatabase, input);
    }
    listWorkflowSteps(workflowID, options) {
        return (0, workflow_management_1.listWorkflowSteps)(this.systemDatabase, workflowID, true, options);
    }
    async waitFirst(handles, options) {
        const pollingIntervalMs = (0, dbos_1.resolvePollingIntervalMs)(options);
        if (handles.length === 0) {
            throw new Error('handles must not be empty');
        }
        const handleMap = new Map();
        for (const handle of handles) {
            if (handleMap.has(handle.workflowID)) {
                throw new Error(`Duplicate workflow ID in waitFirst: ${handle.workflowID}`);
            }
            handleMap.set(handle.workflowID, handle);
        }
        const completedId = await this.systemDatabase.awaitFirstWorkflowId([...handleMap.keys()], undefined, pollingIntervalMs);
        return handleMap.get(completedId);
    }
    async waitAll(handles, options) {
        if (handles.length === 0) {
            return [];
        }
        const pollingIntervalMs = (0, dbos_1.resolvePollingIntervalMs)(options);
        const workflowIds = [...new Set(handles.map((handle) => handle.workflowID))];
        await this.systemDatabase.awaitWorkflowIds(workflowIds, undefined, pollingIntervalMs);
        return handles;
    }
    /**
     * Read values from a stream as an async generator.
     * This function reads values from a stream identified by the workflowID and key,
     * yielding each value in order until the stream is closed or the workflow terminates.
     * @param workflowID - The ID of the workflow that wrote to the stream
     * @param key - The stream key to read from
     * @returns An async generator that yields each value in the stream until the stream is closed
     */
    async *readStream(workflowID, key) {
        const payload = `${workflowID}::${key}`;
        let offset = 0;
        let finalRead = false;
        while (true) {
            // Register a listener before reading so a notification arriving between the
            // read and the wait below is not lost; a fresh promise per iteration gives
            // the "clear before reading" semantics. The client does not run a
            // notification listener thread, so this is never signaled and the wait
            // always falls back to the polling interval below.
            let resolveNotification;
            const messagePromise = new Promise((resolve) => {
                resolveNotification = resolve;
            });
            const cbr = this.systemDatabase.streamsMap.registerCallback(payload, resolveNotification);
            try {
                // One round trip for both the value and the workflow's status.
                const { status, value } = await this.systemDatabase.readStreamValue(workflowID, key, offset);
                if (status === null) {
                    // An unknown workflow ends the generator quietly (the in-process reader raises instead).
                    break;
                }
                if (value !== undefined) {
                    if (value.serializedValue === system_database_1.DBOS_STREAM_CLOSED_SENTINEL) {
                        return;
                    }
                    yield (await (0, serialization_1.deserializeValue)(value.serializedValue, value.serialization, this.serializer));
                    offset += 1;
                    // More may be buffered; read the next offset before waiting.
                    continue;
                }
                if (finalRead) {
                    break;
                }
                // No value yet: stop if the workflow is done, else wait for a notification (bounded by the poll interval so termination is noticed).
                if (!(0, workflow_1.isWorkflowActive)(status)) {
                    // Cancel/timeout set a terminal status while the workflow may still be writing, so drain to the first empty offset before stopping.
                    finalRead = true;
                    continue;
                }
                const { promise, cancel } = (0, utils_1.cancellableSleep)(1000); // 1 second polling fallback
                try {
                    await Promise.race([messagePromise, promise]);
                }
                finally {
                    cancel();
                }
            }
            finally {
                this.systemDatabase.streamsMap.deregisterCallback(cbr);
            }
        }
    }
    // ---------------------------------------------------------------------------
    // Dynamic Workflow Schedules
    // ---------------------------------------------------------------------------
    async createSchedule(options) {
        (0, crontab_1.validateCrontab)(options.schedule);
        if (options.options?.cronTimezone) {
            (0, crontab_1.validateTimezone)(options.options.cronTimezone);
        }
        const schedInternal = {
            scheduleId: (0, scheduler_1.createScheduleId)(),
            scheduleName: options.scheduleName,
            workflowName: options.workflowName,
            workflowClassName: options.workflowClassName ?? '',
            schedule: options.schedule,
            status: 'ACTIVE',
            context: await this.serializer.stringify(options.context),
            lastFiredAt: null,
            automaticBackfill: options.options?.automaticBackfill ?? false,
            cronTimezone: options.options?.cronTimezone ?? null,
            queueName: options.options?.queueName ?? null,
        };
        await this.systemDatabase.createSchedule(schedInternal);
    }
    async listSchedules(filters) {
        const results = await this.systemDatabase.listSchedules(filters);
        return await Promise.all(results.map((r) => (0, scheduler_1.toWorkflowSchedule)(r, this.serializer)));
    }
    async getSchedule(name) {
        const result = await this.systemDatabase.getSchedule(name);
        return result ? await (0, scheduler_1.toWorkflowSchedule)(result, this.serializer) : null;
    }
    async deleteSchedule(name) {
        await this.systemDatabase.deleteSchedule(name);
    }
    async pauseSchedule(name) {
        await this.systemDatabase.setScheduleStatus(name, 'PAUSED');
    }
    async resumeSchedule(name) {
        await this.systemDatabase.setScheduleStatus(name, 'ACTIVE');
    }
    async updateSchedule(name, updates) {
        if (updates.schedule !== undefined) {
            (0, crontab_1.validateCrontab)(updates.schedule);
        }
        if (updates.cronTimezone) {
            (0, crontab_1.validateTimezone)(updates.cronTimezone);
        }
        // Only the keys the caller provided are updated. An `undefined` value leaves a field unchanged; `null` clears a nullable field. Including `context` (even as `undefined`) sets it, since `undefined` is a valid empty context.
        const internalUpdates = {};
        if (updates.schedule !== undefined)
            internalUpdates.schedule = updates.schedule;
        if ('context' in updates)
            internalUpdates.context = await this.serializer.stringify(updates.context);
        if (updates.automaticBackfill !== undefined)
            internalUpdates.automaticBackfill = updates.automaticBackfill;
        if (updates.cronTimezone !== undefined)
            internalUpdates.cronTimezone = updates.cronTimezone;
        if (updates.queueName !== undefined)
            internalUpdates.queueName = updates.queueName;
        await this.systemDatabase.updateSchedule(name, internalUpdates);
    }
    async applySchedules(schedules) {
        const internals = [];
        for (const sched of schedules) {
            (0, crontab_1.validateCrontab)(sched.schedule);
            if (sched.cronTimezone) {
                (0, crontab_1.validateTimezone)(sched.cronTimezone);
            }
            internals.push({
                scheduleId: (0, scheduler_1.createScheduleId)(),
                scheduleName: sched.scheduleName,
                workflowName: sched.workflowName,
                workflowClassName: sched.workflowClassName ?? '',
                schedule: sched.schedule,
                status: 'ACTIVE',
                context: await this.serializer.stringify(sched.context),
                lastFiredAt: null,
                automaticBackfill: sched.automaticBackfill ?? false,
                cronTimezone: sched.cronTimezone ?? null,
                queueName: sched.queueName ?? null,
            });
        }
        await this.systemDatabase.applySchedules(internals);
    }
    async triggerSchedule(name) {
        const workflowID = await (0, scheduler_1.triggerSchedule)(this.systemDatabase, this.serializer, name);
        return new ClientHandle(this.systemDatabase, workflowID);
    }
    async backfillSchedule(name, start, end) {
        const workflowIDs = await (0, scheduler_1.backfillSchedule)(this.systemDatabase, this.serializer, name, start, end);
        return workflowIDs.map((id) => new ClientHandle(this.systemDatabase, id));
    }
    // ==================== Application Versions ====================
    async listApplicationVersions() {
        return this.systemDatabase.listApplicationVersions();
    }
    async getLatestApplicationVersion() {
        return this.systemDatabase.getLatestApplicationVersion();
    }
    async setLatestApplicationVersion(versionName) {
        await this.systemDatabase.updateApplicationVersionTimestamp(versionName, Date.now());
    }
}
exports.DBOSClient = DBOSClient;
//# sourceMappingURL=client.js.map