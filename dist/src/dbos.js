"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.DBOS = exports.runTransactionalInternalStep = exports.runInternalStep = exports.getExecutor = exports.resolveDelayEpochMS = exports.resolvePollingIntervalMs = exports.resolveTimeoutSeconds = void 0;
const context_1 = require("./context");
const dbos_executor_1 = require("./dbos-executor");
const traces_1 = require("./telemetry/traces");
const workflow_1 = require("./workflow");
const logs_1 = require("./telemetry/logs");
const error_1 = require("./error");
const config_1 = require("./config");
const scheduler_decorator_1 = require("./scheduler/scheduler_decorator");
const decorators_1 = require("./decorators");
const utils_1 = require("./utils");
const serialization_1 = require("./serialization");
const adminserver_1 = require("./adminserver");
const node_crypto_1 = require("node:crypto");
const step_1 = require("./step");
const conductor_1 = require("./conductor/conductor");
const system_database_1 = require("./system_database");
const scheduler_1 = require("./scheduler/scheduler");
const crontab_1 = require("./scheduler/crontab");
const wfqueue_1 = require("./wfqueue");
const authdecorators_1 = require("./authdecorators");
const node_assert_1 = __importDefault(require("node:assert"));
function resolveTimeoutSeconds(options) {
    if (options === undefined)
        return undefined;
    if (typeof options === 'number')
        return options;
    if (options.deadlineEpochMS !== undefined) {
        return Math.max(0, (options.deadlineEpochMS - Date.now()) / 1000);
    }
    return options.timeoutSeconds;
}
exports.resolveTimeoutSeconds = resolveTimeoutSeconds;
function resolvePollingIntervalMs(options) {
    if (options === undefined || typeof options === 'number')
        return undefined;
    const interval = options.pollingIntervalMs;
    if (interval === undefined)
        return undefined;
    if (!Number.isFinite(interval) || interval <= 0) {
        throw new error_1.DBOSError('pollingIntervalMs must be a positive finite number');
    }
    return interval;
}
exports.resolvePollingIntervalMs = resolvePollingIntervalMs;
function resolveDelayEpochMS(options) {
    if (typeof options === 'number') {
        if (options <= 0)
            throw new error_1.DBOSError('delaySeconds must be greater than 0');
        return Date.now() + options * 1000;
    }
    if (options.delayUntilEpochMS !== undefined) {
        if (options.delayUntilEpochMS <= 0)
            throw new error_1.DBOSError('delayUntilEpochMS must be greater than 0');
        return options.delayUntilEpochMS;
    }
    if (options.delaySeconds !== undefined) {
        if (options.delaySeconds <= 0)
            throw new error_1.DBOSError('delaySeconds must be greater than 0');
        return Date.now() + options.delaySeconds * 1000;
    }
    throw new error_1.DBOSError('SetWorkflowDelayOptions must specify either delaySeconds or delayUntilEpochMS');
}
exports.resolveDelayEpochMS = resolveDelayEpochMS;
function getExecutor() {
    if (!dbos_executor_1.DBOSExecutor.globalInstance) {
        throw new error_1.DBOSExecutorNotInitializedError();
    }
    return dbos_executor_1.DBOSExecutor.globalInstance;
}
exports.getExecutor = getExecutor;
function runInternalStep(callback, funcName, childWFID, assignedFuncID) {
    if (DBOS.isWithinWorkflow()) {
        if (DBOS.isInStep()) {
            // OK to use directly
            return callback();
        }
        else if (DBOS.isInWorkflow()) {
            return dbos_executor_1.DBOSExecutor.globalInstance.runInternalStep(callback, funcName, DBOS.workflowID, assignedFuncID ?? (0, context_1.functionIDGetIncrement)(), childWFID);
        }
        else {
            throw new error_1.DBOSInvalidWorkflowTransitionError(`Invalid call to \`${funcName}\` inside a \`transaction\``);
        }
    }
    return callback();
}
exports.runInternalStep = runInternalStep;
/**
 * Like runInternalStep, but when called from within a workflow, the callback and the step
 * result recording run in the same database transaction (via runTransactionalStep).
 * The callback receives a PoolClient that should be passed to any SystemDatabase
 * methods so they participate in the same transaction.
 * Outside a workflow, the callback is called directly with `undefined` as the client.
 */
async function runTransactionalInternalStep(callback, funcName) {
    if (DBOS.isWithinWorkflow()) {
        if (DBOS.isInStep()) {
            return callback(undefined);
        }
        else if (DBOS.isInWorkflow()) {
            const executor = dbos_executor_1.DBOSExecutor.globalInstance;
            // Reserve the function ID synchronously, before any await.
            const functionID = (0, context_1.functionIDGetIncrement)();
            let freshResult;
            const stored = await executor.systemDatabase.runTransactionalStep(DBOS.workflowID, functionID, funcName, async (client) => {
                freshResult = await callback(client);
                return await executor.serializer.stringify(freshResult !== undefined ? freshResult : null);
            });
            if (stored !== undefined) {
                if (stored.functionName !== funcName) {
                    throw new error_1.DBOSUnexpectedStepError(DBOS.workflowID, functionID, funcName, stored.functionName);
                }
                return await dbos_executor_1.DBOSExecutor.reviveResultOrError(stored, executor.serializer);
            }
            return freshResult;
        }
        else {
            throw new error_1.DBOSInvalidWorkflowTransitionError(`Invalid call to \`${funcName}\` inside a \`transaction\``);
        }
    }
    return callback(undefined);
}
exports.runTransactionalInternalStep = runTransactionalInternalStep;
class DBOS {
    ///////
    // Lifecycle
    ///////
    static adminServer = undefined;
    static conductor = undefined;
    /**
     * Set configuration of `DBOS` prior to `launch`
     * @param config - configuration of services needed by DBOS
     */
    static setConfig(config) {
        (0, node_assert_1.default)(!DBOS.isInitialized(), 'Cannot call DBOS.setConfig after DBOS.launch');
        DBOS.#dbosConfig = config;
    }
    /**
     * Check if DBOS has been `launch`ed (and not `shutdown`)
     * @returns `true` if DBOS has been launched, or `false` otherwise
     */
    static isInitialized() {
        return !!dbos_executor_1.DBOSExecutor.globalInstance?.initialized;
    }
    /**
     * Launch DBOS, starting recovery and request handling
     * @param options - Launch options for connecting to DBOS Conductor
     */
    static async launch(options) {
        const configFile = await (0, config_1.readConfigFile)();
        let internalConfig = DBOS.#dbosConfig ? (0, config_1.translateDbosConfig)(DBOS.#dbosConfig) : (0, config_1.getDbosConfig)(configFile);
        let runtimeConfig = DBOS.#dbosConfig ? (0, config_1.translateRuntimeConfig)(DBOS.#dbosConfig) : (0, config_1.getRuntimeConfig)(configFile);
        if (process.env.DBOS__CLOUD === 'true') {
            [internalConfig, runtimeConfig] = (0, config_1.overwriteConfigForDBOSCloud)(internalConfig, runtimeConfig, configFile);
        }
        utils_1.globalParams.enableOTLP = DBOS.#dbosConfig?.enableOTLP ?? (0, utils_1.defaultEnableOTLP)();
        utils_1.globalParams.tracingEnabled = DBOS.#dbosConfig?.tracingEnabled || utils_1.globalParams.enableOTLP;
        if (!(0, traces_1.isTraceContextWorking)())
            (0, traces_1.installTraceContextManager)(internalConfig.name);
        // Do nothing if DBOS is already initialized
        if (DBOS.isInitialized()) {
            return;
        }
        (0, decorators_1.finalizeClassRegistrations)();
        (0, decorators_1.insertAllMiddleware)();
        // Globally set the application version and executor ID.
        // In DBOS Cloud, instead use the value supplied through environment variables.
        if (process.env.DBOS__CLOUD !== 'true') {
            if (DBOS.#dbosConfig?.applicationVersion) {
                utils_1.globalParams.appVersion = DBOS.#dbosConfig.applicationVersion;
            }
            else if (DBOS.#dbosConfig?.enablePatching) {
                utils_1.globalParams.appVersion = 'PATCHING_ENABLED';
            }
            if (DBOS.#dbosConfig?.executorID) {
                utils_1.globalParams.executorID = DBOS.#dbosConfig.executorID;
            }
        }
        if (options?.conductorKey) {
            // Always use a generated executor ID in Conductor.
            utils_1.globalParams.executorID = (0, node_crypto_1.randomUUID)();
        }
        dbos_executor_1.DBOSExecutor.createInternalQueue();
        dbos_executor_1.DBOSExecutor.globalInstance = new dbos_executor_1.DBOSExecutor(internalConfig);
        (0, decorators_1.recordDBOSLaunch)();
        const executor = dbos_executor_1.DBOSExecutor.globalInstance;
        // Initialize data sources before executor.init() dispatches recovery, so recovered
        // workflows can run their transactions immediately instead of racing initialization.
        for (const [_n, ds] of decorators_1.transactionalDataSources) {
            await ds.initialize();
        }
        await executor.init();
        // Register the current application version
        await executor.systemDatabase.createApplicationVersion(utils_1.globalParams.appVersion);
        const latest = await executor.systemDatabase.getLatestApplicationVersion();
        if (latest.versionName !== utils_1.globalParams.appVersion) {
            executor.logger.warn(`Current version '${utils_1.globalParams.appVersion}' is not the latest version. Latest version is '${latest.versionName}'.`);
        }
        await dbos_executor_1.DBOSExecutor.globalInstance.initEventReceivers(this.#dbosConfig?.listenQueues || null);
        if (utils_1.globalParams.dbosCloud) {
            const cloudAppName = process.env.DBOS__CONDUCTOR_APP_NAME;
            const cloudConductorKey = process.env.DBOS__CONDUCTOR_KEY;
            const cloudConductorURL = process.env.DBOS__CONDUCTOR_URL;
            if (cloudAppName && cloudConductorKey && cloudConductorURL) {
                DBOS.logger.debug('Starting Conductor connection (DBOS Cloud)');
                DBOS.conductor = new conductor_1.Conductor(dbos_executor_1.DBOSExecutor.globalInstance, cloudAppName, cloudConductorKey, cloudConductorURL);
                DBOS.conductor.dispatchLoop();
            }
        }
        else if (options?.conductorKey) {
            if (!options.conductorURL) {
                const dbosDomain = process.env.DBOS_DOMAIN || 'cloud.dbos.dev';
                options.conductorURL = `wss://${dbosDomain}/conductor/v1alpha1`;
            }
            const executorMetadata = options.conductorExecutorMetadata;
            if (executorMetadata !== undefined) {
                // Validate that the metadata is JSON-serializable
                try {
                    JSON.stringify(executorMetadata);
                }
                catch (e) {
                    throw new error_1.DBOSError(`conductorExecutorMetadata must be JSON-serializable: ${e.message}`);
                }
            }
            const appName = dbos_executor_1.DBOSExecutor.globalInstance.appName;
            (0, node_assert_1.default)(appName, 'Application name must be set in configuration in order to use DBOS Conductor');
            DBOS.conductor = new conductor_1.Conductor(dbos_executor_1.DBOSExecutor.globalInstance, appName, options.conductorKey, options.conductorURL, executorMetadata);
            DBOS.conductor.dispatchLoop();
        }
        // Start the DBOS admin server
        const logger = DBOS.logger;
        if (runtimeConfig.runAdminServer) {
            const adminApp = adminserver_1.DBOSAdminServer.setupAdminApp(executor);
            try {
                await adminserver_1.DBOSAdminServer.checkPortAvailabilityIPv4Ipv6(runtimeConfig.admin_port, logger);
                // Wrap the listen call in a promise to properly catch errors
                DBOS.adminServer = await new Promise((resolve, reject) => {
                    const server = adminApp.listen(runtimeConfig?.admin_port, () => {
                        DBOS.logger.debug(`DBOS Admin Server is running at http://localhost:${runtimeConfig?.admin_port}`);
                        resolve(server);
                    });
                    server.on('error', (err) => {
                        reject(err);
                    });
                });
            }
            catch (e) {
                logger.warn(`Unable to start DBOS admin server on port ${runtimeConfig.admin_port}`);
            }
        }
    }
    /**
     * Logs all workflows that can be invoked externally, rather than directly by the applicaton.
     * This includes:
     *   All DBOS event receiver entrypoints (message queues, URLs, etc.)
     *   Scheduled workflows
     *   Queues
     */
    static logRegisteredEndpoints() {
        if (!dbos_executor_1.DBOSExecutor.globalInstance)
            return;
        for (const lcl of (0, decorators_1.getLifecycleListeners)()) {
            lcl.logRegisteredEndpoints?.();
        }
    }
    /**
     * Shut down DBOS processing:
     *   Stops receiving external workflow requests
     *   Disconnects from administration / Conductor
     *   Stops workflow processing and disconnects from databases
     * @param options Optional shutdown options.
     * @param options.deregister
     *   If true, clear the DBOS workflow, queue, instance, data source, listener, and other registries.
     *   This is available for testing and development purposes only.
     *   Functions may then be registered before the next call to DBOS.launch().
     *   Decorated / registered functions created prior to `clearRegistry` may no longer be used.
     *     Fresh wrappers may be created from the original functions.
     */
    static async shutdown(options) {
        // Stop the admin server
        if (DBOS.adminServer) {
            DBOS.adminServer.close();
            DBOS.adminServer = undefined;
        }
        // Stop the conductor
        if (DBOS.conductor) {
            DBOS.conductor.stop();
            while (!DBOS.conductor.isClosed) {
                await (0, utils_1.sleepms)(500);
            }
            DBOS.conductor = undefined;
        }
        // Stop the executor
        if (dbos_executor_1.DBOSExecutor.globalInstance) {
            await dbos_executor_1.DBOSExecutor.globalInstance.deactivateEventReceivers();
            await dbos_executor_1.DBOSExecutor.globalInstance.destroy();
            dbos_executor_1.DBOSExecutor.globalInstance = undefined;
        }
        for (const [_n, ds] of decorators_1.transactionalDataSources) {
            await ds.destroy();
        }
        // Reset the global app version and executor ID
        utils_1.globalParams.appVersion = process.env.DBOS__APPVERSION || '';
        utils_1.globalParams.wasComputed = false;
        utils_1.globalParams.appID = process.env.DBOS__APPID || '';
        utils_1.globalParams.executorID = process.env.DBOS__VMID || 'local';
        (0, decorators_1.recordDBOSShutdown)();
        if (options?.deregister) {
            DBOS.clearRegistry();
        }
    }
    /**
     * Clear the DBOS workflow, queue, instance, data source, listener, and other registries.
     *   This is available for testing and development purposes only.
     *   This may only be done while DBOS is not launch()ed.
     *   Decorated / registered functions created prior to `clearRegistry` may no longer be used.
     *     Fresh wrappers may be created from the original functions.
     */
    static clearRegistry() {
        (0, node_assert_1.default)(!DBOS.isInitialized(), 'Cannot call DBOS.clearRegistry after DBOS.launch');
        (0, decorators_1.clearAllRegistrations)();
        wfqueue_1.wfQueueRunner.clearRegistrations();
        dbos_executor_1.DBOSExecutor.internalQueue = undefined;
    }
    /** Stop listening for external events (for testing) */
    static async deactivateEventReceivers() {
        return dbos_executor_1.DBOSExecutor.globalInstance?.deactivateEventReceivers();
    }
    /** Start listening for external events (for testing) */
    static async initEventReceivers() {
        return dbos_executor_1.DBOSExecutor.globalInstance?.initEventReceivers(this.#dbosConfig?.listenQueues || null);
    }
    // Global DBOS executor instance
    static get #executor() {
        return getExecutor();
    }
    //////
    // Globals
    //////
    static #dbosConfig;
    //////
    // Context
    //////
    /** Get the current DBOS Logger, appropriate to the current context */
    static get logger() {
        const lctx = (0, context_1.getCurrentContextStore)();
        if (lctx?.logger)
            return lctx.logger;
        const executor = dbos_executor_1.DBOSExecutor.globalInstance;
        if (executor)
            return executor.logger;
        return new logs_1.GlobalLogger(undefined, { logger: DBOS.#dbosConfig?.logger, logLevel: DBOS.#dbosConfig?.logLevel });
    }
    /** Get the current DBOS Tracer, for starting spans */
    static get tracer() {
        const executor = dbos_executor_1.DBOSExecutor.globalInstance;
        if (executor)
            return executor.tracer;
    }
    /** Get the current DBOS tracing span, appropriate to the current context */
    static get span() {
        return (0, traces_1.getActiveSpan)();
    }
    /**
     * Get the current request object (such as an HTTP request)
     * This is intended for use in event libraries that know the type of the current request,
     *  and set it using `withTracedContext` or `runWithContext`
     */
    static requestObject() {
        return (0, context_1.getCurrentContextStore)()?.request;
    }
    /** Get the current HTTP request (within `@DBOS.getApi` et al) */
    static getRequest() {
        return this.requestObject();
    }
    /** Get the current HTTP request (within `@DBOS.getApi` et al) */
    static get request() {
        const r = DBOS.getRequest();
        if (!r)
            throw new error_1.DBOSError('`DBOS.request` accessed from outside of HTTP requests');
        return r;
    }
    /** Get the current application version */
    static get applicationVersion() {
        return utils_1.globalParams.appVersion;
    }
    /** Get the current executor ID */
    static get executorID() {
        return utils_1.globalParams.executorID;
    }
    /** Get the current workflow ID */
    static get workflowID() {
        return (0, context_1.getCurrentContextStore)()?.workflowId;
    }
    /** Use portable serialization by default? */
    static get defaultSerializationType() {
        return (0, context_1.getCurrentContextStore)()?.serializationType;
    }
    /** Get the current step number, within the current workflow */
    static get stepID() {
        if (DBOS.isInStep()) {
            return (0, context_1.getCurrentContextStore)()?.curStepFunctionId;
        }
        else if (DBOS.isInTransaction()) {
            return (0, context_1.getCurrentContextStore)()?.curTxFunctionId;
        }
        else {
            return undefined;
        }
    }
    static get stepStatus() {
        return (0, context_1.getCurrentContextStore)()?.stepStatus;
    }
    /** Get the current authenticated user */
    static get authenticatedUser() {
        return (0, context_1.getCurrentContextStore)()?.authenticatedUser ?? '';
    }
    /** Get the roles granted to the current authenticated user */
    static get authenticatedRoles() {
        return (0, context_1.getCurrentContextStore)()?.authenticatedRoles ?? [];
    }
    /** Get the role assumed by the current user giving authorization to execute the current function */
    static get assumedRole() {
        return (0, context_1.getCurrentContextStore)()?.assumedRole ?? '';
    }
    /** @returns true if called from within a transaction, false otherwise */
    static isInTransaction() {
        return (0, context_1.getCurrentContextStore)()?.curTxFunctionId !== undefined;
    }
    /** @returns true if called from within a step, false otherwise */
    static isInStep() {
        return (0, context_1.getCurrentContextStore)()?.curStepFunctionId !== undefined;
    }
    /**
     * @returns true if called from within a workflow
     *  (regardless of whether the workflow is currently executing a step
     *   or transaction), false otherwise
     */
    static isWithinWorkflow() {
        return (0, context_1.getCurrentContextStore)()?.workflowId !== undefined;
    }
    /**
     * @returns true if called from within a workflow that is not currently executing
     *  a step or transaction, or false otherwise
     */
    static isInWorkflow() {
        return DBOS.isWithinWorkflow() && !DBOS.isInTransaction() && !DBOS.isInStep();
    }
    //////
    // Access to system DB, for event receivers etc.
    //////
    /**
     * Get a state item from the system database, which provides a key/value store interface for event dispatchers.
     *   The full key for the database state should include the service, function, and item.
     *   Values are versioned.  A version can either be a sequence number (long integer), or a time (high precision floating point).
     *       If versions are in use, any upsert is discarded if the version field is less than what is already stored.
     *
     * Examples of state that could be kept:
     *   Offsets into kafka topics, per topic partition
     *   Last time for which a scheduling service completed schedule dispatch
     *
     * @param service - should be unique to the event receiver keeping state, to separate from others
     * @param workflowFnName - function name; should be the fully qualified / unique function name dispatched
     * @param key - The subitem kept by event receiver service for the function, allowing multiple values to be stored per function
     * @returns The latest system database state for the specified service+workflow+item
     */
    static async getEventDispatchState(svc, wfn, key) {
        (0, decorators_1.ensureDBOSIsLaunched)('getEventDispatchState');
        return await DBOS.#executor.getEventDispatchState(svc, wfn, key);
    }
    /**
     * Set a state item into the system database, which provides a key/value store interface for event dispatchers.
     *   The full key for the database state should include the service, function, and item; these fields are part of `state`.
     *   Values are versioned.  A version can either be a sequence number (long integer), or a time (high precision floating point).
     *     If versions are in use, any upsert is discarded if the version field is less than what is already stored.
     *
     * @param state - the service, workflow, item, version, and value to write to the database
     * @returns The upsert returns the current record, which may be useful if it is more recent than the `state` provided.
     */
    static async upsertEventDispatchState(state) {
        (0, decorators_1.ensureDBOSIsLaunched)('upsertEventDispatchState');
        return await DBOS.#executor.upsertEventDispatchState(state);
    }
    //////
    // Workflow and other operations
    //////
    /**
     * Get the workflow status given a workflow ID
     * @param workflowID - ID of the workflow
     * @returns status of the workflow as `WorkflowStatus`, or `null` if there is no workflow with `workflowID`
     */
    static getWorkflowStatus(workflowID) {
        (0, decorators_1.ensureDBOSIsLaunched)('getWorkflowStatus');
        if (DBOS.isWithinWorkflow()) {
            if (DBOS.isInStep()) {
                // OK to use directly
                return DBOS.#executor.getWorkflowStatus(workflowID);
            }
            else if (DBOS.isInWorkflow()) {
                return DBOS.#executor.getWorkflowStatus(workflowID, DBOS.workflowID, (0, context_1.functionIDGetIncrement)());
            }
            else {
                throw new error_1.DBOSInvalidWorkflowTransitionError('Invalid call to `getWorkflowStatus` inside a `transaction`');
            }
        }
        return DBOS.#executor.getWorkflowStatus(workflowID);
    }
    /**
     * Get the workflow result, given a workflow ID
     * @param workflowID - ID of the workflow
     * @param timeoutSeconds - Maximum time to wait for result; if not provided, the operation does not time out
     * @returns The return value of the workflow, or throws the exception thrown by the workflow, or `null` if times out
     */
    static async getResult(workflowID, options) {
        (0, decorators_1.ensureDBOSIsLaunched)('getResult');
        const timeoutSeconds = resolveTimeoutSeconds(options);
        const pollingIntervalMs = resolvePollingIntervalMs(options);
        let timerFuncID = undefined;
        if (DBOS.isWithinWorkflow() && timeoutSeconds !== undefined) {
            // Reserve the function ID synchronously, before any await.
            timerFuncID = (0, context_1.functionIDGetIncrement)();
        }
        return await DBOS.getResultInternal(workflowID, timeoutSeconds, timerFuncID, undefined, pollingIntervalMs);
    }
    static async getResultInternal(workflowID, timeoutSeconds, timerFuncID, assignedFuncID, pollingIntervalMs) {
        return await runInternalStep(async () => {
            const rres = await dbos_executor_1.DBOSExecutor.globalInstance.systemDatabase.awaitWorkflowResult(workflowID, timeoutSeconds, DBOS.workflowID, timerFuncID, pollingIntervalMs);
            if (!rres)
                return null;
            if (rres?.cancelled) {
                throw new error_1.DBOSAwaitedWorkflowCancelledError(workflowID);
            }
            if (rres?.maxRecoveryAttemptsExceeded) {
                throw new error_1.DBOSAwaitedWorkflowExceededMaxRecoveryAttempts(workflowID);
            }
            return await dbos_executor_1.DBOSExecutor.reviveResultOrError(rres, DBOS.#executor.serializer);
        }, 'DBOS.getResult', workflowID, assignedFuncID);
    }
    /**
     * Wait for any one of the given workflow handles to complete and return it.
     * Polls the database until at least one workflow's status is no longer PENDING, ENQUEUED, or DELAYED,
     * then returns the corresponding handle.
     * @param handles - Non-empty array of workflow handles to wait on
     * @returns The first handle whose workflow has completed
     */
    static async waitFirst(handles, options) {
        (0, decorators_1.ensureDBOSIsLaunched)('waitFirst');
        const pollingIntervalMs = resolvePollingIntervalMs(options);
        if (handles.length === 0) {
            throw new Error('handles must not be empty');
        }
        // Build a map from workflow ID to handle, disallowing duplicates.
        const handleMap = new Map();
        for (const handle of handles) {
            if (handleMap.has(handle.workflowID)) {
                throw new Error(`Duplicate workflow ID in waitFirst: ${handle.workflowID}`);
            }
            handleMap.set(handle.workflowID, handle);
        }
        const workflowIds = [...handleMap.keys()];
        const completedId = await runInternalStep(async () => {
            return await dbos_executor_1.DBOSExecutor.globalInstance.systemDatabase.awaitFirstWorkflowId(workflowIds, DBOS.workflowID, pollingIntervalMs);
        }, 'DBOS.waitFirst');
        return handleMap.get(completedId);
    }
    /**
     * Wait for all of the given workflow handles to complete and return them.
     * Polls the database until each workflow's status is no longer PENDING, ENQUEUED, or DELAYED.
     * @param handles - Array of workflow handles to wait on
     * @param options - Optional polling interval
     * @returns The input handles, in the same order
     */
    static async waitAll(handles, options) {
        (0, decorators_1.ensureDBOSIsLaunched)('waitAll');
        if (handles.length === 0) {
            return [];
        }
        const pollingIntervalMs = resolvePollingIntervalMs(options);
        const workflowIds = [...new Set(handles.map((handle) => handle.workflowID))];
        await runInternalStep(async () => {
            await dbos_executor_1.DBOSExecutor.globalInstance.systemDatabase.awaitWorkflowIds(workflowIds, DBOS.workflowID, pollingIntervalMs);
        }, 'DBOS.waitAll');
        return handles;
    }
    /**
     * Create a workflow handle with a given workflow ID.
     * This call always returns a handle, even if the workflow does not exist.
     * The resulting handle will check the database to provide any workflow information.
     * @param workflowID - ID of the workflow
     * @returns `WorkflowHandle` that can be used to poll for the status or result of any workflow with `workflowID`
     */
    static retrieveWorkflow(workflowID) {
        (0, decorators_1.ensureDBOSIsLaunched)('retrieveWorkflow');
        if (DBOS.isWithinWorkflow()) {
            if (!DBOS.isInWorkflow()) {
                throw new error_1.DBOSInvalidWorkflowTransitionError('Invalid call to `retrieveWorkflow` inside a `transaction` or `step`');
            }
            return new workflow_1.RetrievedHandle(dbos_executor_1.DBOSExecutor.globalInstance.systemDatabase, workflowID);
        }
        return DBOS.#executor.retrieveWorkflow(workflowID);
    }
    /**
     * Query the system database for all workflows matching the provided predicate
     * @param input - `GetWorkflowsInput` predicate for filtering returned workflows
     * @returns `WorkflowStatus` array containing details of the matching workflows
     */
    static async listWorkflows(input) {
        (0, decorators_1.ensureDBOSIsLaunched)('listWorkflows');
        return await runInternalStep(async () => {
            return await DBOS.#executor.listWorkflows(input);
        }, 'DBOS.listWorkflows');
    }
    /**
     * Query the system database for all queued workflows matching the provided predicate
     * @param input - `GetQueuedWorkflowsInput` predicate for filtering returned workflows
     * @returns `WorkflowStatus` array containing details of the matching workflows
     */
    static async listQueuedWorkflows(input) {
        (0, decorators_1.ensureDBOSIsLaunched)('listQueuedWorkflows');
        return await runInternalStep(async () => {
            return await DBOS.#executor.listQueuedWorkflows(input);
        }, 'DBOS.listQueuedWorkflows');
    }
    /**
     * Set the priority of a queued workflow.
     * Only affects workflows with ENQUEUED or DELAYED status.
     * @param workflowID - ID of the workflow
     * @param priority - Priority value (1 to 2,147,483,647). Lower values are dequeued first.
     */
    static async setWorkflowPriority(workflowID, priority) {
        (0, decorators_1.ensureDBOSIsLaunched)('setWorkflowPriority');
        if (priority < dbos_executor_1.DBOS_QUEUE_MIN_PRIORITY || priority > dbos_executor_1.DBOS_QUEUE_MAX_PRIORITY) {
            throw new error_1.DBOSInvalidQueuePriorityError(priority, dbos_executor_1.DBOS_QUEUE_MIN_PRIORITY, dbos_executor_1.DBOS_QUEUE_MAX_PRIORITY);
        }
        return runInternalStep(async () => {
            return dbos_executor_1.DBOSExecutor.globalInstance.systemDatabase.setWorkflowPriority(workflowID, priority);
        }, 'DBOS.setWorkflowPriority');
    }
    /**
     * Update the delay on a DELAYED workflow.
     * The workflow will remain in DELAYED status until the delay expires, then transition to ENQUEUED.
     * Only affects workflows with DELAYED status.
     * @param workflowID - ID of the workflow
     * @param options - {@link SetWorkflowDelayOptions} controlling delay duration or absolute deadline, or a number of seconds
     */
    static async setWorkflowDelay(workflowID, options) {
        (0, decorators_1.ensureDBOSIsLaunched)('setWorkflowDelay');
        const delayUntilEpochMS = resolveDelayEpochMS(options);
        return runInternalStep(async () => {
            return dbos_executor_1.DBOSExecutor.globalInstance.systemDatabase.setWorkflowDelay(workflowID, delayUntilEpochMS);
        }, 'DBOS.setWorkflowDelay');
    }
    /**
     * Retrieve the steps of a workflow
     * @param workflowID - ID of the workflow
     * @param options - Optional pagination parameters (limit, offset)
     * @returns `StepInfo` array listing the executed steps of the workflow. If the workflow is not found, `undefined` is returned.
     */
    static async listWorkflowSteps(workflowID, options) {
        (0, decorators_1.ensureDBOSIsLaunched)('listWorkflowSteps');
        return await runInternalStep(async () => {
            return await DBOS.#executor.listWorkflowSteps(workflowID, true, options);
        }, 'DBOS.listWorkflowSteps');
    }
    /**
     * Cancel a workflow given its ID.
     * If the workflow is currently running, `DBOSWorkflowCancelledError` will be
     *   thrown from its next DBOS call.
     * @param workflowID - ID of the workflow
     * @param options - If `cancelChildren` is true, also cancel all descendant workflows recursively
     */
    static async cancelWorkflow(workflowID, options) {
        return this.cancelWorkflows([workflowID], options);
    }
    /**
     * Cancel multiple workflows given their IDs.
     * @param workflowIDs - IDs of the workflows to cancel
     * @param options - If `cancelChildren` is true, also cancel all descendant workflows recursively
     */
    static async cancelWorkflows(workflowIDs, options) {
        (0, decorators_1.ensureDBOSIsLaunched)('cancelWorkflows');
        return runInternalStep(async () => {
            return dbos_executor_1.DBOSExecutor.globalInstance.systemDatabase.cancelWorkflows(workflowIDs, options?.cancelChildren);
        }, 'DBOS.cancelWorkflow');
    }
    /**
     * Resume a workflow given its ID.
     * @param workflowID - ID of the workflow
     */
    static async resumeWorkflow(workflowID, options) {
        const handles = await this.resumeWorkflows([workflowID], options);
        return handles[0];
    }
    /**
     * Resume multiple workflows given their IDs.
     * @param workflowIDs - IDs of the workflows to resume
     * @returns An array of workflow handles
     */
    static async resumeWorkflows(workflowIDs, options) {
        (0, decorators_1.ensureDBOSIsLaunched)('resumeWorkflows');
        await runInternalStep(async () => {
            return dbos_executor_1.DBOSExecutor.globalInstance.systemDatabase.resumeWorkflows(workflowIDs, options?.queueName);
        }, 'DBOS.resumeWorkflow');
        return workflowIDs.map((wfid) => this.retrieveWorkflow(wfid));
    }
    /**
     * Delete a workflow and optionally all its child workflows.
     * This permanently removes the workflow from the system database.
     *
     * WARNING: This operation is irreversible.
     *
     * @param workflowID - ID of the workflow to delete
     * @param deleteChildren - If true, also delete all child workflows recursively (default: false)
     */
    static async deleteWorkflow(workflowID, deleteChildren = false) {
        return this.deleteWorkflows([workflowID], deleteChildren);
    }
    /**
     * Delete multiple workflows and optionally all their child workflows.
     *
     * WARNING: This operation is irreversible.
     *
     * @param workflowIDs - IDs of the workflows to delete
     * @param deleteChildren - If true, also delete all child workflows recursively (default: false)
     */
    static async deleteWorkflows(workflowIDs, deleteChildren = false) {
        (0, decorators_1.ensureDBOSIsLaunched)('deleteWorkflows');
        return runInternalStep(async () => {
            return dbos_executor_1.DBOSExecutor.globalInstance.systemDatabase.deleteWorkflows(workflowIDs, deleteChildren);
        }, 'DBOS.deleteWorkflow');
    }
    /**
     * Fork a workflow given its ID.
     * @param workflowID - ID of the workflow
     * @param startStep - Step ID to start the forked workflow from
     * @param applicationVersion - Version of the application to use for the forked workflow
     * @returns A handle to the forked workflow
     * @throws DBOSInvalidStepIDError if the `startStep` is greater than the maximum step ID of the workflow
     */
    static async forkWorkflow(workflowID, startStep, options) {
        (0, decorators_1.ensureDBOSIsLaunched)('forkWorkflow');
        const forkedID = await runInternalStep(async () => {
            return await DBOS.#executor.forkWorkflow(workflowID, startStep, options);
        }, 'DBOS.forkWorkflow');
        return this.retrieveWorkflow(forkedID);
    }
    /**
     * Sleep for the specified amount of time.
     * If called from within a workflow, the sleep is "durable",
     *   meaning that the workflow will sleep until the wakeup time
     *   (calculated by adding `durationMS` to the original invocation time),
     *   regardless of workflow recovery.
     * @param durationMS - Length of sleep, in milliseconds.
     */
    static async sleepms(durationMS) {
        if (DBOS.isWithinWorkflow() && !DBOS.isInStep()) {
            if (DBOS.isInTransaction()) {
                throw new error_1.DBOSInvalidWorkflowTransitionError('Invalid call to `DBOS.sleep` inside a `transaction`');
            }
            // Reserve the function ID synchronously, before any await.
            const functionID = (0, context_1.functionIDGetIncrement)();
            if (durationMS <= 0) {
                return;
            }
            return await dbos_executor_1.DBOSExecutor.globalInstance.systemDatabase.durableSleepms(DBOS.workflowID, functionID, durationMS);
        }
        const endTime = Date.now() + durationMS;
        while (Date.now() < endTime) {
            await (0, utils_1.sleepms)(Math.min(endTime - Date.now(), utils_1.sleepConfig.maxTimeoutMS));
        }
    }
    /** @see sleepms */
    static async sleepSeconds(durationSec) {
        return DBOS.sleepms(durationSec * 1000);
    }
    /** @see sleepms */
    static async sleep(durationMS) {
        return DBOS.sleepms(durationMS);
    }
    /**
     * Get the current time in milliseconds, similar to `Date.now()`.
     * This function is deterministic and can be used within workflows.
     */
    static async now() {
        if (DBOS.isInWorkflow()) {
            return runInternalStep(async () => Promise.resolve(Date.now()), 'DBOS.now');
        }
        return Date.now();
    }
    /**
     * Generate a random (v4) UUUID, similar to `node:crypto.randomUUID`.
     * This function is deterministic and can be used within workflows.
     */
    static async randomUUID() {
        if (DBOS.isInWorkflow()) {
            return runInternalStep(async () => Promise.resolve((0, node_crypto_1.randomUUID)()), 'DBOS.randomUUID');
        }
        return (0, node_crypto_1.randomUUID)();
    }
    /**
     * Use the provided `workflowID` as the identifier for first workflow started
     *   within the `callback` function.
     * @param workflowID - ID to assign to the first workflow started
     * @param callback - Function to run, which would start a workflow
     * @returns - Return value from `callback`
     */
    static async withNextWorkflowID(workflowID, callback) {
        (0, decorators_1.ensureDBOSIsLaunched)('workflows');
        return DBOS.#withTopContext({ idAssignedForNextWorkflow: workflowID }, callback);
    }
    /**
     * Use the provided `authedUser` and `authedRoles` as the authenticated user for
     *   any security checks or calls to `DBOS.authenticatedUser`
     *   or `DBOS.authenticatedRoles` placed within the `callback` function.
     * @param authedUser - Authenticated user
     * @param authedRoles - Authenticated roles
     * @param callback - Function to run with authentication context in place
     * @returns - Return value from `callback`
     */
    static async withAuthedContext(authedUser, authedRoles, callback) {
        (0, decorators_1.ensureDBOSIsLaunched)('auth');
        return DBOS.#withTopContext({
            authenticatedUser: authedUser,
            authenticatedRoles: authedRoles,
        }, callback);
    }
    /**
     * This generic setter helps users calling DBOS operation to pass a name,
     *   later used in seeding a parent OTel span for the operation.
     * @param callerName - Tracing caller name
     * @param callback - Function to run with tracing context in place
     * @returns - Return value from `callback`
     */
    static async withNamedContext(callerName, callback) {
        (0, decorators_1.ensureDBOSIsLaunched)('tracing');
        return DBOS.#withTopContext({ operationCaller: callerName }, callback);
    }
    /**
     * Use queue named `queueName` for any workflows started within the `callback`.
     * @param queueName - Name of queue upon which all workflows called or started within `callback` will be run
     * @param callback - Function to run, which would call or start workflows
     * @returns - Return value from `callback`
     */
    static async withWorkflowQueue(queueName, callback) {
        (0, decorators_1.ensureDBOSIsLaunched)('workflows');
        return DBOS.#withTopContext({ queueAssignedForWorkflows: queueName }, callback);
    }
    /**
     * Specify workflow timeout for any workflows started within the `callback`.
     * @param timeoutMS - timeout length for all workflows started within `callback` will be run
     * @param callback - Function to run, which would call or start workflows
     * @returns - Return value from `callback`
     */
    static async withWorkflowTimeout(timeoutMS, callback) {
        (0, decorators_1.ensureDBOSIsLaunched)('workflows');
        return DBOS.#withTopContext({ workflowTimeoutMS: timeoutMS }, callback);
    }
    /**
     * Run a workflow with the option to set any of the contextual items
     *
     * @param options - Overrides for options
     * @param callback - Function to run, which would call or start workflows
     * @returns - Return value from `callback`
     */
    static async runWithContext(options, callback) {
        (0, decorators_1.ensureDBOSIsLaunched)('contexts');
        return DBOS.#withTopContext(options, callback);
    }
    static async #withTopContext(options, callback) {
        const pctx = (0, context_1.getCurrentContextStore)();
        if (pctx) {
            // Save existing values and overwrite with new; hard to do cleanly but is actually type correct
            // eslint-disable-next-line @typescript-eslint/no-explicit-any
            const existing = {};
            for (const k of Object.keys(options)) {
                if (Object.hasOwn(pctx, k))
                    // Save the current value (not the incoming one) so the finally block can restore it,
                    // letting nested `with*` blocks correctly roll back to the outer value.
                    // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access
                    existing[k] = pctx[k];
                // eslint-disable-next-line @typescript-eslint/no-explicit-any, @typescript-eslint/no-unsafe-member-access
                pctx[k] = options[k];
            }
            try {
                return await callback();
            }
            finally {
                for (const k of Object.keys(options)) {
                    // eslint-disable-next-line @typescript-eslint/no-unsafe-argument
                    if (Object.hasOwn(existing, k))
                        // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-explicit-any, @typescript-eslint/no-unsafe-assignment
                        pctx[k] = existing[k];
                    else
                        delete pctx[k];
                }
            }
        }
        else {
            return await (0, context_1.runWithTopContext)(options, callback);
        }
    }
    static startWorkflow(target, params) {
        (0, decorators_1.ensureDBOSIsLaunched)('workflows');
        const instance = typeof target === 'function' ? null : target;
        if (instance && typeof instance !== 'function' && !(instance instanceof decorators_1.ConfiguredInstance)) {
            throw new error_1.DBOSInvalidWorkflowTransitionError('Attempt to call `startWorkflow` on an object that is not a `ConfiguredInstance`');
        }
        const regOps = (0, decorators_1.getRegisteredOperations)(target);
        const singletonWorkflow = params?.duplicationPolicy === 'return-existing';
        const handler = {
            apply(target, _thisArg, args) {
                const regOp = (0, decorators_1.getFunctionRegistration)(target);
                if (!regOp) {
                    // eslint-disable-next-line @typescript-eslint/no-base-to-string
                    const name = typeof target === 'function' ? target.name : target.toString();
                    throw new error_1.DBOSNotRegisteredError(name, `${name} is not a registered DBOS workflow function`);
                }
                return singletonWorkflow
                    ? DBOS.#invokeSingletonWorkflow(instance, regOp, args, params)
                    : DBOS.#invokeWorkflow(instance, regOp, args, params);
            },
            get(target, p, receiver) {
                // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment
                const func = Reflect.get(target, p, receiver);
                const regOp = (0, decorators_1.getFunctionRegistration)(func) ?? regOps.find((op) => op.name === p);
                if (regOp) {
                    return (...args) => singletonWorkflow
                        ? DBOS.#invokeSingletonWorkflow(instance, regOp, args, params)
                        : DBOS.#invokeWorkflow(instance, regOp, args, params);
                }
                const name = typeof p === 'string' ? p : String(p);
                throw new error_1.DBOSNotRegisteredError(name, `${name} is not a registered DBOS workflow function`);
            },
        };
        return new Proxy(target, handler);
    }
    /**
     * Send `message` on optional `topic` to the workflow with `destinationID`
     *  This can be done from inside or outside of DBOS workflow functions
     *  Use the optional `idempotencyKey` to guarantee that the message is sent exactly once
     * @see `DBOS.recv`
     *
     * @param destinationID - ID of the workflow that will `recv` the message
     * @param message - Message to send, which must be serializable as JSON
     * @param topic - Optional topic; if specified the `recv` command can specify the same topic to receive selectively
     * @param idempotencyKey - Optional key for sending the message exactly once
     * @param options - `SendOptions`
     */
    static async send(destinationID, message, topic, idempotencyKey, options) {
        (0, decorators_1.ensureDBOSIsLaunched)('send');
        if (DBOS.isInWorkflow()) {
            // Advance the function ID synchronously before any await, so concurrent step
            // calls in the same workflow don't race for IDs.
            const functionID = (0, context_1.functionIDGetIncrement)();
            const sermsg = await (0, serialization_1.serializeValue)(message, DBOS.#executor.serializer, options?.serializationType ?? DBOS.defaultSerializationType);
            return await dbos_executor_1.DBOSExecutor.globalInstance.systemDatabase.send(DBOS.workflowID, functionID, destinationID, sermsg.serializedValue, topic, sermsg.serialization, idempotencyKey);
        }
        else {
            const sermsg = await (0, serialization_1.serializeValue)(message, DBOS.#executor.serializer, options?.serializationType ?? DBOS.defaultSerializationType);
            return dbos_executor_1.DBOSExecutor.globalInstance.systemDatabase.sendDirect(destinationID, sermsg.serializedValue, topic, sermsg.serialization, idempotencyKey);
        }
    }
    /**
     * Receive a message on optional `topic` from within a workflow.
     *  This must be called from within a workflow; this workflow's ID is used to check for messages sent by `DBOS.send`
     *  This can be configured to time out.
     *  Messages are received in the order in which they are sent (per-sender / causal order).
     * @see `DBOS.send`
     *
     * @param topic - Optional topic; if specified the `recv` command can specify the same topic to receive selectively
     * @param options - {@link RecvOptions} controlling timeout or deadline; if neither is set, times out after 60 seconds
     * @template T - The type of message that is expected to be received
     * @returns Any message received, or `null` if the timeout expires
     */
    static async recv(topic, options) {
        (0, decorators_1.ensureDBOSIsLaunched)('recv');
        if (DBOS.isWithinWorkflow()) {
            if (!DBOS.isInWorkflow()) {
                throw new error_1.DBOSInvalidWorkflowTransitionError('Invalid call to `DBOS.recv` inside a `step` or `transaction`');
            }
            // Reserve the function IDs synchronously, before any await.
            const functionID = (0, context_1.functionIDGetIncrement)();
            const timeoutFunctionID = (0, context_1.functionIDGetIncrement)();
            const timeoutSeconds = resolveTimeoutSeconds(options);
            const pollingIntervalMs = resolvePollingIntervalMs(options);
            const msg = await dbos_executor_1.DBOSExecutor.globalInstance.systemDatabase.recv(DBOS.workflowID, functionID, timeoutFunctionID, topic, timeoutSeconds, pollingIntervalMs);
            return (await (0, serialization_1.deserializeValue)(msg.serializedValue, msg.serialization, DBOS.#executor.serializer));
        }
        throw new error_1.DBOSInvalidWorkflowTransitionError('Attempt to call `DBOS.recv` outside of a workflow'); // Only workflows can recv
    }
    /**
     * Set an event, from within a DBOS workflow.  This value can be retrieved with `DBOS.getEvent`.
     * If the event `key` already exists, its `value` is updated.
     * This function can only be called from within a workflow.
     * @see `DBOS.getEvent`
     *
     * @param key - The key for the event; at most one value is associated with a key at any given time.
     * @param value - The value to associate with `key`
     * @param options - `SetEventOptions` allowing control of the recorded event
     */
    static async setEvent(key, value, options) {
        (0, decorators_1.ensureDBOSIsLaunched)('setEvent');
        if (DBOS.isWithinWorkflow()) {
            if (!DBOS.isInWorkflow()) {
                throw new error_1.DBOSInvalidWorkflowTransitionError('Invalid call to `DBOS.setEvent` inside a `step` or `transaction`');
            }
            // Reserve the function ID synchronously, before any await.
            const functionID = (0, context_1.functionIDGetIncrement)();
            const serevt = await (0, serialization_1.serializeValue)(value, DBOS.#executor.serializer, options?.serializationType ?? DBOS.defaultSerializationType);
            return dbos_executor_1.DBOSExecutor.globalInstance.systemDatabase.setEvent(DBOS.workflowID, functionID, key, serevt.serializedValue, serevt.serialization);
        }
        throw new error_1.DBOSInvalidWorkflowTransitionError('Attempt to call `DBOS.setEvent` outside of a workflow'); // Only workflows can set event
    }
    /**
     * Get the value of a workflow event, or wait for it to be set.
     * This function can be called inside or outside of DBOS workflow functions.
     * If this function is called from within a workflow, its result is durably checkpointed.
     * @see `DBOS.setEvent`
     *
     * @param workflowID - The ID of the workflow with the corresponding `setEvent`
     * @param key - The key for the event; at most one value is associated with a key at any given time.
     * @param options - {@link GetEventOptions} controlling timeout or deadline; if neither is set, times out after 60 seconds
     * @template T - The expected type for the value assigned to `key`
     * @returns The value to associate with `key`, or `null` if the timeout is hit
     */
    static async getEvent(workflowID, key, options) {
        (0, decorators_1.ensureDBOSIsLaunched)('getEvent');
        const timeoutSeconds = resolveTimeoutSeconds(options);
        const pollingIntervalMs = resolvePollingIntervalMs(options);
        if (DBOS.isWithinWorkflow()) {
            if (!DBOS.isInWorkflow()) {
                throw new error_1.DBOSInvalidWorkflowTransitionError('Invalid call to `DBOS.getEvent` inside a `step` or `transaction`');
            }
            // Reserve the function IDs synchronously, before any await.
            const functionID = (0, context_1.functionIDGetIncrement)();
            const timeoutFunctionID = (0, context_1.functionIDGetIncrement)();
            const params = {
                workflowID: DBOS.workflowID,
                functionID,
                timeoutFunctionID,
            };
            const evt = await dbos_executor_1.DBOSExecutor.globalInstance.systemDatabase.getEvent(workflowID, key, timeoutSeconds ?? dbos_executor_1.DBOSExecutor.defaultNotificationTimeoutSec, params, pollingIntervalMs);
            return (await (0, serialization_1.deserializeValue)(evt.serializedValue, evt.serialization, DBOS.#executor.serializer));
        }
        return DBOS.#executor.getEvent(workflowID, key, timeoutSeconds, pollingIntervalMs);
    }
    /**
     * Write a value to a stream.
     * @param key - The stream key/name within the workflow
     * @param value - A serializable value to write to the stream
     * @param options - `WriteStreamOptions` for controlling serialization
     */
    static async writeStream(key, value, options = {}) {
        (0, decorators_1.ensureDBOSIsLaunched)('writeStream');
        if (DBOS.isInWorkflow()) {
            // Advance the function ID synchronously before any await.
            const functionID = (0, context_1.functionIDGetIncrement)();
            const serval = await (0, serialization_1.serializeValue)(value, DBOS.#executor.serializer, options.serializationType ?? DBOS.defaultSerializationType);
            return await dbos_executor_1.DBOSExecutor.globalInstance.systemDatabase.writeStreamFromWorkflow(DBOS.workflowID, functionID, key, serval.serializedValue, serval.serialization, system_database_1.DBOS_FUNCNAME_WRITESTREAM);
        }
        else if (DBOS.isInStep()) {
            const serval = await (0, serialization_1.serializeValue)(value, DBOS.#executor.serializer, options.serializationType ?? DBOS.defaultSerializationType);
            return await dbos_executor_1.DBOSExecutor.globalInstance.systemDatabase.writeStreamFromStep(DBOS.workflowID, DBOS.stepID, key, serval.serializedValue, serval.serialization);
        }
        else {
            throw new error_1.DBOSInvalidWorkflowTransitionError('Invalid call to `DBOS.writeStream` outside of a workflow or step');
        }
    }
    /**
     * Close a stream by writing a sentinel value.
     * @param key - The stream key/name within the workflow
     */
    static async closeStream(key) {
        (0, decorators_1.ensureDBOSIsLaunched)('closeStream');
        if (DBOS.isWithinWorkflow()) {
            if (DBOS.isInWorkflow()) {
                // Reserve the function ID synchronously, before any await.
                const functionID = (0, context_1.functionIDGetIncrement)();
                return await dbos_executor_1.DBOSExecutor.globalInstance.systemDatabase.closeStream(DBOS.workflowID, functionID, key);
            }
            else {
                throw new error_1.DBOSInvalidWorkflowTransitionError('Invalid call to `DBOS.closeStream` outside of a workflow or step');
            }
        }
        else {
            throw new error_1.DBOSInvalidWorkflowTransitionError('Invalid call to `DBOS.closeStream` outside of a workflow');
        }
    }
    /**
     * Read values from a stream as an async generator.
     * This function reads values from a stream identified by the workflowID and key,
     * yielding each value in order until the stream is closed or the workflow terminates.
     * @param workflowID - The workflow instance ID that owns the stream
     * @param key - The stream key/name within the workflow
     * @returns An async generator that yields each value in the stream until the stream is closed
     */
    static async *readStream(workflowID, key) {
        (0, decorators_1.ensureDBOSIsLaunched)('readStream');
        const sysdb = dbos_executor_1.DBOSExecutor.globalInstance.systemDatabase;
        const payload = `${workflowID}::${key}`;
        let offset = 0;
        let finalRead = false;
        while (true) {
            // Register a listener before reading so a notification arriving between the
            // read and the wait below is not lost; a fresh promise per iteration gives
            // the "clear before reading" semantics.
            let resolveNotification;
            const messagePromise = new Promise((resolve) => {
                resolveNotification = resolve;
            });
            const cbr = sysdb.streamsMap.registerCallback(payload, resolveNotification);
            try {
                // One round trip for both the value and the workflow's status.
                const { status, value } = await sysdb.readStreamValue(workflowID, key, offset);
                if (status === null) {
                    throw new error_1.DBOSNonExistentWorkflowError(`Workflow ${workflowID} does not exist`);
                }
                if (value !== undefined) {
                    if (value.serializedValue === system_database_1.DBOS_STREAM_CLOSED_SENTINEL) {
                        return;
                    }
                    yield (await (0, serialization_1.deserializeValue)(value.serializedValue, value.serialization, sysdb.getSerializer()));
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
                sysdb.streamsMap.deregisterCallback(cbr);
            }
        }
    }
    /**
     * registers a workflow method or function with an invocation schedule
     * @param func - The workflow method or function to register with an invocation schedule
     * @param options - Configuration information for the scheduled workflow
     */
    static registerScheduled(func, config) {
        scheduler_decorator_1.ScheduledReceiver.registerScheduled(func, config);
    }
    //////
    // Decorators
    //////
    /**
     * Allow a class to be assigned a name
     */
    static className(name) {
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        function clsdec(ctor) {
            const clsreg = (0, decorators_1.getClassRegistration)(ctor, true);
            if (clsreg.reg?.name && clsreg.reg.name !== name && clsreg.reg.name !== ctor.name) {
                throw new error_1.DBOSConflictingRegistrationError(`Attempt to assign name ${name} to class ${ctor.name}, which has already been aliased to ${clsreg.reg.name}`);
            }
            clsreg.reg.name = name;
        }
        return clsdec;
    }
    /**
     * Decorator associating a class static method with an invocation schedule
     * @param config - The schedule, consisting of a crontab and policy for "make-up work"
     */
    static scheduled(config) {
        function methodDecorator(target, propertyKey, descriptor) {
            if (descriptor.value) {
                DBOS.registerScheduled(descriptor.value, {
                    ...config,
                    ctorOrProto: target,
                    name: String(propertyKey),
                });
            }
            return descriptor;
        }
        return methodDecorator;
    }
    /**
     * Decorator designating a method as a DBOS workflow
     *   Durable execution will be applied within calls to the workflow function
     *   This also registers the function so that it is available during recovery
     * @param config - Configuration information for the workflow
     */
    static workflow(config = {}) {
        function decorator(target, propertyKey, inDescriptor) {
            const { descriptor, registration } = (0, decorators_1.wrapDBOSFunctionAndRegisterByTarget)(target, propertyKey, config.name ?? propertyKey, inDescriptor);
            const invoker = DBOS.#getWorkflowInvoker(registration, config);
            descriptor.value = invoker;
            registration.wrappedFunction = invoker;
            (0, decorators_1.registerFunctionWrapper)(invoker, registration);
            return descriptor;
        }
        return decorator;
    }
    /**
     * Create a DBOS workflow function from a provided function.
     *   Similar to the DBOS.workflow, but without requiring a decorator
     *   Durable execution will be applied to calls to the function returned by registerWorkflow
     *   This also registers the function so that it is available during recovery
     * @param func - The function to register as a workflow
     * @param name - The name of the registered workflow
     * @param options - Configuration information for the registered workflow
     */
    static registerWorkflow(func, config) {
        const registration = (0, decorators_1.wrapDBOSFunctionAndRegisterByUniqueName)(config?.ctorOrProto, config?.className, config?.name ?? func.name, config?.name ?? func.name, func);
        return DBOS.#getWorkflowInvoker(registration, config);
    }
    static async #invokeWorkflow($this, regOP, args, params = {}, startWfFuncId) {
        (0, decorators_1.ensureDBOSIsLaunched)('workflows');
        (0, workflow_1.validateWorkflowAttributes)(params.workflowAttributes);
        const wfId = (0, context_1.getNextWFID)(params.workflowID);
        const ppctx = (0, context_1.getCurrentContextStore)();
        const queueName = params.queueName ?? ppctx?.queueAssignedForWorkflows;
        const timeoutMS = params.timeoutMS ?? ppctx?.workflowTimeoutMS;
        const instance = $this === undefined || typeof $this === 'function' ? undefined : $this;
        if (instance && !(instance instanceof decorators_1.ConfiguredInstance)) {
            throw new error_1.DBOSInvalidWorkflowTransitionError('Attempt to call a `workflow` function on an object that is not a `ConfiguredInstance`');
        }
        // If this is called from within a workflow, this is a child workflow.
        // Reserve the child's function ID synchronously, before any await — otherwise
        // concurrent step calls in the parent could race for IDs.
        const isChild = DBOS.isWithinWorkflow();
        if (isChild && !DBOS.isInWorkflow()) {
            throw new error_1.DBOSInvalidWorkflowTransitionError('Invalid call to a `workflow` function from within a `step` or `transaction`');
        }
        const funcId = isChild ? (startWfFuncId ?? (0, context_1.functionIDGetIncrement)()) : undefined;
        // All enqueue-option validation lives here. Param-only checks
        // (priority range, dedup-with-partition) always run; queue-config
        // checks (partition flag, priority-enabled flag) run only for queues
        // in this executor's in-memory map. Database-backed queues skip the
        // queue-config checks to avoid an extra roundtrip on every enqueue.
        if (queueName) {
            const queuePartitionKey = params.enqueueOptions?.queuePartitionKey;
            const priority = params.enqueueOptions?.priority;
            if (priority !== undefined && (priority < dbos_executor_1.DBOS_QUEUE_MIN_PRIORITY || priority > dbos_executor_1.DBOS_QUEUE_MAX_PRIORITY)) {
                throw new error_1.DBOSInvalidQueuePriorityError(priority, dbos_executor_1.DBOS_QUEUE_MIN_PRIORITY, dbos_executor_1.DBOS_QUEUE_MAX_PRIORITY);
            }
            if (queuePartitionKey && params.enqueueOptions?.deduplicationID) {
                throw Error('Deduplication is not supported for partitioned queues');
            }
            const inMem = this.#executor.getQueueByName(queueName);
            if (inMem) {
                if (inMem.partitionQueue && !queuePartitionKey) {
                    throw Error(`A workflow cannot be enqueued on partitioned queue ${queueName} without a partition key`);
                }
                if (queuePartitionKey && !inMem.partitionQueue) {
                    throw Error(`You can only use a partition key on a partition-enabled queue. Key ${queuePartitionKey} was used with non-partitioned queue ${queueName}`);
                }
                if (priority !== undefined && !inMem.priorityEnabled) {
                    throw Error(`Priority is not enabled for queue ${queueName}. Setting priority will not have any effect.`);
                }
            }
        }
        if (isChild) {
            const pwfid = ppctx.workflowId;
            const wfParams = {
                workflowUUID: wfId || pwfid + '-' + funcId,
                configuredInstance: instance,
                queueName,
                timeoutMS,
                // Detach child deadline if a null timeout is configured
                deadlineEpochMS: params.timeoutMS === null || ppctx?.workflowTimeoutMS === null ? undefined : ppctx?.deadlineEpochMS,
                enqueueOptions: params.enqueueOptions,
                duplicationPolicy: params.duplicationPolicy,
                workflowAttributes: params.workflowAttributes,
            };
            return await invokeRegOp(wfParams, pwfid, funcId);
        }
        else {
            const wfParams = {
                workflowUUID: wfId,
                queueName,
                enqueueOptions: params.enqueueOptions,
                configuredInstance: instance,
                timeoutMS,
                duplicationPolicy: params.duplicationPolicy,
                workflowAttributes: params.workflowAttributes,
            };
            return await invokeRegOp(wfParams, undefined, undefined);
        }
        function invokeRegOp(wfParams, workflowID, funcNum) {
            if (regOP.workflowConfig) {
                const func = regOP.registeredFunction;
                return dbos_executor_1.DBOSExecutor.globalInstance.internalWorkflow(func, wfParams, workflowID, funcNum, ...args);
            }
            if (regOP.stepConfig) {
                const func = regOP.registeredFunction;
                return dbos_executor_1.DBOSExecutor.globalInstance.startStepTempWF(func, wfParams, workflowID, funcNum, ...args);
            }
            throw new error_1.DBOSNotRegisteredError(regOP.name, `${regOP.name} is not a registered DBOS workflow, step, or transaction function`);
        }
    }
    static async #invokeSingletonWorkflow($this, regOP, args, params) {
        if (!params.queueName) {
            throw new error_1.DBOSInvalidWorkflowTransitionError("`duplicationPolicy: 'return-existing'` requires a `queueName`");
        }
        const dedupID = params.enqueueOptions?.deduplicationID;
        if (!dedupID) {
            throw new error_1.DBOSInvalidWorkflowTransitionError("`duplicationPolicy: 'return-existing'` requires `enqueueOptions.deduplicationID`");
        }
        const queueName = params.queueName;
        // Reserve the parent's child-funcID once, before any await. Each loop iteration passes it
        // through so the return-existing retry loop consumes a single funcID instead of one per try.
        const childFuncId = DBOS.isInWorkflow() ? (0, context_1.functionIDGetIncrement)() : undefined;
        // Resolve the workflow ID once: if the caller supplied one explicitly or via
        // `withNextWorkflowID`, reuse it on every iteration.
        const resolvedWorkflowID = (0, context_1.getNextWFID)(params.workflowID);
        const callParams = resolvedWorkflowID !== undefined ? { ...params, workflowID: resolvedWorkflowID } : params;
        while (true) {
            try {
                return await DBOS.#invokeWorkflow($this, regOP, args, callParams, childFuncId);
            }
            catch (e) {
                if (!(e instanceof error_1.DBOSQueueDuplicatedError)) {
                    throw e;
                }
                const existingID = await dbos_executor_1.DBOSExecutor.globalInstance.systemDatabase.getDeduplicatedWorkflow(queueName, dedupID);
                if (existingID) {
                    // Record the parent->child mapping at the reserved funcID so that on replay
                    // the parent uses the same child.
                    if (childFuncId !== undefined) {
                        const now = Date.now();
                        await dbos_executor_1.DBOSExecutor.globalInstance.systemDatabase.recordOperationResult(DBOS.workflowID, childFuncId, regOP.name, true, now, now, { childWorkflowID: existingID });
                    }
                    return new workflow_1.RetrievedHandle(dbos_executor_1.DBOSExecutor.globalInstance.systemDatabase, existingID);
                }
                // The prior workflow's deduplication_id was cleared between our INSERT and
                // the lookup (it completed or was cancelled). Loop and try to claim the slot.
            }
        }
    }
    static #getWorkflowInvoker(registration, config) {
        registration.setWorkflowConfig(config ?? {});
        if (config?.inputSchema) {
            const schema = config.inputSchema;
            registration.addEntryInterceptor((_reg, args) => {
                return schema.parse(args);
            });
        }
        const invoker = async function (...rawArgs) {
            (0, decorators_1.ensureDBOSIsLaunched)('workflows');
            if (DBOS.isInWorkflow()) {
                // Reserve the function IDs synchronously, before any await.
                const startWfFuncId = (0, context_1.functionIDGetIncrement)();
                const getResFuncID = (0, context_1.functionIDGetIncrement)();
                const handle = await DBOS.#invokeWorkflow(this, registration, rawArgs, undefined, startWfFuncId);
                return await handle.getResult(getResFuncID);
            }
            const handle = await DBOS.#invokeWorkflow(this, registration, rawArgs);
            return await handle.getResult();
        };
        (0, decorators_1.registerFunctionWrapper)(invoker, registration);
        Object.defineProperty(invoker, 'name', {
            value: registration.name,
        });
        return invoker;
    }
    /**
     * Decorator designating a method as a DBOS step.
     *   A durable checkpoint will be made after the step completes
     *   This ensures "at least once" execution of the step, and that the step will not
     *    be executed again once the checkpoint is recorded
     *
     * @param config - Configuration information for the step, particularly the retry policy
     */
    static step(config = {}) {
        function decorator(target, propertyKey, inDescriptor) {
            const { descriptor, registration } = (0, decorators_1.wrapDBOSFunctionAndRegisterByTarget)(target, propertyKey, config.name, inDescriptor);
            registration.setStepConfig(config);
            const invokeWrapper = async function (...rawArgs) {
                (0, decorators_1.ensureDBOSIsLaunched)('steps');
                let inst = undefined;
                if (this === undefined || typeof this === 'function') {
                    // This is static
                }
                else {
                    inst = this;
                    if (!(inst instanceof decorators_1.ConfiguredInstance)) {
                        throw new error_1.DBOSInvalidWorkflowTransitionError('Attempt to call a `step` function on an object that is not a `ConfiguredInstance`');
                    }
                }
                if (DBOS.isWithinWorkflow()) {
                    if (DBOS.isInTransaction()) {
                        throw new error_1.DBOSInvalidWorkflowTransitionError('Invalid call to a `step` function from within a `transaction`');
                    }
                    if (DBOS.isInStep()) {
                        // There should probably be checks here about the compatibility of the StepConfig...
                        return registration.registeredFunction.call(this, ...rawArgs);
                    }
                    return await dbos_executor_1.DBOSExecutor.globalInstance.callStepFunction(registration.registeredFunction, undefined, undefined, inst ?? null, ...rawArgs);
                }
                const wfId = (0, context_1.getNextWFID)(undefined);
                const wfParams = {
                    configuredInstance: inst,
                    workflowUUID: wfId,
                };
                return await DBOS.#executor.runStepTempWF(registration.registeredFunction, wfParams, ...rawArgs);
            };
            descriptor.value = invokeWrapper;
            registration.wrappedFunction = invokeWrapper;
            (0, decorators_1.registerFunctionWrapper)(invokeWrapper, registration);
            Object.defineProperty(invokeWrapper, 'name', {
                value: registration.name,
            });
            return descriptor;
        }
        return decorator;
    }
    /**
     * Create a check pointed DBOS step function from  a provided function
     *   Similar to the DBOS.step decorator, but without requiring a decorator
     *   A durable checkpoint will be made after the step completes
     *   This ensures "at least once" execution of the step, and that the step will not
     *    be executed again once the checkpoint is recorded
     * @param func - The function to register as a step
     * @param config - Configuration information for the step, particularly the retry policy and name
     */
    static registerStep(func, config = {}) {
        const name = config.name ?? func.name;
        (0, step_1.validateStepConfig)(config, name);
        const reg = (0, decorators_1.wrapDBOSFunctionAndRegister)(config?.ctorOrProto, config?.className, name, name, func);
        const invokeWrapper = async function (...rawArgs) {
            (0, decorators_1.ensureDBOSIsLaunched)('steps');
            // eslint-disable-next-line @typescript-eslint/no-this-alias
            const inst = this;
            const callFunc = reg.registeredFunction ?? reg.origFunction;
            if (DBOS.isWithinWorkflow()) {
                if (DBOS.isInTransaction()) {
                    throw new error_1.DBOSInvalidWorkflowTransitionError('Invalid call to a `step` function from within a `transaction`');
                }
                if (DBOS.isInStep()) {
                    // There should probably be checks here about the compatibility of the StepConfig...
                    return callFunc.call(this, ...rawArgs);
                }
                return await dbos_executor_1.DBOSExecutor.globalInstance.callStepFunction(callFunc, name, config, inst ?? null, ...rawArgs);
            }
            if ((0, context_1.getNextWFID)(undefined)) {
                throw new error_1.DBOSInvalidWorkflowTransitionError(`Invalid call to step '${name}' outside of a workflow; with directive to start a workflow.`);
            }
            return callFunc.call(this, ...rawArgs);
        };
        (0, decorators_1.registerFunctionWrapper)(invokeWrapper, reg);
        Object.defineProperty(invokeWrapper, 'name', { value: name });
        return invokeWrapper;
    }
    /**
     * Run the enclosed `callback` as a checkpointed step within a DBOS workflow
     * @param callback - function containing code to run
     * @param config - Configuration information for the step, particularly the retry policy
     * @param config.name - The name of the step; if not provided, the function name will be used
     * @returns - result (either obtained from invoking function, or retrieved if run before)
     */
    static runStep(func, config = {}) {
        (0, decorators_1.ensureDBOSIsLaunched)('steps');
        const name = config.name ?? func.name;
        if (DBOS.isWithinWorkflow()) {
            if (DBOS.isInTransaction()) {
                throw new error_1.DBOSInvalidWorkflowTransitionError('Invalid call to a runStep from within a `transaction`');
            }
            if (DBOS.isInStep()) {
                // There should probably be checks here about the compatibility of the StepConfig...
                return func();
            }
            return dbos_executor_1.DBOSExecutor.globalInstance.callStepFunction(func, name, config, null);
        }
        if ((0, context_1.getNextWFID)(undefined)) {
            throw new error_1.DBOSInvalidWorkflowTransitionError(`Invalid call to step '${name}' outside of a workflow; with directive to start a workflow.`);
        }
        return func();
    }
    /**
     * Register serialization recipe; this is used to save/retrieve objects from the DBOS system
     *  database.  This includes workflow inputs, function return values, messages, and events.
     */
    static registerSerialization(serReg) {
        if (DBOS.isInitialized()) {
            throw new TypeError(`Serializers/deserializers should not be registered after DBOS.launch()`);
        }
        (0, serialization_1.registerSerializationRecipe)(serReg);
    }
    /**
     * Decorate a class with the default list of required roles.
     *   This class-level default can be overridden on a per-function basis with `requiredRole`.
     * @param anyOf - The list of roles allowed access; authorization is granted if the authenticated user has any role on the list
     */
    static defaultRequiredRole(anyOf) {
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        function clsdec(ctor) {
            const clsreg = (0, decorators_1.associateClassWithExternal)(decorators_1.DBOS_AUTH, ctor);
            clsreg.requiredRole = anyOf;
            (0, authdecorators_1.registerAuthChecker)();
        }
        return clsdec;
    }
    /**
     * Decorate a method with the default list of required roles.
     * @see `DBOS.defaultRequiredRole`
     * @param anyOf - The list of roles allowed access; authorization is granted if the authenticated user has any role on the list
     */
    static requiredRole(anyOf) {
        function apidec(target, propertyKey, inDescriptor) {
            const rr = (0, decorators_1.associateMethodWithExternal)(decorators_1.DBOS_AUTH, target, undefined, propertyKey.toString(), inDescriptor.value);
            rr.regInfo.requiredRole = anyOf;
            (0, authdecorators_1.registerAuthChecker)();
            inDescriptor.value = rr.registration.wrappedFunction ?? rr.registration.registeredFunction;
            return inDescriptor;
        }
        return apidec;
    }
    /////
    // Patching
    /////
    /**
     * Check if a workflow execution has been patched.
     *
     * Patching allows reexecution of workflows to accommate changes to the workflow logic.
     *
     * Patches check the system database to see which code branch to take.  As this adds overhead,
     *  they may eventually be removed; see `deprecatePatch`.
     *
     * @param patchName Name of the patch to check.
     * @returns true if this is the patched(new) workflow variant, or false if the execution predates the patch
     */
    static async patch(patchName) {
        if (!DBOS.isInWorkflow()) {
            throw new error_1.DBOSInvalidWorkflowTransitionError('`DBOS.patch` must be called from a workflow, and not within a step');
        }
        if (!DBOS.#dbosConfig?.enablePatching) {
            throw new error_1.DBOSInvalidWorkflowTransitionError('Patching is not enabled.  See `enablePatching` in `DBOSConfig`');
        }
        const patched = await dbos_executor_1.DBOSExecutor.globalInstance.systemDatabase.checkPatch(DBOS.workflowID, (0, context_1.functionIDGet)(), patchName, false);
        if (patched.hasEntry) {
            (0, context_1.functionIDGetIncrement)();
        }
        return patched.isPatched;
    }
    /**
     * Check if a workflow execution has been patched, within a plan to eventually remove the unpatched (old) variant.
     *
     * `patch` may be changed to `deprecatePatch` after all unpatched workflows have completed and will not be reexecuted.
     * Once all workflows started with `patch` have completed (in favor of those using `deprecatePatch`), the `deprecatePatch` may then be removed.
     *
     * @param patchName Name of the patch to check.
     * @returns true if this is the patched(new) workflow variant, which it should always be if all unpatched workflows have been retired
     */
    static async deprecatePatch(patchName) {
        if (!DBOS.isInWorkflow()) {
            throw new error_1.DBOSInvalidWorkflowTransitionError('`DBOS.deprecatePatch` must be called from a workflow, and not within a step');
        }
        if (!DBOS.#dbosConfig?.enablePatching) {
            throw new error_1.DBOSInvalidWorkflowTransitionError('Patching is not enabled.  See `enablePatching` in `DBOSConfig`');
        }
        const patched = await dbos_executor_1.DBOSExecutor.globalInstance.systemDatabase.checkPatch(DBOS.workflowID, (0, context_1.functionIDGet)(), patchName, true);
        if (patched.hasEntry) {
            (0, context_1.functionIDGetIncrement)();
        }
        return patched.isPatched;
    }
    /////
    // Registration, etc
    /////
    /**
     * Register a lifecycle listener
     */
    static registerLifecycleCallback(lcl) {
        (0, decorators_1.registerLifecycleCallback)(lcl);
    }
    /**
     * Register a middleware provider
     */
    static registerMiddlewareInstaller(mwp) {
        (0, decorators_1.registerMiddlewareInstaller)(mwp);
    }
    /**
     * Register information to be associated with a DBOS class
     */
    static associateClassWithInfo(external, cls) {
        return (0, decorators_1.associateClassWithExternal)(external, cls);
    }
    /**
     * Register information to be associated with a DBOS function
     */
    static associateFunctionWithInfo(external, func, target) {
        return (0, decorators_1.associateMethodWithExternal)(external, target.ctorOrProto, target.className, target.name ?? func.name, func);
    }
    /**
     * Register information to be associated with a DBOS function
     */
    static associateParamWithInfo(external, func, target) {
        return (0, decorators_1.associateParameterWithExternal)(external, target.ctorOrProto, target.className, target.name ?? func?.name ?? '<unknown>', func, target.param);
    }
    /** Get registrations */
    static getAssociatedInfo(external, cls, funcName) {
        return (0, decorators_1.getRegistrationsForExternal)(external, cls, funcName);
    }
    /////
    // Alert Handling
    /////
    /**
     * Register an alert handler to receive alerts from DBOS Conductor.
     * Only one handler can be registered, and it must be registered before DBOS.launch().
     * If no handler is registered, alerts will be logged.
     *
     * @param handler - Function to handle incoming alerts
     * @throws Error if DBOS is already initialized or if a handler is already registered
     *
     * @example
     * ```typescript
     * DBOS.setAlertHandler(async (name: string, message: string, metadata: Record<string, string>) => {
     *   console.log(`Alert: ${name} - ${message}`, metadata);
     *   // Send to monitoring service, etc.
     * });
     * ```
     */
    static setAlertHandler(handler) {
        if (DBOS.isInitialized()) {
            throw new error_1.DBOSError('Cannot set alert handler after DBOS.launch()');
        }
        if ((0, decorators_1.getAlertHandler)()) {
            throw new error_1.DBOSError('Alert handler is already registered. Only one handler is allowed.');
        }
        (0, decorators_1.setAlertHandler)(handler);
    }
    /////
    // Dynamic Workflow Schedules
    /////
    static async createSchedule(options) {
        (0, decorators_1.ensureDBOSIsLaunched)('createSchedule');
        const reg = (0, decorators_1.getFunctionRegistration)(options.workflowFn);
        if (reg?.isInstance) {
            throw new error_1.DBOSError(`Cannot schedule instance method "${reg.name}". Only static methods and free functions can be used in schedules.`);
        }
        const { className, name: funcName } = (0, decorators_1.getRegisteredFunctionFullName)(options.workflowFn);
        (0, crontab_1.validateCrontab)(options.schedule);
        if (options.options?.cronTimezone) {
            (0, crontab_1.validateTimezone)(options.options.cronTimezone);
        }
        const serializer = dbos_executor_1.DBOSExecutor.globalInstance.serializer;
        const schedInternal = {
            scheduleId: (0, scheduler_1.createScheduleId)(),
            scheduleName: options.scheduleName,
            workflowName: funcName,
            workflowClassName: className,
            schedule: options.schedule,
            status: 'ACTIVE',
            context: await serializer.stringify(options.context),
            lastFiredAt: null,
            automaticBackfill: options.options?.automaticBackfill ?? false,
            cronTimezone: options.options?.cronTimezone ?? null,
            queueName: options.options?.queueName ?? null,
        };
        await runTransactionalInternalStep((client) => dbos_executor_1.DBOSExecutor.globalInstance.systemDatabase.createSchedule(schedInternal, client), 'DBOS.createSchedule');
    }
    static async listSchedules(filters) {
        (0, decorators_1.ensureDBOSIsLaunched)('listSchedules');
        const serializer = dbos_executor_1.DBOSExecutor.globalInstance.serializer;
        const results = await runTransactionalInternalStep((client) => dbos_executor_1.DBOSExecutor.globalInstance.systemDatabase.listSchedules(filters, client), 'DBOS.listSchedules');
        return await Promise.all(results.map((r) => (0, scheduler_1.toWorkflowSchedule)(r, serializer)));
    }
    static async getSchedule(name) {
        (0, decorators_1.ensureDBOSIsLaunched)('getSchedule');
        const serializer = dbos_executor_1.DBOSExecutor.globalInstance.serializer;
        const result = await runTransactionalInternalStep((client) => dbos_executor_1.DBOSExecutor.globalInstance.systemDatabase.getSchedule(name, client), 'DBOS.getSchedule');
        return result ? await (0, scheduler_1.toWorkflowSchedule)(result, serializer) : null;
    }
    static async deleteSchedule(name) {
        (0, decorators_1.ensureDBOSIsLaunched)('deleteSchedule');
        await runTransactionalInternalStep((client) => dbos_executor_1.DBOSExecutor.globalInstance.systemDatabase.deleteSchedule(name, client), 'DBOS.deleteSchedule');
    }
    static async pauseSchedule(name) {
        (0, decorators_1.ensureDBOSIsLaunched)('pauseSchedule');
        await runTransactionalInternalStep((client) => dbos_executor_1.DBOSExecutor.globalInstance.systemDatabase.setScheduleStatus(name, 'PAUSED', client), 'DBOS.pauseSchedule');
    }
    static async resumeSchedule(name) {
        (0, decorators_1.ensureDBOSIsLaunched)('resumeSchedule');
        await runTransactionalInternalStep((client) => dbos_executor_1.DBOSExecutor.globalInstance.systemDatabase.setScheduleStatus(name, 'ACTIVE', client), 'DBOS.resumeSchedule');
    }
    static async updateSchedule(name, updates) {
        (0, decorators_1.ensureDBOSIsLaunched)('updateSchedule');
        if (updates.schedule !== undefined) {
            (0, crontab_1.validateCrontab)(updates.schedule);
        }
        if (updates.cronTimezone) {
            (0, crontab_1.validateTimezone)(updates.cronTimezone);
        }
        const serializer = dbos_executor_1.DBOSExecutor.globalInstance.serializer;
        // Only the keys the caller provided are updated. An `undefined` value leaves a field unchanged; `null` clears a nullable field. Including `context` (even as `undefined`) sets it, since `undefined` is a valid empty context.
        const internalUpdates = {};
        if (updates.schedule !== undefined)
            internalUpdates.schedule = updates.schedule;
        if ('context' in updates)
            internalUpdates.context = await serializer.stringify(updates.context);
        if (updates.automaticBackfill !== undefined)
            internalUpdates.automaticBackfill = updates.automaticBackfill;
        if (updates.cronTimezone !== undefined)
            internalUpdates.cronTimezone = updates.cronTimezone;
        if (updates.queueName !== undefined)
            internalUpdates.queueName = updates.queueName;
        await runTransactionalInternalStep((client) => dbos_executor_1.DBOSExecutor.globalInstance.systemDatabase.updateSchedule(name, internalUpdates, client), 'DBOS.updateSchedule');
    }
    static async applySchedules(schedules) {
        (0, decorators_1.ensureDBOSIsLaunched)('applySchedules');
        if (DBOS.isWithinWorkflow()) {
            throw new error_1.DBOSError('applySchedules cannot be called from within a workflow');
        }
        const serializer = dbos_executor_1.DBOSExecutor.globalInstance.serializer;
        const internals = [];
        for (const sched of schedules) {
            const reg = (0, decorators_1.getFunctionRegistration)(sched.workflowFn);
            if (reg?.isInstance) {
                throw new error_1.DBOSError(`Cannot schedule instance method "${reg.name}". Only static methods and free functions can be used in schedules.`);
            }
            const { className, name: funcName } = (0, decorators_1.getRegisteredFunctionFullName)(sched.workflowFn);
            (0, crontab_1.validateCrontab)(sched.schedule);
            if (sched.cronTimezone) {
                (0, crontab_1.validateTimezone)(sched.cronTimezone);
            }
            internals.push({
                scheduleId: (0, scheduler_1.createScheduleId)(),
                scheduleName: sched.scheduleName,
                workflowName: funcName,
                workflowClassName: className,
                schedule: sched.schedule,
                status: 'ACTIVE',
                context: await serializer.stringify(sched.context),
                lastFiredAt: null,
                automaticBackfill: sched.automaticBackfill ?? false,
                cronTimezone: sched.cronTimezone ?? null,
                queueName: sched.queueName ?? null,
            });
        }
        await dbos_executor_1.DBOSExecutor.globalInstance.systemDatabase.applySchedules(internals);
    }
    static async triggerSchedule(name) {
        (0, decorators_1.ensureDBOSIsLaunched)('triggerSchedule');
        if (DBOS.isWithinWorkflow()) {
            throw new error_1.DBOSError('triggerSchedule cannot be called from within a workflow');
        }
        const executor = dbos_executor_1.DBOSExecutor.globalInstance;
        const workflowID = await (0, scheduler_1.triggerSchedule)(executor.systemDatabase, executor.serializer, name);
        return new workflow_1.RetrievedHandle(executor.systemDatabase, workflowID);
    }
    static async backfillSchedule(name, start, end) {
        (0, decorators_1.ensureDBOSIsLaunched)('backfillSchedule');
        if (DBOS.isWithinWorkflow()) {
            throw new error_1.DBOSError('backfillSchedule cannot be called from within a workflow');
        }
        const executor = dbos_executor_1.DBOSExecutor.globalInstance;
        const workflowIDs = await (0, scheduler_1.backfillSchedule)(executor.systemDatabase, executor.serializer, name, start, end);
        return workflowIDs.map((id) => new workflow_1.RetrievedHandle(executor.systemDatabase, id));
    }
    // ==================== Application Versions ====================
    /**
     * List all registered application versions, ordered by version_timestamp descending (latest first).
     */
    static async listApplicationVersions() {
        (0, decorators_1.ensureDBOSIsLaunched)('listApplicationVersions');
        return await dbos_executor_1.DBOSExecutor.globalInstance.systemDatabase.listApplicationVersions();
    }
    /**
     * Get the latest application version (the one with the highest version_timestamp).
     * Throws if no versions are registered.
     */
    static async getLatestApplicationVersion() {
        (0, decorators_1.ensureDBOSIsLaunched)('getLatestApplicationVersion');
        return await dbos_executor_1.DBOSExecutor.globalInstance.systemDatabase.getLatestApplicationVersion();
    }
    /**
     * Set a version as the latest by updating its version_timestamp to now.
     * @param versionName - The version name to promote to latest
     */
    static async setLatestApplicationVersion(versionName) {
        (0, decorators_1.ensureDBOSIsLaunched)('setLatestApplicationVersion');
        await dbos_executor_1.DBOSExecutor.globalInstance.systemDatabase.updateApplicationVersionTimestamp(versionName, Date.now());
    }
    /**
     * Register a workflow queue and persist its configuration in the system
     * database. Returns a `WorkflowQueue` whose `setX` methods write through
     * to the database so the queue can be reconfigured at runtime.
     *
     * @param name - Unique name of the queue.
     * @param options - Queue parameters plus an `onConflict` policy that
     *   controls what happens when a row with this name already exists.
     *   Defaults to `update_if_latest_version`, which only overwrites when the
     *   running application version matches the latest registered version —
     *   safe for rolling deploys.
     */
    static async registerQueue(name, options = {}) {
        (0, decorators_1.ensureDBOSIsLaunched)('registerQueue');
        const { onConflict = 'update_if_latest_version', ...params } = options;
        wfqueue_1.WorkflowQueue.validateQueueParams(params);
        const sysdb = dbos_executor_1.DBOSExecutor.globalInstance.systemDatabase;
        let updateExisting;
        if (onConflict === 'always_update') {
            updateExisting = true;
        }
        else if (onConflict === 'never_update') {
            updateExisting = false;
        }
        else {
            const latest = await sysdb.getLatestApplicationVersion();
            updateExisting = latest.versionName === utils_1.globalParams.appVersion;
        }
        const record = wfqueue_1.WorkflowQueue.recordFromParams(name, params);
        const inserted = await sysdb.upsertQueue(record, updateExisting);
        const persisted = await sysdb.getQueue(name);
        if (persisted === null) {
            throw new Error(`Queue '${name}' missing from database after upsert`);
        }
        const queue = wfqueue_1.WorkflowQueue._fromRecord(persisted);
        if (inserted) {
            dbos_executor_1.DBOSExecutor.globalInstance.logger.info(`Registered new queue:`);
            (0, wfqueue_1.logQueue)(dbos_executor_1.DBOSExecutor.globalInstance.logger, queue);
        }
        return queue;
    }
    /** Retrieve a database-backed queue by name, or `null` if no row exists. */
    static async retrieveQueue(name) {
        (0, decorators_1.ensureDBOSIsLaunched)('retrieveQueue');
        const record = await dbos_executor_1.DBOSExecutor.globalInstance.systemDatabase.getQueue(name);
        return record === null ? null : wfqueue_1.WorkflowQueue._fromRecord(record);
    }
    /** Delete a database-backed queue. Pending workflows on it are unrecoverable. */
    static async deleteQueue(name) {
        (0, decorators_1.ensureDBOSIsLaunched)('deleteQueue');
        await dbos_executor_1.DBOSExecutor.globalInstance.systemDatabase.deleteQueue(name);
    }
}
exports.DBOS = DBOS;
//# sourceMappingURL=dbos.js.map