"use strict";
var __createBinding = (this && this.__createBinding) || (Object.create ? (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    var desc = Object.getOwnPropertyDescriptor(m, k);
    if (!desc || ("get" in desc ? !m.__esModule : desc.writable || desc.configurable)) {
      desc = { enumerable: true, get: function() { return m[k]; } };
    }
    Object.defineProperty(o, k2, desc);
}) : (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    o[k2] = m[k];
}));
var __setModuleDefault = (this && this.__setModuleDefault) || (Object.create ? (function(o, v) {
    Object.defineProperty(o, "default", { enumerable: true, value: v });
}) : function(o, v) {
    o["default"] = v;
});
var __importStar = (this && this.__importStar) || function (mod) {
    if (mod && mod.__esModule) return mod;
    var result = {};
    if (mod != null) for (var k in mod) if (k !== "default" && Object.prototype.hasOwnProperty.call(mod, k)) __createBinding(result, mod, k);
    __setModuleDefault(result, mod);
    return result;
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.DBOSExecutor = exports.TempWorkflowType = exports.OperationType = exports.DBOS_QUEUE_MAX_PRIORITY = exports.DBOS_QUEUE_MIN_PRIORITY = void 0;
const error_1 = require("./error");
const workflow_1 = require("./workflow");
const step_1 = require("./step");
const collector_1 = require("./telemetry/collector");
const traces_1 = require("./telemetry/traces");
const logs_1 = require("./telemetry/logs");
const exporters_1 = require("./telemetry/exporters");
const system_database_1 = require("./system_database");
const node_crypto_1 = require("node:crypto");
const decorators_1 = require("./decorators");
const context_1 = require("./context");
const serialize_error_1 = require("serialize-error");
const utils_1 = require("./utils");
const serialization_1 = require("./serialization");
const wfqueue_1 = require("./wfqueue");
const debugpoint_1 = require("./debugpoint");
const scheduler_decorator_1 = require("./scheduler/scheduler_decorator");
const scheduler_1 = require("./scheduler/scheduler");
const crypto = __importStar(require("crypto"));
const workflow_management_1 = require("./workflow_management");
const database_utils_1 = require("./database_utils");
const dbosNull = {};
exports.DBOS_QUEUE_MIN_PRIORITY = 1;
exports.DBOS_QUEUE_MAX_PRIORITY = 2 ** 31 - 1; // 2,147,483,647
exports.OperationType = {
    HANDLER: 'handler',
    WORKFLOW: 'workflow',
    TRANSACTION: 'transaction',
    STEP: 'step',
};
exports.TempWorkflowType = {
    step: 'step',
    send: 'send',
};
class DBOSExecutor {
    config;
    initialized;
    // System Database
    systemDatabase;
    // Temporary workflows are created by calling transaction/send/recv directly from the executor class
    static #tempWorkflowName = 'temp_workflow';
    telemetryCollector;
    static defaultNotificationTimeoutSec = 60;
    systemDBSchemaName;
    logger;
    ctxLogger;
    tracer;
    serializer;
    #wfqEnded = undefined;
    executorID = utils_1.globalParams.executorID;
    static globalInstance = undefined;
    /* WORKFLOW EXECUTOR LIFE CYCLE MANAGEMENT */
    constructor(config, { systemDatabase } = {}) {
        this.config = config;
        this.systemDBSchemaName = config.systemDatabaseSchemaName;
        if (config.telemetry.OTLPExporter) {
            const OTLPExporter = new exporters_1.TelemetryExporter(config.telemetry.OTLPExporter);
            this.telemetryCollector = new collector_1.TelemetryCollector(OTLPExporter);
        }
        else {
            // We always setup a collector to drain the signals queue, even if we don't have an exporter.
            this.telemetryCollector = new collector_1.TelemetryCollector();
        }
        this.logger = new logs_1.GlobalLogger(this.telemetryCollector, this.config.telemetry.logs, this.appName);
        this.ctxLogger = new logs_1.DBOSContextualLogger(this.logger, () => (0, traces_1.getActiveSpan)());
        this.tracer = new traces_1.Tracer(this.telemetryCollector, config.telemetry.otelAttributeFormat);
        this.serializer = config.serializer;
        if (systemDatabase) {
            this.logger.debug('Using provided system database'); // XXX print the name or something
            this.systemDatabase = systemDatabase;
        }
        else {
            this.logger.debug('Using Postgres system database');
            this.systemDatabase = new system_database_1.SystemDatabase(this.config.systemDatabaseUrl, this.logger, this.serializer, this.config.sysDbPoolSize, this.config.systemDatabasePool, this.systemDBSchemaName, this.config.useListenNotify, this.config.systemDatabasePollingConcurrency, this.config.notificationCoalesceMs, this.config.enableWakeNotifications);
        }
        new scheduler_decorator_1.ScheduledReceiver(); // Create the scheduler, which registers itself.
        new scheduler_1.DynamicSchedulerLoop(config.schedulerPollingIntervalMs); // Create the dynamic scheduler, which registers itself.
        this.initialized = false;
        DBOSExecutor.globalInstance = this;
    }
    get appName() {
        return this.config.name;
    }
    async init() {
        if (this.initialized) {
            this.logger.error('Workflow executor already initialized!');
            return;
        }
        try {
            await this.systemDatabase.init();
        }
        catch (err) {
            if (err instanceof error_1.DBOSInitializationError) {
                throw err;
            }
            this.logger.error(err);
            let message = 'Failed to initialize workflow executor: ';
            if (err instanceof AggregateError) {
                for (const error of err.errors) {
                    message += `${error.message}; `;
                }
            }
            else if (err instanceof Error) {
                message += err.message;
            }
            else {
                message += String(err);
            }
            throw new error_1.DBOSInitializationError(message, err instanceof Error ? err : undefined);
        }
        this.initialized = true;
        // Compute the application version if not provided
        if (utils_1.globalParams.appVersion === '') {
            utils_1.globalParams.appVersion = this.computeAppVersion();
            utils_1.globalParams.wasComputed = true;
        }
        // Any initialization hooks
        const classnames = (0, decorators_1.getAllRegisteredClassNames)();
        for (const cls of classnames) {
            // Init its configurations
            const creg = (0, decorators_1.getClassRegistrationByName)(cls);
            for (const [_cfgname, cfg] of creg.configuredInstances) {
                await cfg.initialize();
            }
        }
        this.logger.info(`Initializing DBOS (v${utils_1.globalParams.dbosVersion})`);
        this.logger.info(`System Database URL: ${(0, database_utils_1.maskDatabaseUrl)(this.config.systemDatabaseUrl)}`);
        this.logger.info(`Executor ID: ${this.executorID}`);
        this.logger.info(`Application version: ${utils_1.globalParams.appVersion}`);
        await this.recoverPendingWorkflows([this.executorID]);
        this.logger.info('DBOS launched!');
    }
    async destroy() {
        try {
            await this.systemDatabase.awaitRunningWorkflows();
            await this.systemDatabase.destroy();
            await this.logger.destroy();
        }
        catch (err) {
            const e = err;
            this.logger.error(e);
            throw err;
        }
    }
    // This could return WF, or the function underlying a temp wf
    #getFunctionInfoFromWFStatus(wf) {
        const methReg = (0, decorators_1.getFunctionRegistrationByName)(wf.workflowClassName, wf.workflowName);
        return { methReg, configuredInst: (0, decorators_1.getConfiguredInstance)(wf.workflowClassName, wf.workflowConfigName) };
    }
    static async reviveResultOrError(r, serializer) {
        if (r.error) {
            throw await (0, serialization_1.deserializeResError)(r.error, r.serialization ?? null, serializer);
        }
        return (await (0, serialization_1.deserializeValue)(r.output ?? null, r.serialization ?? null, serializer));
    }
    async workflow(wf, params, ...args) {
        return this.internalWorkflow(wf, params, undefined, undefined, ...args);
    }
    /**
     * Build, without persisting, an ENQUEUED status row for `wf` on `options.queueName`.
     *
     * For batch enqueuers (e.g. a Kafka consumer) that persist many rows in one transaction via
     * {@link SystemDatabase.enqueueWorkflows}. Any ambient DBOS context is ignored: the workflow ID and
     * enqueue options are passed explicitly, and the row inherits no parent, auth, or attributes.
     */
    async prepareEnqueuedWorkflow(wf, args, options) {
        const wInfo = (0, decorators_1.getFunctionRegistration)(wf);
        if (!wInfo || !wInfo.workflowConfig) {
            throw new error_1.DBOSNotRegisteredError(wf.name, `${wf.name} is not a registered workflow function`);
        }
        if (wInfo.isInstance) {
            // The row would carry no config name, so the dequeuer could not find the instance to bind and
            // would run the workflow with a null `this`.
            throw new error_1.DBOSError(`Cannot enqueue instance method "${wInfo.name}" in a batch. Only static methods and free functions can be used.`);
        }
        const { name: workflowName, className: workflowClassName } = (0, decorators_1.getRegisteredFunctionFullName)(wf);
        const serializationType = wInfo.workflowConfig.serialization;
        let funcArgs;
        try {
            funcArgs = await (0, serialization_1.serializeFunctionInputOutput)(serializationType === 'portable' ? { positionalArgs: args } : args, [workflowName, '<arguments>'], this.serializer, serializationType);
        }
        catch (e) {
            // Tagged so a batch enqueuer can tell "these arguments are bad" apart from the failures above,
            // which condemn every workflow it would enqueue rather than just this one.
            throw new error_1.DBOSInvalidWorkflowInputError(workflowName, e);
        }
        return {
            workflowUUID: options.workflowID,
            status: workflow_1.StatusString.ENQUEUED,
            workflowName,
            workflowClassName,
            workflowConfigName: '',
            queueName: options.queueName,
            output: null,
            error: null,
            authenticatedUser: '',
            assumedRole: '',
            authenticatedRoles: [],
            request: {},
            executorId: utils_1.globalParams.executorID,
            applicationVersion: utils_1.globalParams.appVersion,
            applicationID: utils_1.globalParams.appID,
            createdAt: Date.now(),
            input: funcArgs.stringified,
            priority: 0,
            queuePartitionKey: options.queuePartitionKey,
            serialization: funcArgs.sername,
        };
    }
    // If callerWFID and functionID are set, it means the workflow is invoked from within a workflow.
    async internalWorkflow(wf, params, callerID, callerFunctionID, ...args) {
        const workflowID = params.workflowUUID ? params.workflowUUID : (0, node_crypto_1.randomUUID)();
        const presetID = params.workflowUUID ? true : false;
        const timeoutMS = params.timeoutMS ?? undefined;
        // If a timeout is explicitly specified, use it over any propagated deadline
        const deadlineEpochMS = params.timeoutMS
            ? // Queued workflows are assigned a deadline on dequeue. Otherwise, compute the deadline immediately
                params.queueName
                    ? undefined
                    : Date.now() + params.timeoutMS
            : // if no timeout is specified, use the propagated deadline (if any)
                params.deadlineEpochMS;
        const priority = params?.enqueueOptions?.priority;
        const pctx = { ...(0, context_1.getCurrentContextStore)() }; // function ID was already incremented...
        let wConfig = {};
        const wInfo = (0, decorators_1.getFunctionRegistration)(wf);
        const wfNames = (0, decorators_1.getRegisteredFunctionFullName)(wf);
        let wfname = wfNames.name;
        let wfclassname = wfNames.className;
        const isTempWorkflow = DBOSExecutor.#tempWorkflowName === wfname || !!params.tempWfType;
        if (!isTempWorkflow) {
            if (!wInfo || !wInfo.workflowConfig) {
                throw new error_1.DBOSNotRegisteredError(wf.name);
            }
            wConfig = wInfo.workflowConfig;
        }
        else if (params.tempWfName) {
            wfname = params.tempWfName;
            wfclassname = params.tempWfClass ?? '';
        }
        const maxRecoveryAttempts = wConfig.maxRecoveryAttempts
            ? wConfig.maxRecoveryAttempts
            : workflow_1.DEFAULT_MAX_RECOVERY_ATTEMPTS;
        const span = this.tracer.startSpan(wfname, {
            status: workflow_1.StatusString.PENDING,
            operationUUID: workflowID,
            operationType: exports.OperationType.WORKFLOW,
            operationName: wInfo?.name ?? wf.name,
            authenticatedUser: pctx?.authenticatedUser ?? '',
            authenticatedRoles: pctx?.authenticatedRoles ?? [],
            assumedRole: pctx?.assumedRole ?? '',
        });
        let serializationType = wInfo?.workflowConfig?.serialization;
        const funcArgs = await (0, serialization_1.serializeFunctionInputOutput)(serializationType === 'portable' ? { positionalArgs: args } : args, [wfname, '<arguments>'], this.serializer, serializationType);
        args =
            serializationType === 'portable'
                ? funcArgs.deserialized.positionalArgs
                : funcArgs.deserialized;
        const delaySeconds = params.enqueueOptions?.delaySeconds;
        let delayUntilEpochMS = params.queueName !== undefined && delaySeconds !== undefined && delaySeconds > 0
            ? Date.now() + delaySeconds * 1000
            : undefined;
        const debounceDeadlineEpochMS = params.enqueueOptions?.debounceDeadlineEpochMS;
        if (delayUntilEpochMS !== undefined &&
            debounceDeadlineEpochMS !== undefined &&
            debounceDeadlineEpochMS < delayUntilEpochMS) {
            delayUntilEpochMS = debounceDeadlineEpochMS;
        }
        const internalStatus = {
            workflowUUID: workflowID,
            status: params.queueName !== undefined
                ? delayUntilEpochMS !== undefined
                    ? workflow_1.StatusString.DELAYED
                    : workflow_1.StatusString.ENQUEUED
                : workflow_1.StatusString.PENDING,
            workflowName: wfname,
            workflowClassName: wfclassname,
            workflowConfigName: params.configuredInstance?.name || '',
            queueName: params.queueName,
            output: null,
            error: null,
            authenticatedUser: pctx?.authenticatedUser || '',
            assumedRole: pctx?.assumedRole || '',
            authenticatedRoles: pctx?.authenticatedRoles || [],
            request: pctx?.request || {},
            executorId: utils_1.globalParams.executorID,
            applicationVersion: params.enqueueOptions?.applicationVersion ?? utils_1.globalParams.appVersion,
            applicationID: utils_1.globalParams.appID,
            createdAt: Date.now(), // Remember the start time of this workflow,
            timeoutMS: timeoutMS,
            deadlineEpochMS: deadlineEpochMS,
            input: funcArgs.stringified,
            deduplicationID: params.enqueueOptions?.deduplicationID,
            priority: priority ?? 0,
            queuePartitionKey: params.enqueueOptions?.queuePartitionKey,
            parentWorkflowID: callerID,
            serialization: funcArgs.sername,
            delayUntilEpochMS,
            attributes: params.workflowAttributes,
            debounceDeadlineEpochMS,
            isDebounced: params.enqueueOptions?.isDebounced ?? false,
        };
        if (isTempWorkflow) {
            internalStatus.workflowName = `${DBOSExecutor.#tempWorkflowName}-${params.tempWfType}-${params.tempWfName}`;
        }
        let $deadlineEpochMS = undefined;
        let shouldExecute = undefined;
        // Synchronously set the workflow's status to PENDING and record workflow inputs.
        // We have to do it for all types of workflows because operation_outputs table has a foreign key constraint on workflow status table.
        if (callerFunctionID !== undefined && callerID !== undefined) {
            const result = await this.systemDatabase.getOperationResultAndThrowIfCancelled(callerID, callerFunctionID);
            if (result) {
                if (result.error) {
                    throw await (0, serialization_1.deserializeResError)(result.error, result.serialization ?? null, this.serializer);
                }
                return new workflow_1.RetrievedHandle(this.systemDatabase, result.childWorkflowID);
            }
        }
        let ires;
        try {
            ires = await this.systemDatabase.initWorkflowStatus(internalStatus, (0, node_crypto_1.randomUUID)(), {
                maxRetries: maxRecoveryAttempts,
                isDequeuedRequest: params.isQueueDispatch,
                isRecoveryRequest: params.isRecoveryDispatch,
            });
            serializationType = ires.serialization === serialization_1.DBOSPortableJSON.name() ? 'portable' : undefined;
        }
        catch (e) {
            // For 'return-existing' enqueues we don't pre-record the dedup error: the wrapper will
            // catch it, attach to the existing workflow, and record the child mapping itself.
            if (e instanceof error_1.DBOSQueueDuplicatedError &&
                callerID &&
                callerFunctionID &&
                params.duplicationPolicy !== 'return-existing') {
                const sererr = await (0, serialization_1.serializeResError)(e, this.serializer, undefined); // This is a step result
                await this.systemDatabase.recordOperationResult(callerID, callerFunctionID, internalStatus.workflowName, true, Date.now(), Date.now(), { error: sererr.serializedValue, serialization: sererr.serialization });
            }
            this.tracer.endSpan(span);
            throw e;
        }
        if (callerFunctionID !== undefined && callerID !== undefined) {
            await this.systemDatabase.recordOperationResult(callerID, callerFunctionID, internalStatus.workflowName, true, Date.now(), Date.now(), {
                childWorkflowID: workflowID,
            });
        }
        $deadlineEpochMS = ires.deadlineEpochMS;
        shouldExecute = ires.shouldExecuteOnThisExecutor;
        await (0, debugpoint_1.debugTriggerPoint)(debugpoint_1.DEBUG_TRIGGER_WORKFLOW_ENQUEUE);
        async function callPromiseWithTimeout(callPromise, deadlineEpochMS, sysdb) {
            let timeoutID = undefined;
            const timeoutResult = {};
            const timeoutPromise = new Promise((_, reject) => {
                timeoutID = setTimeout(reject, deadlineEpochMS - Date.now(), timeoutResult);
            });
            try {
                return await Promise.race([callPromise, timeoutPromise]);
            }
            catch (err) {
                if (err === timeoutResult) {
                    await sysdb.cancelWorkflows([workflowID]);
                    await callPromise.catch(() => { });
                    throw new error_1.DBOSWorkflowCancelledError(workflowID);
                }
                throw err;
            }
            finally {
                clearTimeout(timeoutID);
            }
        }
        // Release the running-workflow map entry before the terminal outcome
        // becomes durable: once it is visible, a resume can re-dispatch this
        // workflow ID to this executor, and a stale entry would block that
        // dispatch. Guarded so the backstop in registerRunningWorkflow's finally
        // never deletes an entry a resumed run has re-acquired.
        let runningWorkflowReleased = false;
        const releaseRunningWorkflow = () => {
            if (runningWorkflowReleased)
                return;
            runningWorkflowReleased = true;
            this.systemDatabase.clearRunningWorkflow(workflowID);
        };
        const eserializer = this.serializer;
        async function handleWorkflowError(err, exec) {
            // Record the error.
            const e = err;
            exec.logger.error(e);
            e.dbos_already_logged = true;
            const sererr = await (0, serialization_1.serializeResErrorWithSerializer)(e, eserializer, ires.serialization ?? null);
            internalStatus.error = sererr.serializedValue;
            internalStatus.status = workflow_1.StatusString.ERROR;
            releaseRunningWorkflow();
            await exec.systemDatabase.recordWorkflowError(workflowID, internalStatus);
            span.setStatus({ code: traces_1.SpanStatusCode.ERROR, message: e.message });
            return await (0, serialization_1.deserializeResError)(sererr.serializedValue, sererr.serialization, eserializer);
        }
        const runWorkflow = async () => {
            let result;
            // Execute the workflow.
            try {
                const callResult = await (0, traces_1.runWithTrace)(span, async () => {
                    return await (0, context_1.runWithParentContext)(pctx, {
                        presetID,
                        workflowTimeoutMS: undefined, // Becomes deadline
                        deadlineEpochMS,
                        workflowId: workflowID,
                        logger: this.ctxLogger,
                        curWFFunctionId: undefined,
                        serializationType,
                    }, () => {
                        const callPromise = wf.call(params.configuredInstance, ...args);
                        if ($deadlineEpochMS === undefined) {
                            return callPromise;
                        }
                        else {
                            return callPromiseWithTimeout(callPromise, $deadlineEpochMS, this.systemDatabase);
                        }
                    });
                });
                result = callResult;
                const funcResult = await (0, serialization_1.serializeFunctionInputOutputWithSerializer)(result, [wfname, '<result>'], this.serializer, ires.serialization);
                result = funcResult.deserialized;
                internalStatus.output = funcResult.stringified;
                internalStatus.status = workflow_1.StatusString.SUCCESS;
                releaseRunningWorkflow();
                await this.systemDatabase.recordWorkflowOutput(workflowID, internalStatus);
                span.setStatus({ code: traces_1.SpanStatusCode.OK });
            }
            catch (err) {
                if (err instanceof error_1.DBOSWorkflowConflictError) {
                    // Retrieve the handle and wait for the result.
                    const retrievedHandle = this.retrieveWorkflow(workflowID);
                    result = await retrievedHandle.getResult();
                    span.setAttribute('cached', true);
                    span.setStatus({ code: traces_1.SpanStatusCode.OK });
                }
                else if (err instanceof error_1.DBOSWorkflowCancelledError) {
                    span.setStatus({ code: traces_1.SpanStatusCode.ERROR, message: err.message });
                    internalStatus.error = err.message;
                    if (err.workflowID === workflowID) {
                        // The row is already durably CANCELLED (resumable): release now so
                        // a resume re-dispatched here is not blocked during async unwind.
                        releaseRunningWorkflow();
                        internalStatus.status = workflow_1.StatusString.CANCELLED;
                        throw err;
                    }
                    else {
                        const e = new error_1.DBOSAwaitedWorkflowCancelledError(err.workflowID);
                        await handleWorkflowError(e, this);
                        throw e;
                    }
                }
                else {
                    // If we want to be consistent about what is thrown (stored result vs live)
                    //  we would have to do this.  It is a breaking change in the sense that it
                    //  is a behavior change, but it would "break" things that are already broken
                    if (serializationType === 'portable') {
                        throw await handleWorkflowError(err, this);
                    }
                    await handleWorkflowError(err, this);
                    throw err;
                }
            }
            finally {
                this.tracer.endSpan(span);
            }
            return result;
        };
        if (shouldExecute &&
            (params.queueName === undefined || params.executeWorkflow) &&
            !this.systemDatabase.checkForRunningWorkflow(workflowID)) {
            const workflowPromise = runWorkflow();
            this.systemDatabase.registerRunningWorkflow(workflowID, workflowPromise, releaseRunningWorkflow, params.queueName, params.enqueueOptions?.queuePartitionKey);
            // Return the normal handle that doesn't capture errors.
            return new workflow_1.InvokedHandle(this.systemDatabase, workflowPromise, workflowID, wf.name);
        }
        else {
            return new workflow_1.RetrievedHandle(this.systemDatabase, workflowID);
        }
    }
    /**
     * Look up an in-memory workflow queue by name. Returns `undefined` for
     * names that are not registered in-process; database-backed queues are
     * not in this map, so callers using this for sync validation should treat
     * `undefined` as "no in-process information" rather than as an error.
     */
    getQueueByName(name) {
        return wfqueue_1.wfQueueRunner.wfQueuesByName.get(name);
    }
    async runStepTempWF(stepFn, params, ...args) {
        return await (await this.startStepTempWF(stepFn, params, undefined, undefined, ...args)).getResult();
    }
    async startStepTempWF(stepFn, params, callerWFID, callerFunctionID, ...args) {
        // Create a workflow and call external.
        const temp_workflow = async (...args) => {
            return await this.callStepFunction(stepFn, undefined, undefined, params.configuredInstance ?? null, ...args);
        };
        return await this.internalWorkflow(temp_workflow, {
            ...params,
            tempWfType: exports.TempWorkflowType.step,
            tempWfName: (0, decorators_1.getRegisteredFunctionName)(stepFn),
            tempWfClass: (0, decorators_1.getRegisteredFunctionClassName)(stepFn),
        }, callerWFID, callerFunctionID, ...args);
    }
    /**
     * Execute a step function.
     * If it encounters any error, retry according to its configured retry policy until the maximum number of attempts is reached, then throw an DBOSError.
     * The step may execute many times, but once it is complete, it will not re-execute.
     */
    async callStepFunction(stepFn, stepFnName, stepConfig, clsInst, ...args) {
        stepFnName = stepFnName ?? stepFn.name ?? '<unnamed>';
        const startTime = Date.now();
        if (!stepConfig) {
            const stepReg = (0, decorators_1.getFunctionRegistration)(stepFn);
            stepConfig = stepReg?.stepConfig;
        }
        if (stepConfig === undefined) {
            throw new error_1.DBOSNotRegisteredError(stepFnName);
        }
        (0, step_1.validateStepConfig)(stepConfig, stepFnName);
        // Intentionally advance the function ID before any awaits, then work with a copy of the context.
        const funcID = (0, context_1.functionIDGetIncrement)();
        const lctx = { ...(0, context_1.getCurrentContextStore)() };
        const wfid = lctx.workflowId;
        await this.systemDatabase.checkIfCanceled(wfid);
        const maxRetryIntervalSec = 3600; // Maximum retry interval: 1 hour
        const span = this.tracer.startSpan(stepFnName, {
            operationUUID: wfid,
            operationType: exports.OperationType.STEP,
            operationName: stepFnName,
            authenticatedUser: lctx.authenticatedUser ?? '',
            assumedRole: lctx.assumedRole ?? '',
            authenticatedRoles: lctx.authenticatedRoles ?? [],
            retriesAllowed: stepConfig.retriesAllowed,
            intervalSeconds: stepConfig.intervalSeconds,
            maxAttempts: stepConfig.maxAttempts,
            backoffRate: stepConfig.backoffRate,
            timeoutMS: stepConfig.timeoutMS,
        });
        // For errors that propagate without being recorded as the step's outcome (e.g. workflow cancellation):
        // mark the span failed and end it before rethrowing.
        const endSpanAndRethrow = (e) => {
            span.setStatus({ code: traces_1.SpanStatusCode.ERROR, message: e.message });
            this.tracer.endSpan(span);
            throw e;
        };
        // Check if this execution previously happened, returning its original result if it did.
        const checkr = await this.systemDatabase
            .getOperationResultAndThrowIfCancelled(wfid, funcID)
            .catch(endSpanAndRethrow);
        if (checkr) {
            if (checkr.functionName !== stepFnName) {
                endSpanAndRethrow(new error_1.DBOSUnexpectedStepError(wfid, funcID, stepFnName, checkr.functionName ?? '?'));
            }
            const check = await DBOSExecutor.reviveResultOrError(checkr, this.serializer).catch(endSpanAndRethrow);
            span.setAttribute('cached', true);
            span.setStatus({ code: traces_1.SpanStatusCode.OK });
            this.tracer.endSpan(span);
            return check;
        }
        const maxAttempts = stepConfig.maxAttempts ?? 3;
        const timeoutMS = stepConfig.timeoutMS;
        // Run a single attempt of the step function.
        // If `timeoutMS` is set, race the attempt against a timer, firing the attempt's AbortSignal
        // (exposed as `DBOS.stepStatus.timeoutSignal`) so the step can cancel its underlying operation.
        // A timed-out attempt is abandoned: it has no path to record a result, and its eventual
        // settlement is discarded (Promise.race keeps the abandoned promise's rejection handled,
        // so it never surfaces as an unhandled rejection).
        const invokeStepAttempt = async (attemptNum) => {
            const timeoutAbort = timeoutMS === undefined ? undefined : new AbortController();
            let cresult;
            const attemptPromise = (0, traces_1.runWithTrace)(span, async () => {
                await (0, context_1.runInStepContext)(lctx, funcID, maxAttempts, attemptNum, timeoutAbort?.signal, async () => {
                    const sf = stepFn;
                    cresult = await sf.call(clsInst, ...args);
                });
            });
            if (timeoutMS === undefined) {
                await attemptPromise;
                return cresult;
            }
            const timeoutError = new error_1.DBOSStepTimeoutError(stepFnName, timeoutMS);
            let timeoutID = undefined;
            const timeoutPromise = new Promise((_, reject) => {
                timeoutID = setTimeout(() => {
                    timeoutAbort.abort(timeoutError);
                    reject(timeoutError);
                }, timeoutMS);
            });
            try {
                await Promise.race([attemptPromise, timeoutPromise]);
                return cresult;
            }
            finally {
                clearTimeout(timeoutID);
            }
        };
        // Execute the step function.  If it throws an exception, retry with exponential backoff.
        // After reaching the maximum number of retries, throw an DBOSError.
        let result = dbosNull;
        let err = dbosNull;
        const errors = [];
        if (stepConfig.retriesAllowed) {
            let attemptNum = 0;
            let intervalSeconds = stepConfig.intervalSeconds ?? 1;
            if (intervalSeconds > maxRetryIntervalSec) {
                this.logger.warn(`Step config interval exceeds maximum allowed interval, capped to ${maxRetryIntervalSec} seconds!`);
            }
            while (result === dbosNull && attemptNum++ < (maxAttempts ?? 3)) {
                // Outside the try so workflow cancellation propagates immediately instead of consuming the remaining attempts
                await this.systemDatabase.checkIfCanceled(wfid).catch(endSpanAndRethrow);
                try {
                    result = await invokeStepAttempt(attemptNum);
                }
                catch (error) {
                    const e = error;
                    if (stepConfig.shouldRetry) {
                        try {
                            const shouldRetry = await stepConfig.shouldRetry(e);
                            if (!shouldRetry) {
                                err = e;
                                this.logger.warn(`Non-retryable error in step. Attempt ${attemptNum} of ${maxAttempts}. ${e.stack}`);
                                span.addEvent(`Step attempt ${attemptNum + 1} failed`, { retryIntervalSeconds: intervalSeconds, error: e.message, shouldRetry: false }, performance.now());
                                break;
                            }
                        }
                        catch (retryError) {
                            err = retryError;
                            this.logger.warn(`Step retry predicate failed. Attempt ${attemptNum} of ${maxAttempts}. ${err.stack}`);
                            span.addEvent(`Step retry predicate failed after attempt ${attemptNum + 1}`, { error: e.message, shouldRetryError: err.message }, performance.now());
                            break;
                        }
                    }
                    errors.push(e);
                    this.logger.warn(`Error in step being automatically retried. Attempt ${attemptNum} of ${maxAttempts}. ${e.stack}`);
                    span.addEvent(`Step attempt ${attemptNum + 1} failed`, { retryIntervalSeconds: intervalSeconds, error: error.message }, performance.now());
                    if (attemptNum < maxAttempts) {
                        // Sleep for an interval, then increase the interval by backoffRate.
                        // Cap at the maximum allowed retry interval.
                        await (0, utils_1.sleepms)(intervalSeconds * 1000);
                        intervalSeconds *= stepConfig.backoffRate ?? 2;
                        intervalSeconds = intervalSeconds < maxRetryIntervalSec ? intervalSeconds : maxRetryIntervalSec;
                    }
                }
            }
        }
        else {
            try {
                result = await invokeStepAttempt(undefined);
            }
            catch (error) {
                err = error;
            }
        }
        // `result` can only be dbosNull when the step timed out
        if (result === dbosNull) {
            // Record the error, then throw it.
            err = err === dbosNull ? new error_1.DBOSMaxStepRetriesError(stepFnName, maxAttempts, errors) : err;
            await this.systemDatabase.recordOperationResult(wfid, funcID, stepFnName, true, startTime, Date.now(), {
                error: await this.serializer.stringify((0, serialize_error_1.serializeError)(err)),
                serialization: this.serializer.name(),
            });
            span.setStatus({ code: traces_1.SpanStatusCode.ERROR, message: err.message });
            this.tracer.endSpan(span);
            throw err;
        }
        else {
            // Record the execution and return.
            const funcResult = await (0, serialization_1.serializeFunctionInputOutput)(result, [stepFnName, '<result>'], this.serializer);
            await this.systemDatabase.recordOperationResult(wfid, funcID, stepFnName, true, startTime, Date.now(), {
                output: funcResult.stringified,
                serialization: funcResult.sername,
            });
            span.setStatus({ code: traces_1.SpanStatusCode.OK });
            this.tracer.endSpan(span);
            return funcResult.deserialized;
        }
    }
    /**
     * Wait for a workflow to emit an event, then return its value.
     */
    async getEvent(workflowUUID, key, timeoutSeconds = DBOSExecutor.defaultNotificationTimeoutSec, pollingIntervalMs) {
        const evt = await this.systemDatabase.getEvent(workflowUUID, key, timeoutSeconds, undefined, pollingIntervalMs);
        return (await (0, serialization_1.deserializeValue)(evt.serializedValue, evt.serialization, this.serializer));
    }
    /**
     * Fork a workflow.
     * The forked workflow will be assigned a new ID.
     */
    forkWorkflow(workflowID, startStep, options = {}) {
        const newWorkflowID = options.newWorkflowID ?? (0, context_1.getNextWFID)(undefined);
        return (0, workflow_management_1.forkWorkflow)(this.systemDatabase, workflowID, startStep, { ...options, newWorkflowID });
    }
    /**
     * Retrieve a handle for a workflow UUID.
     */
    retrieveWorkflow(workflowID) {
        return new workflow_1.RetrievedHandle(this.systemDatabase, workflowID);
    }
    async runInternalStep(callback, functionName, workflowID, functionID, childWfId) {
        const startTime = Date.now();
        const result = await this.systemDatabase.getOperationResultAndThrowIfCancelled(workflowID, functionID);
        if (result) {
            if (result.functionName !== functionName) {
                throw new error_1.DBOSUnexpectedStepError(workflowID, functionID, functionName, result.functionName);
            }
            return await DBOSExecutor.reviveResultOrError(result, this.serializer);
        }
        try {
            const output = await callback();
            const funcOutput = await (0, serialization_1.serializeFunctionInputOutput)(output, [functionName, '<result>'], this.serializer);
            await this.systemDatabase.recordOperationResult(workflowID, functionID, functionName, true, startTime, Date.now(), {
                output: funcOutput.stringified,
                childWorkflowID: childWfId,
            });
            return funcOutput.deserialized;
        }
        catch (e) {
            if (e instanceof error_1.DBOSWorkflowCancelledError && e.workflowID === workflowID) {
                // The calling workflow's own cancellation interrupted the step; it did not
                // complete. Don't checkpoint it, so a resumed workflow re-executes it.
                throw e;
            }
            await this.systemDatabase.recordOperationResult(workflowID, functionID, functionName, false, startTime, Date.now(), {
                error: await this.serializer.stringify((0, serialize_error_1.serializeError)(e)),
                childWorkflowID: childWfId,
            });
            throw e;
        }
    }
    async getWorkflowStatus(workflowID, callerID, callerFN) {
        // use sysdb getWorkflowStatus directly in order to support caller ID/FN params
        const status = await this.systemDatabase.getWorkflowStatus(workflowID, callerID, callerFN);
        return status ? await (0, workflow_management_1.toWorkflowStatus)(status, this.serializer) : null;
    }
    async listWorkflows(input) {
        return (0, workflow_management_1.listWorkflows)(this.systemDatabase, input);
    }
    async listQueuedWorkflows(input) {
        return (0, workflow_management_1.listQueuedWorkflows)(this.systemDatabase, input);
    }
    async listWorkflowSteps(workflowID, loadOutput = true, options) {
        return (0, workflow_management_1.listWorkflowSteps)(this.systemDatabase, workflowID, loadOutput, options);
    }
    /* INTERNAL HELPERS */
    /**
     * A recovery process that by default runs during executor init time.
     * It runs to completion all pending workflows that were executing when the previous executor failed.
     */
    async recoverPendingWorkflows(executorIDs = ['local']) {
        const handlerArray = [];
        for (const execID of executorIDs) {
            this.logger.debug(`Recovering workflows assigned to executor: ${execID}`);
            const requeuedWorkflowIDs = await this.systemDatabase.reenqueuePendingQueuedWorkflows(execID, utils_1.globalParams.appVersion);
            if (requeuedWorkflowIDs.length > 0) {
                this.logger.info(`Re-enqueued ${requeuedWorkflowIDs.length} queued workflows from application version ${utils_1.globalParams.appVersion}`);
                for (const workflowID of requeuedWorkflowIDs) {
                    handlerArray.push(this.retrieveWorkflow(workflowID));
                }
            }
            const pendingWorkflows = await this.systemDatabase.getPendingWorkflows(execID, utils_1.globalParams.appVersion);
            if (pendingWorkflows.length > 0) {
                this.logger.info(`Recovering ${pendingWorkflows.length} workflows from application version ${utils_1.globalParams.appVersion}`);
            }
            else if (requeuedWorkflowIDs.length === 0) {
                this.logger.info(`No workflows to recover from application version ${utils_1.globalParams.appVersion}`);
            }
            for (const pendingWorkflow of pendingWorkflows) {
                this.logger.debug(`Recovering workflow: ${pendingWorkflow.workflowUUID}. Queue name: ${pendingWorkflow.queueName}`);
                try {
                    // If the workflow is member of a queue, re-enqueue it.
                    if (pendingWorkflow.queueName) {
                        const cleared = await this.systemDatabase.clearQueueAssignment(pendingWorkflow.workflowUUID);
                        if (cleared) {
                            handlerArray.push(this.retrieveWorkflow(pendingWorkflow.workflowUUID));
                        }
                        else {
                            handlerArray.push(await this.executeWorkflowId(pendingWorkflow.workflowUUID, { isRecoveryDispatch: true }));
                        }
                    }
                    else {
                        handlerArray.push(await this.executeWorkflowId(pendingWorkflow.workflowUUID, { isRecoveryDispatch: true }));
                    }
                }
                catch (e) {
                    this.logger.warn(`Recovery of workflow ${pendingWorkflow.workflowUUID} failed: ${e.message}`);
                }
            }
        }
        return handlerArray;
    }
    async initEventReceivers(listenQueues) {
        this.#wfqEnded = wfqueue_1.wfQueueRunner.dispatchLoop(this, listenQueues, this.config.maxConcurrentQueueDispatches);
        for (const lcl of (0, decorators_1.getLifecycleListeners)()) {
            await lcl.initialize?.();
        }
    }
    async deactivateEventReceivers(stopQueueThread = true) {
        this.logger.debug('Deactivating lifecycle listeners');
        for (const lcl of (0, decorators_1.getLifecycleListeners)()) {
            try {
                await lcl.destroy?.();
            }
            catch (err) {
                const e = err;
                this.logger.warn(`Error destroying lifecycle listener: ${e.message}`);
            }
        }
        this.logger.debug('Deactivating queue runner');
        if (stopQueueThread) {
            try {
                wfqueue_1.wfQueueRunner.stop();
                await this.#wfqEnded;
            }
            catch (err) {
                const e = err;
                this.logger.warn(`Error destroying wf queue runner: ${e.message}`);
            }
        }
    }
    async executeWorkflowId(workflowID, options) {
        const wfStatus = await this.systemDatabase.getWorkflowStatus(workflowID);
        if (!wfStatus) {
            this.logger.error(`Failed to find workflow status for workflowUUID: ${workflowID}`);
            throw new error_1.DBOSError(`Failed to find workflow status for workflow UUID: ${workflowID}`);
        }
        if (!wfStatus?.input) {
            this.logger.error(`Failed to find inputs for workflowUUID: ${workflowID}`);
            throw new error_1.DBOSError(`Failed to find inputs for workflow UUID: ${workflowID}`);
        }
        let inputs;
        try {
            inputs = await (0, serialization_1.deserializePositionalArgs)(wfStatus.input, wfStatus.serialization, this.serializer);
        }
        catch (err) {
            // If deserialization fails, record the error on the workflow
            // so it transitions to ERROR instead of being stuck in PENDING.
            this.logger.error(`Failed to deserialize inputs for workflow ${workflowID}: ${err.message}`);
            const sererr = await (0, serialization_1.serializeResErrorWithSerializer)(err, this.serializer, wfStatus.serialization);
            wfStatus.error = sererr.serializedValue;
            await this.systemDatabase.recordWorkflowError(workflowID, wfStatus);
            throw err;
        }
        const recoverCtx = this.#getRecoveryContext(workflowID, wfStatus);
        const { methReg, configuredInst } = this.#getFunctionInfoFromWFStatus(wfStatus);
        // If starting a new workflow, assign a new UUID. Otherwise, use the workflow's original UUID.
        const workflowStartID = !!options?.startNewWorkflow ? undefined : workflowID;
        const enqueueOptions = wfStatus.queuePartitionKey !== undefined ? { queuePartitionKey: wfStatus.queuePartitionKey } : undefined;
        if (methReg?.workflowConfig) {
            return await (0, context_1.runWithTopContext)(recoverCtx, async () => {
                return await this.workflow(methReg.registeredFunction, {
                    workflowUUID: workflowStartID,
                    configuredInstance: configuredInst,
                    queueName: wfStatus.queueName,
                    enqueueOptions,
                    executeWorkflow: true,
                    deadlineEpochMS: wfStatus.deadlineEpochMS,
                    isRecoveryDispatch: !!options?.isRecoveryDispatch,
                    isQueueDispatch: !!options?.isQueueDispatch,
                }, ...inputs);
            });
        }
        // Should be temporary workflows. Parse the name of the workflow.
        const wfName = wfStatus.workflowName;
        const nameArr = wfName.split('-');
        if (!nameArr[0].startsWith(DBOSExecutor.#tempWorkflowName)) {
            throw new error_1.DBOSError(`Cannot find workflow function for a non-temporary workflow, ID ${workflowID}, class '${wfStatus.workflowClassName}', function '${wfName}'; did you change your code?`);
        }
        if (nameArr[1] === exports.TempWorkflowType.step) {
            const stepReg = (0, decorators_1.getFunctionRegistrationByName)(wfStatus.workflowClassName, nameArr[2]);
            if (!stepReg?.stepConfig) {
                this.logger.error(`Cannot find step info for ID ${workflowID}, name ${nameArr[2]}`);
                throw new error_1.DBOSNotRegisteredError(nameArr[2]);
            }
            return await (0, context_1.runWithTopContext)(recoverCtx, async () => {
                return await this.startStepTempWF(stepReg.registeredFunction, {
                    workflowUUID: workflowStartID,
                    configuredInstance: configuredInst,
                    queueName: wfStatus.queueName, // Probably null
                    enqueueOptions,
                    executeWorkflow: true,
                    isRecoveryDispatch: !!options?.isRecoveryDispatch,
                    isQueueDispatch: !!options?.isQueueDispatch,
                }, undefined, undefined, ...inputs);
            });
        }
        else if (nameArr[1] === exports.TempWorkflowType.send) {
            // Backwards compatibility: recover send temp workflows created before sendDirect was introduced.
            const swf = async (destinationID, message, topic, serialization) => {
                const ctx = (0, context_1.getCurrentContextStore)();
                // Reserve the function ID synchronously, before any await.
                const functionID = (0, context_1.functionIDGetIncrement)();
                const sermsg = await (0, serialization_1.serializeValue)(message, this.serializer, serialization ?? undefined);
                await this.systemDatabase.send(ctx.workflowId, functionID, destinationID, sermsg.serializedValue, topic, sermsg.serialization);
            };
            const temp_workflow = swf;
            return await (0, context_1.runWithTopContext)(recoverCtx, async () => {
                return this.workflow(temp_workflow, {
                    tempWfName: nameArr[2],
                    tempWfType: exports.TempWorkflowType.send,
                    workflowUUID: workflowStartID,
                    queueName: wfStatus.queueName,
                    enqueueOptions,
                    executeWorkflow: true,
                    isRecoveryDispatch: !!options?.isRecoveryDispatch,
                    isQueueDispatch: !!options?.isQueueDispatch,
                }, ...inputs);
            });
        }
        else {
            this.logger.error(`Unrecognized temporary workflow! UUID ${workflowID}, name ${wfName}`);
            throw new error_1.DBOSNotRegisteredError(wfName);
        }
    }
    async getEventDispatchState(svc, wfn, key) {
        return await this.systemDatabase.getEventDispatchState(svc, wfn, key);
    }
    async upsertEventDispatchState(state) {
        return await this.systemDatabase.upsertEventDispatchState(state);
    }
    #getRecoveryContext(_workflowID, status) {
        // Note: this doesn't inherit the original parent context's span.
        const oc = {};
        oc.request = status.request;
        oc.authenticatedUser = status.authenticatedUser;
        oc.authenticatedRoles = status.authenticatedRoles;
        oc.assumedRole = status.assumedRole;
        oc.serializationType = status.serialization === serialization_1.DBOSPortableJSON.name() ? 'portable' : undefined;
        return oc;
    }
    async getWorkflowSteps(workflowID) {
        const outputs = await this.systemDatabase.getAllOperationResults(workflowID);
        return await Promise.all(outputs.map(async (row) => ({
            function_id: row.function_id,
            function_name: row.function_name ?? '<unknown>',
            child_workflow_id: row.child_workflow_id,
            output: row.output !== null ? await this.serializer.parse(row.output) : null,
            error: row.error !== null ? (0, serialize_error_1.deserializeError)(await this.serializer.parse(row.error)) : null,
        })));
    }
    /**
      An application's version is computed from a hash of the source of its workflows.
      This is guaranteed to be stable given identical source code because it uses an MD5 hash
      and because it iterates through the workflows in sorted order.
      This way, if the app's workflows are updated (which would break recovery), its version changes.
      App version can be manually set through the DBOS__APPVERSION environment variable.
     */
    computeAppVersion() {
        const hasher = crypto.createHash('md5');
        const sortedWorkflowSource = Array.from((0, decorators_1.getAllRegisteredFunctions)())
            .filter((e) => e.workflowConfig)
            .map((i) => i.origFunction.toString())
            .sort();
        // Different DBOS versions should produce different hashes.
        sortedWorkflowSource.push(utils_1.globalParams.dbosVersion);
        for (const sourceCode of sortedWorkflowSource) {
            hasher.update(sourceCode);
        }
        return hasher.digest('hex');
    }
    static internalQueue = undefined;
    static createInternalQueue() {
        if (DBOSExecutor.internalQueue !== undefined) {
            return;
        }
        DBOSExecutor.internalQueue = new wfqueue_1.WorkflowQueue(utils_1.INTERNAL_QUEUE_NAME, {});
    }
}
exports.DBOSExecutor = DBOSExecutor;
//# sourceMappingURL=dbos-executor.js.map