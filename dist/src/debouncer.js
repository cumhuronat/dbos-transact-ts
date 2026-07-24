"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.DebouncerClient = exports.Debouncer = void 0;
const _1 = require(".");
const dbos_1 = require("./dbos");
const dbos_executor_1 = require("./dbos-executor");
const context_1 = require("./context");
const decorators_1 = require("./decorators");
const error_1 = require("./error");
const serialization_1 = require("./serialization");
const utils_1 = require("./utils");
const system_db_schema_1 = require("../schemas/system_db_schema");
/**
 * True if `e` is a queue-deduplication error, including the portable-serialization
 * replay form, which carries only the original type name.
 */
function isQueueDeduplicatedError(e) {
    if (e instanceof Error && (0, error_1.getDBOSErrorCode)(e) === error_1.QueueDedupIDDuplicated) {
        return true;
    }
    return e instanceof system_db_schema_1.PortableWorkflowError && e.name === error_1.DBOSQueueDuplicatedError.name;
}
/**
 * A debounce owns the workflow's deduplication ID (the debounce key) and its delay
 * (the debounce period), so a caller must not also set them. Priority and partition
 * keys are rejected because they cannot apply to a debounced enqueue.
 */
function rejectConflictingOptions(params) {
    const enqueueOptions = params?.enqueueOptions;
    if (enqueueOptions?.deduplicationID !== undefined) {
        throw new error_1.DBOSError('Cannot debounce a workflow with a deduplicationID set: the debounce key is used as the workflow deduplication ID.');
    }
    if (enqueueOptions?.delaySeconds !== undefined) {
        throw new error_1.DBOSError('Cannot debounce a workflow with a delay set: the debounce period controls the delay.');
    }
    if (enqueueOptions?.priority !== undefined) {
        throw new error_1.DBOSError('Cannot debounce a workflow with a priority set: priority is not supported for debounced workflows.');
    }
    if (enqueueOptions?.queuePartitionKey !== undefined) {
        throw new error_1.DBOSError('Cannot debounce a workflow with a queue partition key set: partitioned queues do not support deduplication, which debouncing requires.');
    }
    if (params?.duplicationPolicy === 'return-existing') {
        throw new error_1.DBOSError("Cannot debounce a workflow with duplicationPolicy 'return-existing': a debounce owns the deduplication behavior.");
    }
}
/**
 * Decide what a debounce caller should do after a bounce attempt.
 * - 'return': an existing debounced workflow was extended; return a handle to it.
 * - 'enqueue': the key is unheld; enqueue a fresh debounced workflow.
 * - 'raise': the key is held by a non-debounced workflow or by a different workflow
 *   whose debounce key collides; surface the deduplication conflict.
 * - 'retry': a same-name debounced holder flipped out of DELAYED mid-bounce (a rare race); retry.
 */
function classifyBounce(result, workflowName, workflowClassName) {
    if (result.bouncedWorkflowID !== null) {
        return 'return';
    }
    if (result.holderWorkflowID === null) {
        return 'enqueue';
    }
    if (!result.holderIsDebounced) {
        return 'raise';
    }
    if (result.holderWorkflowName !== workflowName || (result.holderWorkflowClassName ?? '') !== workflowClassName) {
        return 'raise';
    }
    return 'retry';
}
class Debouncer {
    cfg;
    constructor(params) {
        const wInfo = (0, decorators_1.getFunctionRegistration)(params.workflow);
        this.cfg = {
            workflowName: wInfo?.name ?? params.workflow.name,
            workflowClassName: (0, decorators_1.getRegisteredFunctionClassName)(params.workflow),
            startWorkflowParams: params.startWorkflowParams,
            debounceTimeoutMs: params.debounceTimeoutMs,
        };
    }
    async debounce(debounceKey, debouncePeriodMs, ...args) {
        if (debouncePeriodMs <= 0) {
            throw Error(`debouncePeriodMs must be positive, not ${debouncePeriodMs}`);
        }
        (0, decorators_1.ensureDBOSIsLaunched)('debounce');
        rejectConflictingOptions(this.cfg.startWorkflowParams);
        const exec = dbos_executor_1.DBOSExecutor.globalInstance;
        const queueName = this.cfg.startWorkflowParams?.queueName ?? utils_1.INTERNAL_QUEUE_NAME;
        const deduplicationID = `${this.cfg.workflowClassName}.${this.cfg.workflowName}-${debounceKey}`;
        // Capture a pinned workflow ID once: it is re-applied on every enqueue attempt (a lost
        // dedup race consumes it before throwing) and goes unused when a bounce coalesces.
        const pinnedWorkflowID = (0, context_1.getNextWFID)(this.cfg.startWorkflowParams?.workflowID);
        // Resolve the workflow function so bounced inputs serialize identically to enqueued ones.
        const methReg = (0, decorators_1.getFunctionRegistrationByName)(this.cfg.workflowClassName, this.cfg.workflowName);
        if (!methReg || !methReg.registeredFunction) {
            throw new error_1.DBOSError(`Invalid workflow name provided to debouncer: ${this.cfg.workflowName}`);
        }
        const func = methReg.registeredFunction;
        const serializationType = methReg.workflowConfig?.serialization;
        while (true) {
            // Try to extend an existing debounced workflow for this key first (the sole coalescing mechanism).
            // In a workflow, the bounce commits atomically with its step checkpoint so a crash can never
            // commit one without the other, which on recovery would re-bounce work that already ran.
            const funcArgs = await (0, serialization_1.serializeFunctionInputOutput)(serializationType === 'portable' ? { positionalArgs: args } : args, [this.cfg.workflowName, '<arguments>'], exec.serializer, serializationType);
            const bounceParams = {
                workflowName: this.cfg.workflowName,
                workflowClassName: this.cfg.workflowClassName,
                queueName,
                deduplicationID,
                delayUntilEpochMS: Date.now() + debouncePeriodMs,
                input: funcArgs.stringified,
                serialization: funcArgs.sername,
            };
            const result = await (0, dbos_1.runTransactionalInternalStep)((client) => exec.systemDatabase.debounceDelayedWorkflow(bounceParams, client), 'DBOS.debounceDelayedWorkflow');
            const action = classifyBounce(result, this.cfg.workflowName, this.cfg.workflowClassName);
            if (action === 'return') {
                return _1.DBOS.retrieveWorkflow(result.bouncedWorkflowID);
            }
            if (action === 'raise') {
                throw new error_1.DBOSQueueDuplicatedError(pinnedWorkflowID ?? '', queueName, deduplicationID);
            }
            if (action === 'retry') {
                continue;
            }
            // action === 'enqueue': the key is free, create a fresh debounced workflow.
            const debounceDeadlineEpochMS = this.cfg.debounceTimeoutMs ? Date.now() + this.cfg.debounceTimeoutMs : undefined;
            try {
                // A null timeout detaches any propagated workflow deadline: a debounce delay can be long,
                // so an inherited absolute deadline could expire before the debounced workflow ever runs.
                const handle = await _1.DBOS.startWorkflow(func, {
                    workflowID: pinnedWorkflowID,
                    queueName,
                    timeoutMS: this.cfg.startWorkflowParams?.timeoutMS ?? null,
                    workflowAttributes: this.cfg.startWorkflowParams?.workflowAttributes,
                    enqueueOptions: {
                        applicationVersion: this.cfg.startWorkflowParams?.enqueueOptions?.applicationVersion,
                        deduplicationID,
                        delaySeconds: debouncePeriodMs / 1000,
                        debounceDeadlineEpochMS,
                        isDebounced: true,
                    },
                })(...args);
                return handle;
            }
            catch (e) {
                // A concurrent debounce grabbed the key between bounce and enqueue; loop to bounce that workflow instead.
                if (!isQueueDeduplicatedError(e)) {
                    throw e;
                }
                continue;
            }
        }
    }
}
exports.Debouncer = Debouncer;
class DebouncerClient {
    client;
    cfg;
    serializationType;
    constructor(client, params) {
        this.client = client;
        this.cfg = {
            workflowName: params.workflowName,
            workflowClassName: params.workflowClassName || '',
            startWorkflowParams: params.startWorkflowParams,
            debounceTimeoutMs: params.debounceTimeoutMs,
        };
        this.serializationType = params.serializationType;
    }
    async debounce(debounceKey, debouncePeriodMs, ...args) {
        if (debouncePeriodMs <= 0) {
            throw Error(`debouncePeriodMs must be positive, not ${debouncePeriodMs}`);
        }
        rejectConflictingOptions(this.cfg.startWorkflowParams);
        const queueName = this.cfg.startWorkflowParams?.queueName ?? utils_1.INTERNAL_QUEUE_NAME;
        const deduplicationID = `${this.cfg.workflowClassName}.${this.cfg.workflowName}-${debounceKey}`;
        while (true) {
            // Try to extend an existing debounced workflow for this key first (the sole coalescing mechanism).
            const serparam = await (0, serialization_1.serializeArgs)(args, undefined, this.client.serializer, this.serializationType);
            const result = await this.client.debounceDelayedWorkflow({
                workflowName: this.cfg.workflowName,
                workflowClassName: this.cfg.workflowClassName,
                queueName,
                deduplicationID,
                delayUntilEpochMS: Date.now() + debouncePeriodMs,
                input: serparam.serializedValue,
                serialization: serparam.serialization,
            });
            const action = classifyBounce(result, this.cfg.workflowName, this.cfg.workflowClassName);
            if (action === 'return') {
                return this.client.retrieveWorkflow(result.bouncedWorkflowID);
            }
            if (action === 'raise') {
                throw new error_1.DBOSQueueDuplicatedError(this.cfg.startWorkflowParams?.workflowID ?? '', queueName, deduplicationID);
            }
            if (action === 'retry') {
                continue;
            }
            // action === 'enqueue': the key is free, create a fresh debounced workflow.
            const debounceDeadlineEpochMS = this.cfg.debounceTimeoutMs ? Date.now() + this.cfg.debounceTimeoutMs : undefined;
            try {
                const workflowID = await this.client.enqueueDebounced({
                    workflowName: this.cfg.workflowName,
                    workflowClassName: this.cfg.workflowClassName || undefined,
                    queueName,
                    workflowID: this.cfg.startWorkflowParams?.workflowID,
                    workflowTimeoutMS: this.cfg.startWorkflowParams?.timeoutMS ?? undefined,
                    appVersion: this.cfg.startWorkflowParams?.enqueueOptions?.applicationVersion,
                    attributes: this.cfg.startWorkflowParams?.workflowAttributes,
                    deduplicationID,
                    delaySeconds: debouncePeriodMs / 1000,
                    serializationType: this.serializationType,
                }, debounceDeadlineEpochMS, args);
                return this.client.retrieveWorkflow(workflowID);
            }
            catch (e) {
                // A concurrent debounce grabbed the key between bounce and enqueue; loop to bounce that workflow instead.
                if (!isQueueDeduplicatedError(e)) {
                    throw e;
                }
                continue;
            }
        }
    }
}
exports.DebouncerClient = DebouncerClient;
//# sourceMappingURL=debouncer.js.map