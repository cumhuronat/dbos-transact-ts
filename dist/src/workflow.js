"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.RetrievedHandle = exports.InvokedHandle = exports.isWorkflowActive = exports.StatusString = exports.DEFAULT_MAX_RECOVERY_ATTEMPTS = exports.validateWorkflowAttributes = void 0;
const serialization_1 = require("./serialization");
const dbos_1 = require("./dbos");
const dbos_executor_1 = require("./dbos-executor");
const error_1 = require("./error");
/**
 * Validate that custom workflow attributes, if provided, are a plain key-value object that
 * can be serialized to JSON for storage in the JSONB `attributes` column. A key-value object
 * is required because attributes are queried with the `@>` containment filter; scalars and
 * arrays would store but never match meaningfully. Called at the workflow-creation entry
 * points (`startWorkflow`/enqueue) so invalid input fails fast at the call site rather than
 * surfacing as a database error at insert time.
 */
function validateWorkflowAttributes(attributes) {
    if (attributes === undefined || attributes === null) {
        return;
    }
    if (typeof attributes !== 'object' || Array.isArray(attributes)) {
        throw new error_1.DBOSError(`Invalid workflow attributes: must be a key-value object, got ${Array.isArray(attributes) ? 'array' : typeof attributes}.`);
    }
    let serialized;
    try {
        serialized = JSON.stringify(attributes);
    }
    catch (e) {
        throw new error_1.DBOSError(`Invalid workflow attributes: must be JSON-serializable. ${e.message}`);
    }
    // JSON.stringify silently drops keys whose values are functions or undefined, and returns
    // "{}" for objects with no serializable keys (e.g. a class instance). Reject these rather
    // than store an empty object that does not reflect what the caller passed.
    if (serialized === '{}' && Object.keys(attributes).length > 0) {
        throw new error_1.DBOSError('Invalid workflow attributes: object has no JSON-serializable properties.');
    }
}
exports.validateWorkflowAttributes = validateWorkflowAttributes;
exports.DEFAULT_MAX_RECOVERY_ATTEMPTS = 100;
/** Enumeration of values for workflow status */
exports.StatusString = {
    /** Workflow has may be running */
    PENDING: 'PENDING',
    /** Workflow complete with return value */
    SUCCESS: 'SUCCESS',
    /** Workflow complete with error thrown */
    ERROR: 'ERROR',
    /** Workflow has exceeded its maximum number of execution or recovery attempts */
    MAX_RECOVERY_ATTEMPTS_EXCEEDED: 'MAX_RECOVERY_ATTEMPTS_EXCEEDED',
    /** Workflow is being, or has been, cancelled */
    CANCELLED: 'CANCELLED',
    /** Workflow is on a `WorkflowQueue` and has not yet started */
    ENQUEUED: 'ENQUEUED',
    /** Workflow is on a `WorkflowQueue` waiting for a delay to expire before it can start */
    DELAYED: 'DELAYED',
};
function isWorkflowActive(status) {
    return status === exports.StatusString.PENDING || status === exports.StatusString.ENQUEUED || status === exports.StatusString.DELAYED;
}
exports.isWorkflowActive = isWorkflowActive;
/**
 * The handle returned when invoking a workflow with DBOSExecutor.workflow
 */
class InvokedHandle {
    systemDatabase;
    workflowPromise;
    workflowUUID;
    workflowName;
    constructor(systemDatabase, workflowPromise, workflowUUID, workflowName) {
        this.systemDatabase = systemDatabase;
        this.workflowPromise = workflowPromise;
        this.workflowUUID = workflowUUID;
        this.workflowName = workflowName;
    }
    getWorkflowUUID() {
        return this.workflowUUID;
    }
    get workflowID() {
        return this.workflowUUID;
    }
    async getStatus() {
        return await dbos_1.DBOS.getWorkflowStatus(this.workflowUUID);
    }
    async getResult(optionsOrFuncIdForGet) {
        const funcIdForGet = typeof optionsOrFuncIdForGet === 'number' ? optionsOrFuncIdForGet : undefined;
        (0, dbos_1.resolvePollingIntervalMs)(optionsOrFuncIdForGet);
        return await (0, dbos_1.runInternalStep)(async () => {
            return await this.workflowPromise;
        }, 'DBOS.getResult', this.workflowUUID, funcIdForGet);
    }
    async getWorkflowInputs() {
        const status = (await this.systemDatabase.getWorkflowStatus(this.workflowUUID));
        return (await (0, serialization_1.deserializePositionalArgs)(status.input, status.serialization, this.systemDatabase.getSerializer()));
    }
}
exports.InvokedHandle = InvokedHandle;
/**
 * The handle returned when retrieving a workflow with DBOSExecutor.retrieve
 */
class RetrievedHandle {
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
        return await dbos_1.DBOS.getWorkflowStatus(this.workflowUUID);
    }
    async getResult(optionsOrFuncIdForGet) {
        const funcIdForGet = typeof optionsOrFuncIdForGet === 'number' ? optionsOrFuncIdForGet : undefined;
        const pollingIntervalMs = (0, dbos_1.resolvePollingIntervalMs)(optionsOrFuncIdForGet);
        return (await dbos_1.DBOS.getResultInternal(this.workflowUUID, undefined, undefined, funcIdForGet, pollingIntervalMs));
    }
    async getWorkflowInputs() {
        const status = (await this.systemDatabase.getWorkflowStatus(this.workflowUUID));
        return (await (0, serialization_1.deserializePositionalArgs)(status.input, status.serialization, this.systemDatabase.getSerializer()));
    }
}
exports.RetrievedHandle = RetrievedHandle;
(0, serialization_1.registerSerializationRecipe)({
    name: 'DBOS.WorkflowHandle',
    isApplicable: (v) => {
        return v instanceof RetrievedHandle || v instanceof InvokedHandle;
    },
    serialize: (v) => {
        return { wfid: v.workflowID };
    },
    deserialize: (s) => new RetrievedHandle(dbos_executor_1.DBOSExecutor.globalInstance.systemDatabase, s.wfid),
});
//# sourceMappingURL=workflow.js.map