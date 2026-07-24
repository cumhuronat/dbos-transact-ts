"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.getDBOSErrorCode = exports.DBOSInvalidWorkflowInputError = exports.DBOSStepTimeoutError = exports.DBOSAwaitedWorkflowExceededMaxRecoveryAttempts = exports.DBOSInvalidQueuePriorityError = exports.DBOSQueueDuplicatedError = exports.QueueDedupIDDuplicated = exports.DBOSAwaitedWorkflowCancelledError = exports.DBOSUnexpectedStepError = exports.DBOSConflictingRegistrationError = exports.DBOSWorkflowCancelledError = exports.DBOSMaxStepRetriesError = exports.DBOSConflictingWorkflowError = exports.DBOSInvalidWorkflowTransitionError = exports.DBOSExecutorNotInitializedError = exports.DBOSMaxRecoveryAttemptsExceededError = exports.DBOSFailLoadOperationsError = exports.DBOSNonExistentWorkflowError = exports.DBOSConfigKeyTypeError = exports.DBOSNotAuthorizedError = exports.DBOSDataValidationError = exports.DBOSNotRegisteredError = exports.DBOSWorkflowConflictError = exports.DBOSInitializationError = exports.DBOSError = exports.isDataValidationError = void 0;
function isDataValidationError(e) {
    const dbosErrorCode = e?.dbosErrorCode;
    if (!dbosErrorCode)
        return false;
    if (dbosErrorCode === DataValidationError) {
        return true;
    }
    return false;
}
exports.isDataValidationError = isDataValidationError;
class DBOSError extends Error {
    dbosErrorCode;
    // TODO: define a better coding system.
    constructor(msg, dbosErrorCode = 1) {
        super(msg);
        this.dbosErrorCode = dbosErrorCode;
    }
}
exports.DBOSError = DBOSError;
const InitializationError = 3;
class DBOSInitializationError extends DBOSError {
    error;
    constructor(msg, error) {
        super(msg, InitializationError);
        this.error = error;
    }
}
exports.DBOSInitializationError = DBOSInitializationError;
const ConflictingWFIDError = 5;
class DBOSWorkflowConflictError extends DBOSError {
    constructor(workflowID) {
        super(`Conflicting WF ID ${workflowID}`, ConflictingWFIDError);
    }
}
exports.DBOSWorkflowConflictError = DBOSWorkflowConflictError;
const NotRegisteredError = 6;
class DBOSNotRegisteredError extends DBOSError {
    constructor(name, fullmsg) {
        const msg = fullmsg ?? `Operation (Name: ${name}) not registered`;
        super(msg, NotRegisteredError);
    }
}
exports.DBOSNotRegisteredError = DBOSNotRegisteredError;
const DataValidationError = 9;
class DBOSDataValidationError extends DBOSError {
    constructor(msg) {
        super(msg, DataValidationError);
    }
}
exports.DBOSDataValidationError = DBOSDataValidationError;
const NotAuthorizedError = 12;
class DBOSNotAuthorizedError extends DBOSError {
    status;
    constructor(msg, status = 403) {
        super(msg, NotAuthorizedError);
        this.status = status;
    }
}
exports.DBOSNotAuthorizedError = DBOSNotAuthorizedError;
const ConfigKeyTypeError = 14;
class DBOSConfigKeyTypeError extends DBOSError {
    constructor(configKey, expectedType, actualType) {
        super(`${configKey} should be of type ${expectedType}, but got ${actualType}`, ConfigKeyTypeError);
    }
}
exports.DBOSConfigKeyTypeError = DBOSConfigKeyTypeError;
const NonExistentWorkflowError = 16;
class DBOSNonExistentWorkflowError extends DBOSError {
    constructor(msg) {
        super(msg, NonExistentWorkflowError);
    }
}
exports.DBOSNonExistentWorkflowError = DBOSNonExistentWorkflowError;
const FailLoadOperationsError = 17;
class DBOSFailLoadOperationsError extends DBOSError {
    constructor(msg) {
        super(msg, FailLoadOperationsError);
    }
}
exports.DBOSFailLoadOperationsError = DBOSFailLoadOperationsError;
const MaxRecoveryAttemptsExceededError = 18;
class DBOSMaxRecoveryAttemptsExceededError extends DBOSError {
    constructor(workflowID, maxRetries) {
        super(`Workflow ${workflowID} has exceeded its maximum of ${maxRetries} execution or recovery attempts. Further attempts to execute or recover it will fail.`, MaxRecoveryAttemptsExceededError);
    }
}
exports.DBOSMaxRecoveryAttemptsExceededError = DBOSMaxRecoveryAttemptsExceededError;
const ExecutorNotInitializedError = 20;
class DBOSExecutorNotInitializedError extends DBOSError {
    constructor() {
        super('DBOS not initialized', ExecutorNotInitializedError);
    }
}
exports.DBOSExecutorNotInitializedError = DBOSExecutorNotInitializedError;
const InvalidWorkflowTransition = 21;
class DBOSInvalidWorkflowTransitionError extends DBOSError {
    constructor(msg) {
        super(msg ?? 'Invalid workflow state', InvalidWorkflowTransition);
    }
}
exports.DBOSInvalidWorkflowTransitionError = DBOSInvalidWorkflowTransitionError;
const ConflictingWorkflowError = 22;
class DBOSConflictingWorkflowError extends DBOSError {
    constructor(workflowID, msg) {
        super(`Conflicting workflow invocation with the same ID (${workflowID}): ${msg}`, ConflictingWorkflowError);
    }
}
exports.DBOSConflictingWorkflowError = DBOSConflictingWorkflowError;
const MaximumRetriesError = 23;
class DBOSMaxStepRetriesError extends DBOSError {
    errors;
    constructor(stepName, maxRetries, errors) {
        const formattedErrors = errors.map((error, index) => `Error ${index + 1}: ${error.message}`).join('. ');
        super(`Step ${stepName} has exceeded its maximum of ${maxRetries} retries. Previous errors: ${formattedErrors}`, MaximumRetriesError);
        this.errors = errors;
    }
}
exports.DBOSMaxStepRetriesError = DBOSMaxStepRetriesError;
const WorkFlowCancelled = 24;
class DBOSWorkflowCancelledError extends DBOSError {
    workflowID;
    constructor(workflowID) {
        super(`Workflow ${workflowID} has been cancelled`, WorkFlowCancelled);
        this.workflowID = workflowID;
    }
}
exports.DBOSWorkflowCancelledError = DBOSWorkflowCancelledError;
const ConflictingRegistrationError = 25;
class DBOSConflictingRegistrationError extends DBOSError {
    constructor(msg) {
        super(msg, ConflictingRegistrationError);
    }
}
exports.DBOSConflictingRegistrationError = DBOSConflictingRegistrationError;
const UnexpectedStep = 26;
/** Exception raised when a step has an unexpected recorded name, indicating a determinism problem. */
class DBOSUnexpectedStepError extends DBOSError {
    workflowID;
    stepID;
    expectedName;
    constructor(workflowID, stepID, expectedName, recordedName) {
        super(recordedName.startsWith('DBOS.patch')
            ? `During execution of workflow ${workflowID} step ${stepID}, function ${recordedName} was recorded when ${expectedName} was expected.\n
          Check that your patches are backward compatible, that you do not have older code trying to recover workflows with newer patches, and that your workflow is deterministic.`
            : `During execution of workflow ${workflowID} step ${stepID}, function ${recordedName} was recorded when ${expectedName} was expected. Check that your workflow is deterministic.`, UnexpectedStep);
        this.workflowID = workflowID;
        this.stepID = stepID;
        this.expectedName = expectedName;
    }
}
exports.DBOSUnexpectedStepError = DBOSUnexpectedStepError;
const TargetWorkflowCancelled = 27;
class DBOSAwaitedWorkflowCancelledError extends DBOSError {
    workflowID;
    constructor(workflowID) {
        super(`Awaited ${workflowID} was cancelled`, TargetWorkflowCancelled);
        this.workflowID = workflowID;
    }
}
exports.DBOSAwaitedWorkflowCancelledError = DBOSAwaitedWorkflowCancelledError;
exports.QueueDedupIDDuplicated = 28;
/** Exception raised when workflow with same dedupid is queued*/
class DBOSQueueDuplicatedError extends DBOSError {
    workflowID;
    queue;
    deduplicationID;
    constructor(workflowID, queue, deduplicationID) {
        super(`Workflow ${workflowID} was deduplicated due to an existing workflow in queue ${queue} with deduplication ID ${deduplicationID}.`, exports.QueueDedupIDDuplicated);
        this.workflowID = workflowID;
        this.queue = queue;
        this.deduplicationID = deduplicationID;
        // Portable error serialization records err.name, which is otherwise 'Error'; the debouncer matches on it after replay.
        this.name = 'DBOSQueueDuplicatedError';
    }
}
exports.DBOSQueueDuplicatedError = DBOSQueueDuplicatedError;
const InvalidQueuePriority = 29;
/** Exception raised queue priority is invalid */
class DBOSInvalidQueuePriorityError extends DBOSError {
    priority;
    min;
    max;
    constructor(priority, min, max) {
        super(`Invalid priority ${priority}. Priority must be between ${min} and ${max}.`, InvalidQueuePriority);
        this.priority = priority;
        this.min = min;
        this.max = max;
    }
}
exports.DBOSInvalidQueuePriorityError = DBOSInvalidQueuePriorityError;
const AwaitedWorkflowExceededMaxRecoveryAttempts = 30;
class DBOSAwaitedWorkflowExceededMaxRecoveryAttempts extends DBOSError {
    workflowID;
    constructor(workflowID) {
        super(`Awaited ${workflowID} exceeded its maximum recovery attempts`, AwaitedWorkflowExceededMaxRecoveryAttempts);
        this.workflowID = workflowID;
    }
}
exports.DBOSAwaitedWorkflowExceededMaxRecoveryAttempts = DBOSAwaitedWorkflowExceededMaxRecoveryAttempts;
const StepTimeout = 31;
/** Exception raised when a single attempt of a step exceeds its configured `timeoutMS` */
class DBOSStepTimeoutError extends DBOSError {
    stepName;
    timeoutMS;
    constructor(stepName, timeoutMS) {
        super(`Step ${stepName} timed out after ${timeoutMS}ms`, StepTimeout);
        this.stepName = stepName;
        this.timeoutMS = timeoutMS;
    }
}
exports.DBOSStepTimeoutError = DBOSStepTimeoutError;
const InvalidWorkflowInput = 32;
/** Exception raised when a workflow's arguments cannot be serialized. Blames the arguments alone. */
class DBOSInvalidWorkflowInputError extends DBOSError {
    constructor(workflowName, cause) {
        super(`Could not serialize the arguments to workflow ${workflowName}: ${cause instanceof Error ? cause.message : String(cause)}`, InvalidWorkflowInput);
    }
}
exports.DBOSInvalidWorkflowInputError = DBOSInvalidWorkflowInputError;
function getDBOSErrorCode(e) {
    if (e && typeof e === 'object' && 'dbosErrorCode' in e) {
        const code = e.dbosErrorCode;
        return typeof code === 'number' ? code : undefined;
    }
    return undefined;
}
exports.getDBOSErrorCode = getDBOSErrorCode;
//# sourceMappingURL=error.js.map