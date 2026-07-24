export declare function isDataValidationError(e: Error): boolean;
export declare class DBOSError extends Error {
    readonly dbosErrorCode: number;
    constructor(msg: string, dbosErrorCode?: number);
}
export declare class DBOSInitializationError extends DBOSError {
    readonly error?: Error | undefined;
    constructor(msg: string, error?: Error | undefined);
}
export declare class DBOSWorkflowConflictError extends DBOSError {
    constructor(workflowID: string);
}
export declare class DBOSNotRegisteredError extends DBOSError {
    constructor(name: string, fullmsg?: string);
}
export declare class DBOSDataValidationError extends DBOSError {
    constructor(msg: string);
}
export declare class DBOSNotAuthorizedError extends DBOSError {
    readonly status: number;
    constructor(msg: string, status?: number);
}
export declare class DBOSConfigKeyTypeError extends DBOSError {
    constructor(configKey: string, expectedType: string, actualType: string);
}
export declare class DBOSNonExistentWorkflowError extends DBOSError {
    constructor(msg: string);
}
export declare class DBOSFailLoadOperationsError extends DBOSError {
    constructor(msg: string);
}
export declare class DBOSMaxRecoveryAttemptsExceededError extends DBOSError {
    constructor(workflowID: string, maxRetries: number);
}
export declare class DBOSExecutorNotInitializedError extends DBOSError {
    constructor();
}
export declare class DBOSInvalidWorkflowTransitionError extends DBOSError {
    constructor(msg?: string);
}
export declare class DBOSConflictingWorkflowError extends DBOSError {
    constructor(workflowID: string, msg: string);
}
export declare class DBOSMaxStepRetriesError extends DBOSError {
    readonly errors: Error[];
    constructor(stepName: string, maxRetries: number, errors: Error[]);
}
export declare class DBOSWorkflowCancelledError extends DBOSError {
    readonly workflowID: string;
    constructor(workflowID: string);
}
export declare class DBOSConflictingRegistrationError extends DBOSError {
    constructor(msg: string);
}
/** Exception raised when a step has an unexpected recorded name, indicating a determinism problem. */
export declare class DBOSUnexpectedStepError extends DBOSError {
    readonly workflowID: string;
    readonly stepID: number;
    readonly expectedName: string;
    constructor(workflowID: string, stepID: number, expectedName: string, recordedName: string);
}
export declare class DBOSAwaitedWorkflowCancelledError extends DBOSError {
    readonly workflowID: string;
    constructor(workflowID: string);
}
export declare const QueueDedupIDDuplicated = 28;
/** Exception raised when workflow with same dedupid is queued*/
export declare class DBOSQueueDuplicatedError extends DBOSError {
    readonly workflowID: string;
    readonly queue: string;
    readonly deduplicationID: string;
    constructor(workflowID: string, queue: string, deduplicationID: string);
}
/** Exception raised queue priority is invalid */
export declare class DBOSInvalidQueuePriorityError extends DBOSError {
    readonly priority: number;
    readonly min: number;
    readonly max: number;
    constructor(priority: number, min: number, max: number);
}
export declare class DBOSAwaitedWorkflowExceededMaxRecoveryAttempts extends DBOSError {
    readonly workflowID: string;
    constructor(workflowID: string);
}
/** Exception raised when a single attempt of a step exceeds its configured `timeoutMS` */
export declare class DBOSStepTimeoutError extends DBOSError {
    readonly stepName: string;
    readonly timeoutMS: number;
    constructor(stepName: string, timeoutMS: number);
}
/** Exception raised when a workflow's arguments cannot be serialized. Blames the arguments alone. */
export declare class DBOSInvalidWorkflowInputError extends DBOSError {
    constructor(workflowName: string, cause: unknown);
}
export declare function getDBOSErrorCode(e: Error): number | undefined;
//# sourceMappingURL=error.d.ts.map