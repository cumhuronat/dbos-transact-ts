/// <reference types="node" />
import { HTTPRequest, StepStatus, DBOSContextOptions } from './context';
import { DBOSConfig, DBOSExecutor, DBOSExternalState } from './dbos-executor';
import { DBOSSpan, Tracer } from './telemetry/traces';
import { GetWorkflowsInput, StepInfo, ListWorkflowStepsOptions, WorkflowConfig, WorkflowHandle, WorkflowSerializationFormat, WorkflowStatus } from './workflow';
import { DLogger } from './telemetry/logs';
import { ScheduledArgs, SchedulerConfig } from './scheduler/scheduler_decorator';
import { AlertHandler, ExternalRegistration, MethodRegistration, FunctionName, ConfiguredInstance, DBOSMethodMiddlewareInstaller, DBOSLifecycleCallback } from './decorators';
import { JSONValue, SerializationRecipe } from './serialization';
import { Server } from 'http';
import { StepConfig } from './step';
import { Conductor } from './conductor/conductor';
import { DuplicationPolicy, EnqueueOptions, VersionInfo } from './system_database';
import { PoolClient } from 'pg';
import { WorkflowSchedule, ScheduledWorkflowFn, ScheduleOptions } from './scheduler/scheduler';
import { RegisterQueueOptions, WorkflowQueue } from './wfqueue';
type AnyConstructor = new (...args: unknown[]) => object;
export interface DBOSLaunchOptions {
    conductorURL?: string;
    conductorKey?: string;
    conductorExecutorMetadata?: Record<string, unknown>;
}
type PossiblyWFFunc = (...args: any[]) => Promise<unknown>;
type InvokeFunctionsAsync<T> = T extends Function ? {
    [P in keyof T]: T[P] extends PossiblyWFFunc ? (...args: Parameters<T[P]>) => Promise<WorkflowHandle<Awaited<ReturnType<T[P]>>>> : never;
} : never;
type InvokeFunctionsAsyncInst<T> = T extends ConfiguredInstance ? {
    [P in keyof T]: T[P] extends PossiblyWFFunc ? (...args: Parameters<T[P]>) => Promise<WorkflowHandle<Awaited<ReturnType<T[P]>>>> : never;
} : never;
export interface StartWorkflowParams {
    workflowID?: string;
    queueName?: string;
    timeoutMS?: number | null;
    enqueueOptions?: EnqueueOptions;
    duplicationPolicy?: DuplicationPolicy;
    workflowAttributes?: Record<string, unknown>;
}
/**
 * Options for `DBOS.send`
 */
export interface SendOptions {
    /** Serialization format override, allows cross-language send/recv */
    serializationType?: WorkflowSerializationFormat;
}
/**
 * Options for `DBOS.writeStream`
 */
export interface WriteStreamOptions {
    /** Serialization format override, allows cross-language writeStream / readStream */
    serializationType?: WorkflowSerializationFormat;
}
/**
 * Options for DBOS operations that poll the system database while waiting.
 */
export interface PollingOptions {
    /**
     * Milliseconds to wait between system database polls. Must be positive and finite.
     *
     * This affects DB-backed polling waits. Live in-process workflow handles that await
     * an already-running promise do not poll.
     */
    pollingIntervalMs?: number;
}
/**
 * Options for `DBOS.getResult` and workflow handle `getResult`.
 */
export interface GetResultOptions extends PollingOptions {
    /** Timeout in seconds; if the workflow does not complete before the timeout, `null` will be returned */
    timeoutSeconds?: number;
    /** Absolute deadline as a UNIX epoch timestamp in milliseconds; if the workflow does not complete before this time, `null` will be returned */
    deadlineEpochMS?: number;
}
/**
 * Options for `DBOS.waitFirst`.
 */
export type WaitFirstOptions = PollingOptions;
/**
 * Options for `DBOS.waitAll`.
 */
export type WaitAllOptions = PollingOptions;
/**
 * Options for `DBOS.recv`
 */
export interface RecvOptions extends PollingOptions {
    /** Timeout in seconds; if no message is received before the timeout (default 60 seconds), `null` will be returned */
    timeoutSeconds?: number;
    /** Absolute deadline as a UNIX epoch timestamp in milliseconds; if no message is received before this time, `null` will be returned */
    deadlineEpochMS?: number;
}
/**
 * Options for `DBOS.getEvent`
 */
export interface GetEventOptions extends PollingOptions {
    /** Timeout in seconds; if no event is received before the timeout (default 60 seconds), `null` will be returned */
    timeoutSeconds?: number;
    /** Absolute deadline as a UNIX epoch timestamp in milliseconds; if no event is received before this time, `null` will be returned */
    deadlineEpochMS?: number;
}
/**
 * Options for `DBOS.setWorkflowDelay`
 */
export interface SetWorkflowDelayOptions {
    /** Number of seconds to delay the workflow from now */
    delaySeconds?: number;
    /** Absolute deadline as a UNIX epoch timestamp in milliseconds; the workflow will remain DELAYED until this time */
    delayUntilEpochMS?: number;
}
/**
 * Options for `DBOS.cancelWorkflow` and `DBOS.cancelWorkflows`
 */
export interface CancelWorkflowsOptions {
    /** If true, also cancel all descendant (child) workflows recursively (default: false) */
    cancelChildren?: boolean;
}
/**
 * Options for `DBOS.setEvent`
 */
export interface SetEventOptions {
    /**
     * Serialization format override, allows event to be read by
     *   workflows or clients written in other languages
     */
    serializationType?: WorkflowSerializationFormat;
}
export declare function resolveTimeoutSeconds(options?: number | RecvOptions | GetEventOptions | GetResultOptions): number | undefined;
export declare function resolvePollingIntervalMs(options?: number | PollingOptions): number | undefined;
export declare function resolveDelayEpochMS(options: number | SetWorkflowDelayOptions): number;
export declare function getExecutor(): DBOSExecutor;
export declare function runInternalStep<T>(callback: () => Promise<T>, funcName: string, childWFID?: string, assignedFuncID?: number): Promise<T>;
/**
 * Like runInternalStep, but when called from within a workflow, the callback and the step
 * result recording run in the same database transaction (via runTransactionalStep).
 * The callback receives a PoolClient that should be passed to any SystemDatabase
 * methods so they participate in the same transaction.
 * Outside a workflow, the callback is called directly with `undefined` as the client.
 */
export declare function runTransactionalInternalStep<T>(callback: (client: PoolClient | undefined) => Promise<T>, funcName: string): Promise<T>;
export declare class DBOS {
    #private;
    static adminServer: Server | undefined;
    static conductor: Conductor | undefined;
    /**
     * Set configuration of `DBOS` prior to `launch`
     * @param config - configuration of services needed by DBOS
     */
    static setConfig(config: DBOSConfig): void;
    /**
     * Check if DBOS has been `launch`ed (and not `shutdown`)
     * @returns `true` if DBOS has been launched, or `false` otherwise
     */
    static isInitialized(): boolean;
    /**
     * Launch DBOS, starting recovery and request handling
     * @param options - Launch options for connecting to DBOS Conductor
     */
    static launch(options?: DBOSLaunchOptions): Promise<void>;
    /**
     * Logs all workflows that can be invoked externally, rather than directly by the applicaton.
     * This includes:
     *   All DBOS event receiver entrypoints (message queues, URLs, etc.)
     *   Scheduled workflows
     *   Queues
     */
    static logRegisteredEndpoints(): void;
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
    static shutdown(options?: {
        deregister?: boolean;
    }): Promise<void>;
    /**
     * Clear the DBOS workflow, queue, instance, data source, listener, and other registries.
     *   This is available for testing and development purposes only.
     *   This may only be done while DBOS is not launch()ed.
     *   Decorated / registered functions created prior to `clearRegistry` may no longer be used.
     *     Fresh wrappers may be created from the original functions.
     */
    private static clearRegistry;
    /** Stop listening for external events (for testing) */
    static deactivateEventReceivers(): Promise<void | undefined>;
    /** Start listening for external events (for testing) */
    static initEventReceivers(): Promise<void | undefined>;
    /** Get the current DBOS Logger, appropriate to the current context */
    static get logger(): DLogger;
    /** Get the current DBOS Tracer, for starting spans */
    static get tracer(): Tracer | undefined;
    /** Get the current DBOS tracing span, appropriate to the current context */
    static get span(): DBOSSpan | undefined;
    /**
     * Get the current request object (such as an HTTP request)
     * This is intended for use in event libraries that know the type of the current request,
     *  and set it using `withTracedContext` or `runWithContext`
     */
    static requestObject(): object | undefined;
    /** Get the current HTTP request (within `@DBOS.getApi` et al) */
    static getRequest(): HTTPRequest | undefined;
    /** Get the current HTTP request (within `@DBOS.getApi` et al) */
    static get request(): HTTPRequest;
    /** Get the current application version */
    static get applicationVersion(): string;
    /** Get the current executor ID */
    static get executorID(): string;
    /** Get the current workflow ID */
    static get workflowID(): string | undefined;
    /** Use portable serialization by default? */
    static get defaultSerializationType(): WorkflowSerializationFormat | undefined;
    /** Get the current step number, within the current workflow */
    static get stepID(): number | undefined;
    static get stepStatus(): StepStatus | undefined;
    /** Get the current authenticated user */
    static get authenticatedUser(): string;
    /** Get the roles granted to the current authenticated user */
    static get authenticatedRoles(): string[];
    /** Get the role assumed by the current user giving authorization to execute the current function */
    static get assumedRole(): string;
    /** @returns true if called from within a transaction, false otherwise */
    static isInTransaction(): boolean;
    /** @returns true if called from within a step, false otherwise */
    static isInStep(): boolean;
    /**
     * @returns true if called from within a workflow
     *  (regardless of whether the workflow is currently executing a step
     *   or transaction), false otherwise
     */
    static isWithinWorkflow(): boolean;
    /**
     * @returns true if called from within a workflow that is not currently executing
     *  a step or transaction, or false otherwise
     */
    static isInWorkflow(): boolean;
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
    static getEventDispatchState(svc: string, wfn: string, key: string): Promise<DBOSExternalState | undefined>;
    /**
     * Set a state item into the system database, which provides a key/value store interface for event dispatchers.
     *   The full key for the database state should include the service, function, and item; these fields are part of `state`.
     *   Values are versioned.  A version can either be a sequence number (long integer), or a time (high precision floating point).
     *     If versions are in use, any upsert is discarded if the version field is less than what is already stored.
     *
     * @param state - the service, workflow, item, version, and value to write to the database
     * @returns The upsert returns the current record, which may be useful if it is more recent than the `state` provided.
     */
    static upsertEventDispatchState(state: DBOSExternalState): Promise<DBOSExternalState>;
    /**
     * Get the workflow status given a workflow ID
     * @param workflowID - ID of the workflow
     * @returns status of the workflow as `WorkflowStatus`, or `null` if there is no workflow with `workflowID`
     */
    static getWorkflowStatus(workflowID: string): Promise<WorkflowStatus | null>;
    /**
     * Get the workflow result, given a workflow ID
     * @param workflowID - ID of the workflow
     * @param timeoutSeconds - Maximum time to wait for result; if not provided, the operation does not time out
     * @returns The return value of the workflow, or throws the exception thrown by the workflow, or `null` if times out
     */
    static getResult<T>(workflowID: string, options?: number | GetResultOptions): Promise<T | null>;
    static getResultInternal<T>(workflowID: string, timeoutSeconds?: number, timerFuncID?: number, assignedFuncID?: number, pollingIntervalMs?: number): Promise<T | null>;
    /**
     * Wait for any one of the given workflow handles to complete and return it.
     * Polls the database until at least one workflow's status is no longer PENDING, ENQUEUED, or DELAYED,
     * then returns the corresponding handle.
     * @param handles - Non-empty array of workflow handles to wait on
     * @returns The first handle whose workflow has completed
     */
    static waitFirst(handles: WorkflowHandle<unknown>[], options?: WaitFirstOptions): Promise<WorkflowHandle<unknown>>;
    /**
     * Wait for all of the given workflow handles to complete and return them.
     * Polls the database until each workflow's status is no longer PENDING, ENQUEUED, or DELAYED.
     * @param handles - Array of workflow handles to wait on
     * @param options - Optional polling interval
     * @returns The input handles, in the same order
     */
    static waitAll<R>(handles: WorkflowHandle<R>[], options?: WaitAllOptions): Promise<WorkflowHandle<R>[]>;
    /**
     * Create a workflow handle with a given workflow ID.
     * This call always returns a handle, even if the workflow does not exist.
     * The resulting handle will check the database to provide any workflow information.
     * @param workflowID - ID of the workflow
     * @returns `WorkflowHandle` that can be used to poll for the status or result of any workflow with `workflowID`
     */
    static retrieveWorkflow<T = unknown>(workflowID: string): WorkflowHandle<Awaited<T>>;
    /**
     * Query the system database for all workflows matching the provided predicate
     * @param input - `GetWorkflowsInput` predicate for filtering returned workflows
     * @returns `WorkflowStatus` array containing details of the matching workflows
     */
    static listWorkflows(input: GetWorkflowsInput): Promise<WorkflowStatus[]>;
    /**
     * Query the system database for all queued workflows matching the provided predicate
     * @param input - `GetQueuedWorkflowsInput` predicate for filtering returned workflows
     * @returns `WorkflowStatus` array containing details of the matching workflows
     */
    static listQueuedWorkflows(input: GetWorkflowsInput): Promise<WorkflowStatus[]>;
    /**
     * Set the priority of a queued workflow.
     * Only affects workflows with ENQUEUED or DELAYED status.
     * @param workflowID - ID of the workflow
     * @param priority - Priority value (1 to 2,147,483,647). Lower values are dequeued first.
     */
    static setWorkflowPriority(workflowID: string, priority: number): Promise<void>;
    /**
     * Update the delay on a DELAYED workflow.
     * The workflow will remain in DELAYED status until the delay expires, then transition to ENQUEUED.
     * Only affects workflows with DELAYED status.
     * @param workflowID - ID of the workflow
     * @param options - {@link SetWorkflowDelayOptions} controlling delay duration or absolute deadline, or a number of seconds
     */
    static setWorkflowDelay(workflowID: string, options: number | SetWorkflowDelayOptions): Promise<void>;
    /**
     * Retrieve the steps of a workflow
     * @param workflowID - ID of the workflow
     * @param options - Optional pagination parameters (limit, offset)
     * @returns `StepInfo` array listing the executed steps of the workflow. If the workflow is not found, `undefined` is returned.
     */
    static listWorkflowSteps(workflowID: string, options?: ListWorkflowStepsOptions): Promise<StepInfo[] | undefined>;
    /**
     * Cancel a workflow given its ID.
     * If the workflow is currently running, `DBOSWorkflowCancelledError` will be
     *   thrown from its next DBOS call.
     * @param workflowID - ID of the workflow
     * @param options - If `cancelChildren` is true, also cancel all descendant workflows recursively
     */
    static cancelWorkflow(workflowID: string, options?: CancelWorkflowsOptions): Promise<void>;
    /**
     * Cancel multiple workflows given their IDs.
     * @param workflowIDs - IDs of the workflows to cancel
     * @param options - If `cancelChildren` is true, also cancel all descendant workflows recursively
     */
    static cancelWorkflows(workflowIDs: string[], options?: CancelWorkflowsOptions): Promise<void>;
    /**
     * Resume a workflow given its ID.
     * @param workflowID - ID of the workflow
     */
    static resumeWorkflow<T>(workflowID: string, options?: {
        queueName?: string;
    }): Promise<WorkflowHandle<Awaited<T>>>;
    /**
     * Resume multiple workflows given their IDs.
     * @param workflowIDs - IDs of the workflows to resume
     * @returns An array of workflow handles
     */
    static resumeWorkflows<T>(workflowIDs: string[], options?: {
        queueName?: string;
    }): Promise<WorkflowHandle<Awaited<T>>[]>;
    /**
     * Delete a workflow and optionally all its child workflows.
     * This permanently removes the workflow from the system database.
     *
     * WARNING: This operation is irreversible.
     *
     * @param workflowID - ID of the workflow to delete
     * @param deleteChildren - If true, also delete all child workflows recursively (default: false)
     */
    static deleteWorkflow(workflowID: string, deleteChildren?: boolean): Promise<void>;
    /**
     * Delete multiple workflows and optionally all their child workflows.
     *
     * WARNING: This operation is irreversible.
     *
     * @param workflowIDs - IDs of the workflows to delete
     * @param deleteChildren - If true, also delete all child workflows recursively (default: false)
     */
    static deleteWorkflows(workflowIDs: string[], deleteChildren?: boolean): Promise<void>;
    /**
     * Fork a workflow given its ID.
     * @param workflowID - ID of the workflow
     * @param startStep - Step ID to start the forked workflow from
     * @param applicationVersion - Version of the application to use for the forked workflow
     * @returns A handle to the forked workflow
     * @throws DBOSInvalidStepIDError if the `startStep` is greater than the maximum step ID of the workflow
     */
    static forkWorkflow<T>(workflowID: string, startStep: number, options?: {
        newWorkflowID?: string;
        applicationVersion?: string;
        timeoutMS?: number;
        queueName?: string;
        queuePartitionKey?: string;
        replacementChildren?: Record<string, string>;
    }): Promise<WorkflowHandle<Awaited<T>>>;
    /**
     * Sleep for the specified amount of time.
     * If called from within a workflow, the sleep is "durable",
     *   meaning that the workflow will sleep until the wakeup time
     *   (calculated by adding `durationMS` to the original invocation time),
     *   regardless of workflow recovery.
     * @param durationMS - Length of sleep, in milliseconds.
     */
    static sleepms(durationMS: number): Promise<void>;
    /** @see sleepms */
    static sleepSeconds(durationSec: number): Promise<void>;
    /** @see sleepms */
    static sleep(durationMS: number): Promise<void>;
    /**
     * Get the current time in milliseconds, similar to `Date.now()`.
     * This function is deterministic and can be used within workflows.
     */
    static now(): Promise<number>;
    /**
     * Generate a random (v4) UUUID, similar to `node:crypto.randomUUID`.
     * This function is deterministic and can be used within workflows.
     */
    static randomUUID(): Promise<string>;
    /**
     * Use the provided `workflowID` as the identifier for first workflow started
     *   within the `callback` function.
     * @param workflowID - ID to assign to the first workflow started
     * @param callback - Function to run, which would start a workflow
     * @returns - Return value from `callback`
     */
    static withNextWorkflowID<R>(workflowID: string, callback: () => Promise<R>): Promise<R>;
    /**
     * Use the provided `authedUser` and `authedRoles` as the authenticated user for
     *   any security checks or calls to `DBOS.authenticatedUser`
     *   or `DBOS.authenticatedRoles` placed within the `callback` function.
     * @param authedUser - Authenticated user
     * @param authedRoles - Authenticated roles
     * @param callback - Function to run with authentication context in place
     * @returns - Return value from `callback`
     */
    static withAuthedContext<R>(authedUser: string, authedRoles: string[], callback: () => Promise<R>): Promise<R>;
    /**
     * This generic setter helps users calling DBOS operation to pass a name,
     *   later used in seeding a parent OTel span for the operation.
     * @param callerName - Tracing caller name
     * @param callback - Function to run with tracing context in place
     * @returns - Return value from `callback`
     */
    static withNamedContext<R>(callerName: string, callback: () => Promise<R>): Promise<R>;
    /**
     * Use queue named `queueName` for any workflows started within the `callback`.
     * @param queueName - Name of queue upon which all workflows called or started within `callback` will be run
     * @param callback - Function to run, which would call or start workflows
     * @returns - Return value from `callback`
     */
    static withWorkflowQueue<R>(queueName: string, callback: () => Promise<R>): Promise<R>;
    /**
     * Specify workflow timeout for any workflows started within the `callback`.
     * @param timeoutMS - timeout length for all workflows started within `callback` will be run
     * @param callback - Function to run, which would call or start workflows
     * @returns - Return value from `callback`
     */
    static withWorkflowTimeout<R>(timeoutMS: number | null, callback: () => Promise<R>): Promise<R>;
    /**
     * Run a workflow with the option to set any of the contextual items
     *
     * @param options - Overrides for options
     * @param callback - Function to run, which would call or start workflows
     * @returns - Return value from `callback`
     */
    static runWithContext<R>(options: DBOSContextOptions, callback: () => Promise<R>): Promise<R>;
    /**
     * Start a workflow in the background, returning a handle that can be used to check status,
     *   await the result, or otherwise interact with the workflow.
     * The full syntax is:
     * `handle = await DBOS.startWorkflow(<target function>, <params>)(<args>);`
     * @param func - The function to start.
     * @param params - `StartWorkflowParams` which may specify the ID, queue, or other parameters for starting the workflow
     * @returns `WorkflowHandle` which can be used to interact with the started workflow
     */
    static startWorkflow<Args extends unknown[], Return>(target: (...args: Args) => Promise<Return>, params?: StartWorkflowParams): (...args: Args) => Promise<WorkflowHandle<Return>>;
    /**
     * Start a workflow in the background, returning a handle that can be used to check status, await a result,
     *   or otherwise interact with the workflow.
     * The full syntax is:
     * `handle = await DBOS.startWorkflow(<target object>, <params>).<target method>(<args>);`
     * @param target - Object (which must be a `ConfiguredInstance`) containing the instance method to invoke
     * @param params - `StartWorkflowParams` which may specify the ID, queue, or other parameters for starting the workflow
     * @returns - `WorkflowHandle` which can be used to interact with the workflow
     */
    static startWorkflow<T extends ConfiguredInstance>(target: T, params?: StartWorkflowParams): InvokeFunctionsAsyncInst<T>;
    /**
     * Start a workflow in the background, returning a handle that can be used to check status, await a result,
     *   or otherwise interact with the workflow.
     * The full syntax is:
     * `handle = await DBOS.startWorkflow(<target class>, <params>).<target method>(<args>);`
     * @param target - Class containing the static method to invoke
     * @param params - `StartWorkflowParams` which may specify the ID, queue, or other parameters for starting the workflow
     * @returns - `WorkflowHandle` which can be used to interact with the workflow
     */
    static startWorkflow<T extends object>(targetClass: T, params?: StartWorkflowParams): InvokeFunctionsAsync<T>;
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
    static send<T>(destinationID: string, message: T, topic?: string, idempotencyKey?: string, options?: SendOptions): Promise<void>;
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
    static recv<T>(topic?: string, options?: number | RecvOptions): Promise<T | null>;
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
    static setEvent<T>(key: string, value: T, options?: SetEventOptions): Promise<void>;
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
    static getEvent<T>(workflowID: string, key: string, options?: number | GetEventOptions): Promise<T | null>;
    /**
     * Write a value to a stream.
     * @param key - The stream key/name within the workflow
     * @param value - A serializable value to write to the stream
     * @param options - `WriteStreamOptions` for controlling serialization
     */
    static writeStream<T>(key: string, value: T, options?: WriteStreamOptions): Promise<void>;
    /**
     * Close a stream by writing a sentinel value.
     * @param key - The stream key/name within the workflow
     */
    static closeStream(key: string): Promise<void>;
    /**
     * Read values from a stream as an async generator.
     * This function reads values from a stream identified by the workflowID and key,
     * yielding each value in order until the stream is closed or the workflow terminates.
     * @param workflowID - The workflow instance ID that owns the stream
     * @param key - The stream key/name within the workflow
     * @returns An async generator that yields each value in the stream until the stream is closed
     */
    static readStream<T>(workflowID: string, key: string): AsyncGenerator<T, void, unknown>;
    /**
     * registers a workflow method or function with an invocation schedule
     * @param func - The workflow method or function to register with an invocation schedule
     * @param options - Configuration information for the scheduled workflow
     */
    static registerScheduled<This, Return>(func: (this: This, ...args: ScheduledArgs) => Promise<Return>, config: SchedulerConfig & FunctionName): void;
    /**
     * Allow a class to be assigned a name
     */
    static className(name: string): <T extends new (...args: any[]) => object>(ctor: T) => void;
    /**
     * Decorator associating a class static method with an invocation schedule
     * @param config - The schedule, consisting of a crontab and policy for "make-up work"
     */
    static scheduled(config: SchedulerConfig): <This, Return>(target: object, propertyKey: PropertyKey, descriptor: TypedPropertyDescriptor<(this: This, ...args: ScheduledArgs) => Promise<Return>>) => TypedPropertyDescriptor<(this: This, args_0: Date, args_1: Date) => Promise<Return>>;
    /**
     * Decorator designating a method as a DBOS workflow
     *   Durable execution will be applied within calls to the workflow function
     *   This also registers the function so that it is available during recovery
     * @param config - Configuration information for the workflow
     */
    static workflow(config?: WorkflowConfig): <This, Args extends unknown[], Return>(target: object, propertyKey: string, inDescriptor: TypedPropertyDescriptor<(this: This, ...args: Args) => Promise<Return>>) => TypedPropertyDescriptor<(this: This, ...args: Args) => Promise<Return>>;
    /**
     * Create a DBOS workflow function from a provided function.
     *   Similar to the DBOS.workflow, but without requiring a decorator
     *   Durable execution will be applied to calls to the function returned by registerWorkflow
     *   This also registers the function so that it is available during recovery
     * @param func - The function to register as a workflow
     * @param name - The name of the registered workflow
     * @param options - Configuration information for the registered workflow
     */
    static registerWorkflow<This, Args extends unknown[], Return>(func: (this: This, ...args: Args) => Promise<Return>, config?: FunctionName & WorkflowConfig): (this: This, ...args: Args) => Promise<Return>;
    /**
     * Decorator designating a method as a DBOS step.
     *   A durable checkpoint will be made after the step completes
     *   This ensures "at least once" execution of the step, and that the step will not
     *    be executed again once the checkpoint is recorded
     *
     * @param config - Configuration information for the step, particularly the retry policy
     */
    static step(config?: StepConfig): <This, Args extends unknown[], Return>(target: object, propertyKey: string, inDescriptor: TypedPropertyDescriptor<(this: This, ...args: Args) => Promise<Return>>) => TypedPropertyDescriptor<(this: This, ...args: Args) => Promise<Return>>;
    /**
     * Create a check pointed DBOS step function from  a provided function
     *   Similar to the DBOS.step decorator, but without requiring a decorator
     *   A durable checkpoint will be made after the step completes
     *   This ensures "at least once" execution of the step, and that the step will not
     *    be executed again once the checkpoint is recorded
     * @param func - The function to register as a step
     * @param config - Configuration information for the step, particularly the retry policy and name
     */
    static registerStep<This, Args extends unknown[], Return>(func: (this: This, ...args: Args) => Promise<Return>, config?: StepConfig & FunctionName): (this: This, ...args: Args) => Promise<Return>;
    /**
     * Run the enclosed `callback` as a checkpointed step within a DBOS workflow
     * @param callback - function containing code to run
     * @param config - Configuration information for the step, particularly the retry policy
     * @param config.name - The name of the step; if not provided, the function name will be used
     * @returns - result (either obtained from invoking function, or retrieved if run before)
     */
    static runStep<Return>(func: () => Promise<Return>, config?: StepConfig & {
        name?: string;
    }): Promise<Return>;
    /**
     * Register serialization recipe; this is used to save/retrieve objects from the DBOS system
     *  database.  This includes workflow inputs, function return values, messages, and events.
     */
    static registerSerialization<T, S extends JSONValue>(serReg: SerializationRecipe<T, S>): void;
    /**
     * Decorate a class with the default list of required roles.
     *   This class-level default can be overridden on a per-function basis with `requiredRole`.
     * @param anyOf - The list of roles allowed access; authorization is granted if the authenticated user has any role on the list
     */
    static defaultRequiredRole(anyOf: string[]): <T extends new (...args: any[]) => object>(ctor: T) => void;
    /**
     * Decorate a method with the default list of required roles.
     * @see `DBOS.defaultRequiredRole`
     * @param anyOf - The list of roles allowed access; authorization is granted if the authenticated user has any role on the list
     */
    static requiredRole(anyOf: string[]): <This, Args extends unknown[], Return>(target: object, propertyKey: string, inDescriptor: TypedPropertyDescriptor<(this: This, ...args: Args) => Promise<Return>>) => TypedPropertyDescriptor<(this: This, ...args: Args) => Promise<Return>>;
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
    static patch(patchName: string): Promise<boolean>;
    /**
     * Check if a workflow execution has been patched, within a plan to eventually remove the unpatched (old) variant.
     *
     * `patch` may be changed to `deprecatePatch` after all unpatched workflows have completed and will not be reexecuted.
     * Once all workflows started with `patch` have completed (in favor of those using `deprecatePatch`), the `deprecatePatch` may then be removed.
     *
     * @param patchName Name of the patch to check.
     * @returns true if this is the patched(new) workflow variant, which it should always be if all unpatched workflows have been retired
     */
    static deprecatePatch(patchName: string): Promise<boolean>;
    /**
     * Register a lifecycle listener
     */
    static registerLifecycleCallback(lcl: DBOSLifecycleCallback): void;
    /**
     * Register a middleware provider
     */
    static registerMiddlewareInstaller(mwp: DBOSMethodMiddlewareInstaller): void;
    /**
     * Register information to be associated with a DBOS class
     */
    static associateClassWithInfo(external: AnyConstructor | object | string, cls: AnyConstructor | string): object;
    /**
     * Register information to be associated with a DBOS function
     */
    static associateFunctionWithInfo<This, Args extends unknown[], Return>(external: AnyConstructor | object | string, func: (this: This, ...args: Args) => Promise<Return>, target: FunctionName): {
        registration: MethodRegistration<This, Args, Return>;
        regInfo: object;
    };
    /**
     * Register information to be associated with a DBOS function
     */
    static associateParamWithInfo<This, Args extends unknown[], Return>(external: AnyConstructor | object | string, func: ((this: This, ...args: Args) => Promise<Return>) | undefined, target: FunctionName & {
        param: number | string;
    }): object | undefined;
    /** Get registrations */
    static getAssociatedInfo(external: AnyConstructor | object | string, cls?: object | string, funcName?: string): readonly ExternalRegistration[];
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
    static setAlertHandler(handler: AlertHandler): void;
    static createSchedule(options: {
        scheduleName: string;
        workflowFn: ScheduledWorkflowFn;
        schedule: string;
        context?: unknown;
        options?: ScheduleOptions;
    }): Promise<void>;
    static listSchedules(filters?: {
        status?: string | string[];
        workflowName?: string | string[];
        scheduleNamePrefix?: string | string[];
    }): Promise<WorkflowSchedule[]>;
    static getSchedule(name: string): Promise<WorkflowSchedule | null>;
    static deleteSchedule(name: string): Promise<void>;
    static pauseSchedule(name: string): Promise<void>;
    static resumeSchedule(name: string): Promise<void>;
    static updateSchedule(name: string, updates: {
        schedule?: string;
        context?: unknown;
        automaticBackfill?: boolean;
        cronTimezone?: string | null;
        queueName?: string | null;
    }): Promise<void>;
    static applySchedules(schedules: Array<{
        scheduleName: string;
        workflowFn: ScheduledWorkflowFn;
        schedule: string;
        context?: unknown;
        automaticBackfill?: boolean;
        cronTimezone?: string;
        queueName?: string;
    }>): Promise<void>;
    static triggerSchedule(name: string): Promise<WorkflowHandle<unknown>>;
    static backfillSchedule(name: string, start: Date, end: Date): Promise<WorkflowHandle<unknown>[]>;
    /**
     * List all registered application versions, ordered by version_timestamp descending (latest first).
     */
    static listApplicationVersions(): Promise<VersionInfo[]>;
    /**
     * Get the latest application version (the one with the highest version_timestamp).
     * Throws if no versions are registered.
     */
    static getLatestApplicationVersion(): Promise<VersionInfo>;
    /**
     * Set a version as the latest by updating its version_timestamp to now.
     * @param versionName - The version name to promote to latest
     */
    static setLatestApplicationVersion(versionName: string): Promise<void>;
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
    static registerQueue(name: string, options?: RegisterQueueOptions): Promise<WorkflowQueue>;
    /** Retrieve a database-backed queue by name, or `null` if no row exists. */
    static retrieveQueue(name: string): Promise<WorkflowQueue | null>;
    /** Delete a database-backed queue. Pending workflows on it are unrecoverable. */
    static deleteQueue(name: string): Promise<void>;
}
export {};
//# sourceMappingURL=dbos.d.ts.map