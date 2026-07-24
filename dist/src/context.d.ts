/// <reference types="node" />
/// <reference types="node" />
import { DBOSContextualLogger } from './telemetry/logs';
import { IncomingHttpHeaders } from 'http';
import { ParsedUrlQuery } from 'querystring';
import Koa from 'koa';
import { WorkflowSerializationFormat } from './workflow';
export interface StepStatus {
    stepID: number;
    currentAttempt?: number;
    maxAttempts?: number;
    /**
     * If the step is configured with `timeoutMS`: fires when the current attempt's timeout expires,
     * so the step can cancel its underlying operation. A fresh signal is issued for each retry attempt.
     */
    timeoutSignal?: AbortSignal;
}
export interface DBOSContextOptions {
    idAssignedForNextWorkflow?: string;
    queueAssignedForWorkflows?: string;
    logger?: DBOSContextualLogger;
    authenticatedUser?: string;
    authenticatedRoles?: string[];
    assumedRole?: string;
    request?: object;
    operationType?: string;
    operationCaller?: string;
    workflowTimeoutMS?: number | null;
    serializationType?: WorkflowSerializationFormat;
}
export interface DBOSLocalCtx extends DBOSContextOptions {
    parentCtx?: DBOSLocalCtx;
    workflowId?: string;
    curWFFunctionId?: number;
    presetID?: boolean;
    deadlineEpochMS?: number;
    inRecovery?: boolean;
    curStepFunctionId?: number;
    stepStatus?: StepStatus;
    curTxFunctionId?: number;
    koaContext?: Koa.Context;
}
export declare function isInWorkflowCtx(ctx: DBOSLocalCtx): boolean;
export declare function getCurrentContextStore(): DBOSLocalCtx | undefined;
export declare function getNextWFID(assignedID?: string): string | undefined;
export declare function functionIDGetIncrement(): number;
export declare function functionIDGet(): number;
export declare function runWithTopContext<R>(ctx: DBOSLocalCtx, callback: () => Promise<R>): Promise<R>;
export declare function runWithParentContext<R>(pctx: DBOSLocalCtx | undefined, ctx: DBOSLocalCtx, callback: () => Promise<R>): Promise<R>;
export declare function runWithDataSourceContext<R>(callnum: number, callback: () => Promise<R>): Promise<R>;
export declare function runInStepContext<R>(pctx: DBOSLocalCtx, stepID: number, maxAttempts: number | undefined, currentAttempt: number | undefined, timeoutSignal: AbortSignal | undefined, callback: () => Promise<R>): Promise<R>;
/**
 * HTTPRequest includes useful information from http.IncomingMessage and parsed body,
 *   URL parameters, and parsed query string.
 * In essence, it is the serializable part of the request.
 */
export interface HTTPRequest {
    readonly headers?: IncomingHttpHeaders;
    readonly rawHeaders?: string[];
    readonly params?: unknown;
    readonly body?: unknown;
    readonly rawBody?: string;
    readonly query?: ParsedUrlQuery;
    readonly querystring?: string;
    readonly url?: string;
    readonly method?: string;
    readonly ip?: string;
    readonly requestID?: string;
}
//# sourceMappingURL=context.d.ts.map