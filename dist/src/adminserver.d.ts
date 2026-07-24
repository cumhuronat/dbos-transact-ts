/// <reference types="node" />
/// <reference types="node" />
import * as http from 'http';
import { DBOSExecutor } from './dbos-executor';
import { GlobalLogger } from './telemetry/logs';
import { QueueParameters } from './wfqueue';
export type QueueMetadataResponse = QueueParameters & {
    name: string;
};
export declare const WorkflowUUIDHeader = "dbos-idempotency-key";
export declare const WorkflowRecoveryUrl = "/dbos-workflow-recovery";
export declare const HealthUrl = "/dbos-healthz";
export declare const PerfUrl = "/dbos-perf";
export declare const DeactivateUrl = "/deactivate";
export declare const ConductorUrl = "/conductor";
export declare const WorkflowQueuesMetadataUrl = "/dbos-workflow-queues-metadata";
interface Route {
    method: string;
    path: string;
    handler: (req: http.IncomingMessage, res: http.ServerResponse, params?: Record<string, string>) => Promise<void>;
}
export declare class DBOSAdminServer {
    static setupAdminApp(dbosExec: DBOSExecutor): http.Server;
    static checkPortAvailabilityIPv4Ipv6(port: number, logger: GlobalLogger): Promise<void>;
    static checkPortAvailability(port: number, host: string): Promise<void>;
    /**
     * Health check endpoint.
     */
    static registerHealthEndpoint(dbosExec: DBOSExecutor, routes: Route[]): void;
    /**
     * Register workflow queue metadata endpoint.
     */
    static registerQueueMetadataEndpoint(dbosExec: DBOSExecutor, routes: Route[]): void;
    /**
     * Register workflow recovery endpoint.
     * Receives a list of executor IDs and returns a list of workflowUUIDs.
     */
    static registerRecoveryEndpoint(dbosExec: DBOSExecutor, routes: Route[]): void;
    /**
     * Register performance endpoint.
     * Returns information on VM performance since last call.
     */
    static lastELU: import("perf_hooks").EventLoopUtilization;
    static registerPerfEndpoint(dbosExec: DBOSExecutor, routes: Route[]): void;
    /**
     * Register Deactivate endpoint.
     * Deactivate consumers so that they don't start new workflows.
     */
    static isDeactivated: boolean;
    static registerDeactivateEndpoint(dbosExec: DBOSExecutor, routes: Route[]): void;
    /**
     * Register Conductor status endpoint.
     * Indicates that a Conductor connection is enabled for this executor.
     */
    static registerConductorEndpoint(dbosExec: DBOSExecutor, routes: Route[]): void;
    static registerGarbageCollectEndpoint(dbosExec: DBOSExecutor, routes: Route[]): void;
    static registerGlobalTimeoutEndpoint(dbosExec: DBOSExecutor, routes: Route[]): void;
    /**
     * Register Cancel Workflow endpoint.
     * Cancels a workflow by setting its status to CANCELLED.
     */
    static registerCancelWorkflowEndpoint(dbosExec: DBOSExecutor, routes: Route[]): void;
    /**
     * Register Resume Workflow endpoint.
     * Resume a workflow.
     */
    static registerResumeWorkflowEndpoint(dbosExec: DBOSExecutor, routes: Route[]): void;
    /**
     * Register Restart Workflow endpoint.
     * Restart a workflow.
     */
    static registerRestartWorkflowEndpoint(dbosExec: DBOSExecutor, routes: Route[]): void;
    /**
     * Register Fork Workflow endpoint.
     */
    static registerForkWorkflowEndpoint(dbosExec: DBOSExecutor, routes: Route[]): void;
    /**
     * Register List Workflow Steps endpoint.
     * List steps for a given workflow.
     */
    static registerListWorkflowStepsEndpoint(dbosExec: DBOSExecutor, routes: Route[]): void;
    /**
     * Register List Workflows endpoint.
     * List workflows with optional filtering via request body.
     */
    static registerListWorkflowsEndpoint(dbosExec: DBOSExecutor, routes: Route[]): void;
    /**
     * Register List Queued Workflows endpoint.
     * List queued workflows with optional filtering via request body.
     */
    static registerListQueuedWorkflowsEndpoint(dbosExec: DBOSExecutor, routes: Route[]): void;
    /**
     * Register Get Workflow endpoint.
     * Get detailed information about a specific workflow by ID.
     */
    static registerGetWorkflowEndpoint(dbosExec: DBOSExecutor, routes: Route[]): void;
}
export {};
//# sourceMappingURL=adminserver.d.ts.map