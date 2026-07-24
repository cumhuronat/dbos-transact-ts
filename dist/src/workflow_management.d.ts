import type { SystemDatabase, WorkflowStatusInternal } from './system_database';
import type { StepInfo, WorkflowStatus, GetWorkflowsInput, ListWorkflowStepsOptions } from './workflow';
import { DBOSSerializer } from './serialization';
export declare function listWorkflows(sysdb: SystemDatabase, input: GetWorkflowsInput): Promise<WorkflowStatus[]>;
export declare function listQueuedWorkflows(sysdb: SystemDatabase, input: GetWorkflowsInput): Promise<WorkflowStatus[]>;
export declare function getWorkflow(sysdb: SystemDatabase, workflowID: string): Promise<WorkflowStatus | undefined>;
export declare function listWorkflowSteps(sysdb: SystemDatabase, workflowID: string, loadOutput?: boolean, options?: ListWorkflowStepsOptions): Promise<StepInfo[] | undefined>;
export declare function forkWorkflow(sysdb: SystemDatabase, workflowID: string, startStep: number, options?: {
    newWorkflowID?: string;
    applicationVersion?: string;
    timeoutMS?: number;
    queueName?: string;
    queuePartitionKey?: string;
    replacementChildren?: Record<string, string>;
}): Promise<string>;
export declare function toWorkflowStatus(internal: WorkflowStatusInternal, serializer: DBOSSerializer): Promise<WorkflowStatus>;
export declare function globalTimeout(sysdb: SystemDatabase, cutoffEpochTimestampMs: number): Promise<void>;
//# sourceMappingURL=workflow_management.d.ts.map