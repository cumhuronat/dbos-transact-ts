import { type SystemDatabase, WorkflowScheduleInternal } from '../system_database';
import { DBOSSerializer } from '../serialization';
import { DBOSLifecycleCallback } from '../decorators';
export type ScheduledWorkflowFn = (scheduledDate: Date, context: any) => Promise<void>;
export interface ScheduleOptions {
    automaticBackfill?: boolean;
    cronTimezone?: string;
    queueName?: string;
}
export interface WorkflowSchedule {
    scheduleId: string;
    scheduleName: string;
    workflowName: string;
    workflowClassName: string;
    schedule: string;
    status: string;
    context: unknown;
    lastFiredAt: string | null;
    automaticBackfill: boolean;
    cronTimezone: string | null;
    queueName: string | null;
}
export declare function toWorkflowSchedule(internal: WorkflowScheduleInternal, serializer: DBOSSerializer): Promise<WorkflowSchedule>;
export declare function createScheduleId(): string;
export declare class DynamicSchedulerLoop implements DBOSLifecycleCallback {
    #private;
    constructor(pollingIntervalMs?: number);
    initialize(): Promise<void>;
    destroy(): Promise<void>;
}
export declare function triggerSchedule(systemDatabase: SystemDatabase, serializer: DBOSSerializer, name: string): Promise<string>;
export declare function backfillSchedule(systemDatabase: SystemDatabase, serializer: DBOSSerializer, name: string, start: Date, end: Date): Promise<string[]>;
//# sourceMappingURL=scheduler.d.ts.map