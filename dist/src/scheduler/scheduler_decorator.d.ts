import { DBOSLifecycleCallback, FunctionName } from '../decorators';
/**
 * Choices for scheduler mode for `@DBOS.scheduled` workflows
 */
export declare enum SchedulerMode {
    /**
     * Using `ExactlyOncePerInterval` causes the scheduler to add "make-up work" for any
     *  schedule slots that occurred when the app was not running
     */
    ExactlyOncePerInterval = "ExactlyOncePerInterval",
    /**
     * Using `ExactlyOncePerIntervalWhenActive` causes the scheduler to run the workflow once
     *  per interval when the application is active.  If the app is not running at a time
     *  otherwise indicated by the schedule, no workflow will be run.
     */
    ExactlyOncePerIntervalWhenActive = "ExactlyOncePerIntervalWhenActive"
}
/**
 * Configuration for a `@DBOS.scheduled` workflow
 */
export interface SchedulerConfig {
    /** Schedule, in 5- or 6-spot crontab format */
    crontab: string;
    /**
     * Indicates whether or not to retroactively start workflows that were scheduled during
     *  times when the app was not running.  @see `SchedulerMode`.
     */
    mode?: SchedulerMode;
    /** If set, workflows will be enqueued on the named queue, rather than being started immediately */
    queueName?: string;
}
export type ScheduledArgs = [Date, Date];
export declare class ScheduledReceiver implements DBOSLifecycleCallback {
    #private;
    constructor();
    initialize(): Promise<void>;
    destroy(): Promise<void>;
    logRegisteredEndpoints(): void;
    static registerScheduled<This, Return>(func: (this: This, ...args: ScheduledArgs) => Promise<Return>, config: SchedulerConfig & FunctionName): void;
}
//# sourceMappingURL=scheduler_decorator.d.ts.map