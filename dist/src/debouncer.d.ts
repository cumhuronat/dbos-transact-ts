import { DBOSClient } from '.';
import { StartWorkflowParams } from './dbos';
import { WorkflowHandle, WorkflowSerializationFormat } from './workflow';
interface DebouncerConfig<Args extends unknown[], Return> {
    workflow: (...args: Args) => Promise<Return>;
    startWorkflowParams?: StartWorkflowParams;
    debounceTimeoutMs?: number;
}
interface DebouncerClientConfig {
    workflowName: string;
    workflowClassName?: string;
    startWorkflowParams?: StartWorkflowParams;
    debounceTimeoutMs?: number;
    serializationType?: WorkflowSerializationFormat;
}
export declare class Debouncer<Args extends unknown[], Return> {
    private readonly cfg;
    constructor(params: DebouncerConfig<Args, Return>);
    debounce(debounceKey: string, debouncePeriodMs: number, ...args: Args): Promise<WorkflowHandle<Return>>;
}
export declare class DebouncerClient {
    readonly client: DBOSClient;
    private readonly cfg;
    private readonly serializationType;
    constructor(client: DBOSClient, params: DebouncerClientConfig);
    debounce(debounceKey: string, debouncePeriodMs: number, ...args: unknown[]): Promise<WorkflowHandle<unknown>>;
}
export {};
//# sourceMappingURL=debouncer.d.ts.map