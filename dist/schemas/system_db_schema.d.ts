export type SysDBSerializationFormat = string;
export interface workflow_status {
    workflow_uuid: string;
    status: string;
    name: string;
    class_name?: string;
    config_name?: string;
    authenticated_user: string;
    output: string;
    error: string;
    assumed_role: string;
    authenticated_roles: string;
    request: string;
    executor_id: string;
    application_version?: string;
    queue_name?: string;
    created_at: number;
    updated_at: number;
    application_id: string;
    recovery_attempts: number;
    workflow_timeout_ms: number | null;
    workflow_deadline_epoch_ms: number | null;
    inputs: string;
    started_at_epoch_ms?: number;
    deduplication_id?: string;
    priority?: number;
    queue_partition_key?: string;
    forked_from?: string;
    was_forked_from?: boolean;
    owner_xid?: string;
    parent_workflow_id?: string;
    serialization: SysDBSerializationFormat | null;
    delay_until_epoch_ms?: number | null;
    rate_limited?: boolean;
    completed_at?: number | null;
    attributes?: Record<string, unknown> | null;
    schedule_name?: string | null;
    debounce_deadline_epoch_ms?: number | null;
    is_debounced?: boolean;
}
export interface notifications {
    destination_uuid: string;
    topic: string;
    message: string;
    consumed: boolean;
    serialization: SysDBSerializationFormat | null;
}
export interface workflow_events {
    workflow_uuid: string;
    key: string;
    value: string;
    serialization: SysDBSerializationFormat | null;
}
export interface operation_outputs {
    workflow_uuid: string;
    function_id: number;
    output: string;
    error: string;
    child_workflow_id: string;
    function_name?: string;
    started_at_epoch_ms?: number;
    completed_at_epoch_ms?: number;
    serialization: SysDBSerializationFormat | null;
}
export interface event_dispatch_kv {
    service_name: string;
    workflow_fn_name: string;
    key: string;
    value?: string;
    update_time?: number;
    update_seq?: bigint;
}
export interface streams {
    workflow_uuid: string;
    key: string;
    value: string;
    offset: number;
    function_id: number;
    serialization: SysDBSerializationFormat | null;
}
export interface workflow_events_history {
    workflow_uuid: string;
    function_id: number;
    key: string;
    value: string;
    serialization: SysDBSerializationFormat | null;
}
export interface workflow_schedules {
    schedule_id: string;
    schedule_name: string;
    workflow_name: string;
    workflow_class_name: string;
    schedule: string;
    status: string;
    context: string;
    last_fired_at: string | null;
    automatic_backfill: boolean;
    cron_timezone: string | null;
    queue_name: string | null;
}
export interface application_versions {
    version_id: string;
    version_name: string;
    version_timestamp: number;
    created_at: number;
}
export interface queues {
    queue_id: string;
    name: string;
    concurrency: number | null;
    worker_concurrency: number | null;
    rate_limit_max: number | null;
    rate_limit_period_sec: number | null;
    priority_enabled: boolean;
    partition_queue: boolean;
    polling_interval_sec: number;
    created_at: number;
    updated_at: number;
}
export interface step_info {
    function_id: number;
    function_name: string;
    output: unknown;
    error: Error | null;
    child_workflow_id: string | null;
    started_at_epoch_ms?: number;
    completed_at_epoch_ms?: number;
}
export type JsonPrimitive = null | boolean | number | string;
export type JsonValue = JsonPrimitive | JsonObject | JsonArray;
export type JsonObject = {
    [k: string]: JsonValue;
};
export type JsonArray = JsonValue[];
export type JsonWorkflowArgs = {
    positionalArgs?: JsonArray;
    namedArgs?: JsonObject;
};
export type JsonWorkflowResult = JsonValue;
export interface JsonWorkflowErrorData {
    name: string;
    message: string;
    code?: number | string;
    data?: JsonValue;
}
export declare class PortableWorkflowError extends Error {
    readonly name: string;
    readonly code?: string | number | undefined;
    readonly data?: JsonValue | undefined;
    constructor(message: string, name: string, code?: string | number | undefined, data?: JsonValue | undefined);
}
export type JsonMessage = JsonValue;
export type JsonEvent = JsonValue;
//# sourceMappingURL=system_db_schema.d.ts.map