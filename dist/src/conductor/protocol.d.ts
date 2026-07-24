import type { StepInfo, WorkflowStatus } from '../workflow';
export declare enum MessageType {
    EXECUTOR_INFO = "executor_info",
    RECOVERY = "recovery",
    CANCEL = "cancel",
    DELETE = "delete",
    LIST_WORKFLOWS = "list_workflows",
    LIST_QUEUED_WORKFLOWS = "list_queued_workflows",
    RESUME = "resume",
    RESTART = "restart",
    GET_WORKFLOW = "get_workflow",
    EXIST_PENDING_WORKFLOWS = "exist_pending_workflows",
    LIST_STEPS = "list_steps",
    FORK_WORKFLOW = "fork_workflow",
    RETENTION = "retention",
    GET_METRICS = "get_metrics",
    EXPORT_WORKFLOW = "export_workflow",
    IMPORT_WORKFLOW = "import_workflow",
    ALERT = "alert",
    LIST_SCHEDULES = "list_schedules",
    GET_SCHEDULE = "get_schedule",
    PAUSE_SCHEDULE = "pause_schedule",
    RESUME_SCHEDULE = "resume_schedule",
    BACKFILL_SCHEDULE = "backfill_schedule",
    TRIGGER_SCHEDULE = "trigger_schedule",
    LIST_APPLICATION_VERSIONS = "list_application_versions",
    SET_LATEST_APPLICATION_VERSION = "set_latest_application_version",
    GET_WORKFLOW_EVENTS = "get_workflow_events",
    GET_WORKFLOW_NOTIFICATIONS = "get_workflow_notifications",
    GET_WORKFLOW_STREAMS = "get_workflow_streams",
    GET_WORKFLOW_AGGREGATES = "get_workflow_aggregates",
    GET_STEP_AGGREGATES = "get_step_aggregates",
    FORK_FROM_FAILURE = "fork_from_failure",
    LIST_QUEUES = "list_queues",
    GET_QUEUE = "get_queue"
}
export interface BaseMessage {
    type: MessageType;
    request_id: string;
}
export declare class BaseResponse implements BaseMessage {
    type: MessageType;
    request_id: string;
    error_message?: string;
    constructor(type: MessageType, request_id: string, error_message?: string);
}
export declare class ExecutorInfoResponse extends BaseResponse {
    executor_id: string;
    application_version: string;
    hostname: string;
    language: string;
    dbos_version: string;
    executor_metadata?: Record<string, unknown>;
    constructor(request_id: string, executor_id: string, application_version: string, hostname: string, language: string, dbos_version: string, error_message?: string, executor_metadata?: Record<string, unknown>);
}
export declare class RecoveryRequest implements BaseMessage {
    type: MessageType;
    request_id: string;
    executor_ids: string[];
    constructor(request_id: string, executor_ids: string[]);
}
export declare class RecoveryResponse extends BaseResponse {
    success: boolean;
    constructor(request_id: string, success: boolean, error_message?: string);
}
export declare class CancelRequest implements BaseMessage {
    type: MessageType;
    request_id: string;
    workflow_id: string;
    cancel_children: boolean;
    workflow_ids?: string[];
    constructor(request_id: string, workflow_id: string);
}
export declare class CancelResponse extends BaseResponse {
    success: boolean;
    constructor(request_id: string, success: boolean, error_message?: string);
}
export declare class DeleteRequest implements BaseMessage {
    type: MessageType;
    request_id: string;
    workflow_id: string;
    delete_children: boolean;
    workflow_ids?: string[];
    constructor(request_id: string, workflow_id: string, delete_children?: boolean);
}
export declare class DeleteResponse extends BaseResponse {
    success: boolean;
    constructor(request_id: string, success: boolean, error_message?: string);
}
export declare class ResumeRequest implements BaseMessage {
    type: MessageType;
    request_id: string;
    workflow_id: string;
    workflow_ids?: string[];
    queue_name?: string;
    constructor(request_id: string, workflow_id: string);
}
export declare class ResumeResponse extends BaseResponse {
    success: boolean;
    constructor(request_id: string, success: boolean, error_message?: string);
}
export declare class RestartRequest implements BaseMessage {
    type: MessageType;
    request_id: string;
    workflow_id: string;
    constructor(request_id: string, workflow_id: string);
}
export declare class RestartResponse extends BaseResponse {
    success: boolean;
    constructor(request_id: string, success: boolean, error_message?: string);
}
export interface ListWorkflowsBody {
    workflow_uuids: string[];
    workflow_name?: string | string[];
    authenticated_user?: string | string[];
    start_time?: string;
    end_time?: string;
    completed_after?: string;
    completed_before?: string;
    dequeued_after?: string;
    dequeued_before?: string;
    status?: string | string[];
    application_version?: string | string[];
    forked_from?: string | string[];
    parent_workflow_id?: string | string[];
    queue_name?: string | string[];
    limit?: number;
    offset?: number;
    sort_desc: boolean;
    workflow_id_prefix?: string | string[];
    load_input?: boolean;
    load_output?: boolean;
    executor_id?: string | string[];
    queues_only?: boolean;
    was_forked_from?: boolean;
    has_parent?: boolean;
    attributes?: Record<string, unknown>;
    schedule_name?: string | string[];
}
export declare class WorkflowsOutput {
    WorkflowUUID: string;
    Status?: string;
    WorkflowName?: string;
    WorkflowClassName?: string;
    WorkflowConfigName?: string;
    AuthenticatedUser?: string;
    AssumedRole?: string;
    AuthenticatedRoles?: string;
    Input?: string;
    Output?: string;
    Error?: string;
    CreatedAt?: string;
    UpdatedAt?: string;
    QueueName?: string;
    ApplicationVersion?: string;
    ExecutorID?: string;
    WorkflowTimeoutMS?: string;
    WorkflowDeadlineEpochMS?: string;
    DeduplicationID?: string;
    Priority?: string;
    QueuePartitionKey?: string;
    DequeuedAt?: string;
    ForkedFrom?: string;
    WasForkedFrom: boolean;
    ParentWorkflowID?: string;
    DelayUntilEpochMS?: string;
    CompletedAt?: string;
    Attributes?: string;
    ScheduleName?: string;
    constructor(info: WorkflowStatus);
}
export declare class WorkflowSteps {
    function_id: number;
    function_name: string;
    output?: string;
    error?: string;
    child_workflow_id?: string;
    started_at_epoch_ms?: string;
    completed_at_epoch_ms?: string;
    constructor(info: StepInfo);
}
export declare class ListWorkflowsRequest implements BaseMessage {
    type: MessageType;
    request_id: string;
    body: ListWorkflowsBody;
    constructor(request_id: string, body: ListWorkflowsBody);
}
export declare class ListWorkflowsResponse extends BaseResponse {
    output: WorkflowsOutput[];
    constructor(request_id: string, output: WorkflowsOutput[], error_message?: string);
}
export interface ListQueuedWorkflowsBody {
    workflow_uuids?: string[];
    workflow_name?: string | string[];
    authenticated_user?: string | string[];
    start_time?: string;
    end_time?: string;
    completed_after?: string;
    completed_before?: string;
    dequeued_after?: string;
    dequeued_before?: string;
    status?: string | string[];
    application_version?: string | string[];
    forked_from?: string | string[];
    parent_workflow_id?: string | string[];
    queue_name?: string | string[];
    limit?: number;
    offset?: number;
    sort_desc: boolean;
    workflow_id_prefix?: string | string[];
    load_input?: boolean;
    load_output?: boolean;
    executor_id?: string | string[];
    was_forked_from?: boolean;
    has_parent?: boolean;
    attributes?: Record<string, unknown>;
    schedule_name?: string | string[];
}
export declare class ListQueuedWorkflowsRequest implements BaseMessage {
    type: MessageType;
    request_id: string;
    body: ListQueuedWorkflowsBody;
    constructor(request_id: string, body: ListQueuedWorkflowsBody);
}
export declare class ListQueuedWorkflowsResponse extends BaseResponse {
    output: WorkflowsOutput[];
    constructor(request_id: string, output: WorkflowsOutput[], error_message?: string);
}
export declare class GetWorkflowRequest implements BaseMessage {
    type: MessageType;
    request_id: string;
    workflow_id: string;
    load_input?: boolean;
    load_output?: boolean;
    constructor(request_id: string, workflow_id: string);
}
export declare class GetWorkflowResponse extends BaseResponse {
    output?: WorkflowsOutput;
    constructor(request_id: string, output?: WorkflowsOutput, error_message?: string);
}
export declare class ExistPendingWorkflowsRequest implements BaseMessage {
    type: MessageType;
    request_id: string;
    executor_id: string;
    application_version: string;
    constructor(request_id: string, executor_id: string, application_version: string);
}
export declare class ExistPendingWorkflowsResponse extends BaseResponse {
    exist: boolean;
    constructor(request_id: string, exist: boolean, error_message?: string);
}
export declare class ListStepsRequest implements BaseMessage {
    type: MessageType;
    request_id: string;
    workflow_id: string;
    load_output?: boolean;
    limit?: number;
    offset?: number;
    constructor(request_id: string, workflow_id: string);
}
export declare class ListStepsResponse extends BaseResponse {
    output?: WorkflowSteps[];
    constructor(request_id: string, output?: WorkflowSteps[], error_message?: string);
}
export interface ForkWorkflowBody {
    workflow_id: string;
    start_step: number;
    application_version?: string;
    new_workflow_id?: string;
    queue_name?: string;
    queue_partition_key?: string;
}
export declare class ForkWorkflowRequest implements BaseMessage {
    type: MessageType;
    request_id: string;
    body: ForkWorkflowBody;
    constructor(request_id: string, body: ForkWorkflowBody);
}
export declare class ForkWorkflowResponse extends BaseResponse {
    new_workflow_id?: string;
    constructor(request_id: string, new_workflow_id?: string, error_message?: string);
}
export interface ForkFromFailureBody {
    workflow_ids: string[];
    application_version?: string;
    queue_name?: string;
    queue_partition_key?: string;
    from_last_failure?: boolean;
    from_last_step?: boolean;
    from_step?: number;
    from_step_name?: string;
}
export declare class ForkFromFailureRequest implements BaseMessage {
    type: MessageType;
    request_id: string;
    body: ForkFromFailureBody;
    constructor(request_id: string, body: ForkFromFailureBody);
}
export declare class ForkFromFailureResponse extends BaseResponse {
    forked_workflow_ids?: string[];
    constructor(request_id: string, forked_workflow_ids?: string[], error_message?: string);
}
export interface RetentionBody {
    gc_cutoff_epoch_ms?: number;
    gc_rows_threshold?: number;
    timeout_cutoff_epoch_ms?: number;
}
export declare class RetentionRequest implements BaseMessage {
    type: MessageType;
    request_id: string;
    body: RetentionBody;
    constructor(request_id: string, body: RetentionBody);
}
export declare class RetentionResponse extends BaseResponse {
    success: boolean;
    constructor(request_id: string, success: boolean, error_message?: string);
}
export declare class GetMetricsRequest implements BaseMessage {
    type: MessageType;
    request_id: string;
    start_time: string;
    end_time: string;
    metric_class: string;
    constructor(request_id: string, start_time: string, end_time: string, metric_class: string);
}
export declare class MetricDataOutput {
    metric_type: string;
    metric_name: string;
    value: number;
    constructor(metric_type: string, metric_name: string, value: number);
}
export declare class GetMetricsResponse extends BaseResponse {
    metrics: MetricDataOutput[];
    constructor(request_id: string, metrics: MetricDataOutput[], error_message?: string);
}
export declare class ExportWorkflowRequest implements BaseMessage {
    type: MessageType;
    request_id: string;
    workflow_id: string;
    export_children: boolean;
    constructor(request_id: string, workflow_id: string, export_children?: boolean);
}
export declare class ExportWorkflowResponse extends BaseResponse {
    serialized_workflow: string | null;
    constructor(request_id: string, serialized_workflow: string | null, error_message?: string);
}
export declare class ImportWorkflowRequest implements BaseMessage {
    type: MessageType;
    request_id: string;
    serialized_workflow: string;
    constructor(request_id: string, serialized_workflow: string);
}
export declare class ImportWorkflowResponse extends BaseResponse {
    success: boolean;
    constructor(request_id: string, success: boolean, error_message?: string);
}
export interface AlertRequest extends BaseMessage {
    type: MessageType.ALERT;
    name: string;
    message: string;
    metadata: Record<string, string>;
}
export declare class AlertResponse extends BaseResponse {
    success: boolean;
    constructor(request_id: string, success: boolean, error_message?: string);
}
export interface ScheduleOutput {
    schedule_id: string;
    schedule_name: string;
    workflow_name: string;
    workflow_class_name?: string;
    schedule: string;
    status: string;
    context?: string;
    last_fired_at: string | null;
    automatic_backfill: boolean;
    cron_timezone: string | null;
    queue_name: string | null;
}
export interface ListSchedulesBody {
    status?: string | string[];
    workflow_name?: string | string[];
    schedule_name_prefix?: string | string[];
    load_context?: boolean;
}
export declare class ListSchedulesRequest implements BaseMessage {
    type: MessageType;
    request_id: string;
    body: ListSchedulesBody;
    constructor(request_id: string, body: ListSchedulesBody);
}
export declare class ListSchedulesResponse extends BaseResponse {
    output: ScheduleOutput[];
    constructor(request_id: string, output: ScheduleOutput[], error_message?: string);
}
export declare class GetScheduleRequest implements BaseMessage {
    type: MessageType;
    request_id: string;
    schedule_name: string;
    load_context?: boolean;
    constructor(request_id: string, schedule_name: string);
}
export declare class GetScheduleResponse extends BaseResponse {
    output?: ScheduleOutput;
    constructor(request_id: string, output?: ScheduleOutput, error_message?: string);
}
export declare class PauseScheduleRequest implements BaseMessage {
    type: MessageType;
    request_id: string;
    schedule_name: string;
    constructor(request_id: string, schedule_name: string);
}
export declare class PauseScheduleResponse extends BaseResponse {
    success: boolean;
    constructor(request_id: string, success: boolean, error_message?: string);
}
export declare class ResumeScheduleRequest implements BaseMessage {
    type: MessageType;
    request_id: string;
    schedule_name: string;
    constructor(request_id: string, schedule_name: string);
}
export declare class ResumeScheduleResponse extends BaseResponse {
    success: boolean;
    constructor(request_id: string, success: boolean, error_message?: string);
}
export declare class TriggerScheduleRequest implements BaseMessage {
    type: MessageType;
    request_id: string;
    schedule_name: string;
    constructor(request_id: string, schedule_name: string);
}
export declare class TriggerScheduleResponse extends BaseResponse {
    workflow_id?: string;
    constructor(request_id: string, workflow_id?: string, error_message?: string);
}
export declare class BackfillScheduleRequest implements BaseMessage {
    type: MessageType;
    request_id: string;
    schedule_name: string;
    start: string;
    end: string;
    constructor(request_id: string, schedule_name: string, start: string, end: string);
}
export declare class BackfillScheduleResponse extends BaseResponse {
    workflow_ids: string[];
    constructor(request_id: string, workflow_ids: string[], error_message?: string);
}
export interface ApplicationVersionOutput {
    version_id: string;
    version_name: string;
    version_timestamp: number;
    created_at: number;
}
export declare class ListApplicationVersionsRequest implements BaseMessage {
    type: MessageType;
    request_id: string;
    constructor(request_id: string);
}
export declare class ListApplicationVersionsResponse extends BaseResponse {
    output: ApplicationVersionOutput[];
    constructor(request_id: string, output: ApplicationVersionOutput[], error_message?: string);
}
export declare class SetLatestApplicationVersionRequest implements BaseMessage {
    type: MessageType;
    request_id: string;
    version_name: string;
    constructor(request_id: string, version_name: string);
}
export declare class SetLatestApplicationVersionResponse extends BaseResponse {
    success: boolean;
    constructor(request_id: string, success: boolean, error_message?: string);
}
export interface EventOutput {
    key: string;
    value: string;
}
export interface NotificationOutput {
    topic: string | null;
    message: string;
    created_at_epoch_ms: number;
    consumed: boolean;
}
export interface StreamEntryOutput {
    key: string;
    values: string[];
}
export declare class GetWorkflowEventsRequest implements BaseMessage {
    type: MessageType;
    request_id: string;
    workflow_id: string;
    constructor(request_id: string, workflow_id: string);
}
export declare class GetWorkflowEventsResponse extends BaseResponse {
    events?: EventOutput[];
    constructor(request_id: string, events?: EventOutput[], error_message?: string);
}
export declare class GetWorkflowNotificationsRequest implements BaseMessage {
    type: MessageType;
    request_id: string;
    workflow_id: string;
    constructor(request_id: string, workflow_id: string);
}
export declare class GetWorkflowNotificationsResponse extends BaseResponse {
    notifications?: NotificationOutput[];
    constructor(request_id: string, notifications?: NotificationOutput[], error_message?: string);
}
export declare class GetWorkflowStreamsRequest implements BaseMessage {
    type: MessageType;
    request_id: string;
    workflow_id: string;
    constructor(request_id: string, workflow_id: string);
}
export declare class GetWorkflowStreamsResponse extends BaseResponse {
    streams?: StreamEntryOutput[];
    constructor(request_id: string, streams?: StreamEntryOutput[], error_message?: string);
}
export interface GetWorkflowAggregatesBody {
    group_by_status?: boolean;
    group_by_name?: boolean;
    group_by_queue_name?: boolean;
    group_by_executor_id?: boolean;
    group_by_application_version?: boolean;
    select_count?: boolean;
    select_min_created_at?: boolean;
    select_max_queue_wait_ms?: boolean;
    select_max_total_latency_ms?: boolean;
    time_bucket_size_ms?: number;
    status?: string[];
    start_time?: string;
    end_time?: string;
    completed_after?: string;
    completed_before?: string;
    dequeued_after?: string;
    dequeued_before?: string;
    name?: string[];
    app_version?: string[];
    executor_id?: string[];
    queue_name?: string[];
    workflow_id_prefix?: string[];
    workflow_uuids?: string[];
    authenticated_user?: string[];
    forked_from?: string[];
    parent_workflow_id?: string[];
    schedule_name?: string[];
    queues_only?: boolean;
    was_forked_from?: boolean;
    has_parent?: boolean;
    attributes?: Record<string, unknown>;
}
export declare class GetWorkflowAggregatesRequest implements BaseMessage {
    type: MessageType;
    request_id: string;
    body: GetWorkflowAggregatesBody;
    constructor(request_id: string, body: GetWorkflowAggregatesBody);
}
export interface WorkflowAggregateOutput {
    group: Record<string, string | null>;
    count?: number | null;
    min_created_at?: number | null;
    max_queue_wait_ms?: number | null;
    max_total_latency_ms?: number | null;
}
export declare class GetWorkflowAggregatesResponse extends BaseResponse {
    output: WorkflowAggregateOutput[];
    constructor(request_id: string, output: WorkflowAggregateOutput[], error_message?: string);
}
export interface GetStepAggregatesBody {
    group_by_function_name?: boolean;
    group_by_status?: boolean;
    select_count?: boolean;
    select_max_duration_ms?: boolean;
    time_bucket_size_ms?: number;
    status?: string[];
    function_name?: string[];
    workflow_id_prefix?: string[];
    completed_after?: string;
    completed_before?: string;
}
export declare class GetStepAggregatesRequest implements BaseMessage {
    type: MessageType;
    request_id: string;
    body: GetStepAggregatesBody;
    constructor(request_id: string, body: GetStepAggregatesBody);
}
export interface StepAggregateOutput {
    group: Record<string, string | null>;
    count?: number | null;
    max_duration_ms?: number | null;
}
export declare class GetStepAggregatesResponse extends BaseResponse {
    output: StepAggregateOutput[];
    constructor(request_id: string, output: StepAggregateOutput[], error_message?: string);
}
export interface QueueOutput {
    name: string;
    concurrency: number | null;
    worker_concurrency: number | null;
    rate_limit_max: number | null;
    rate_limit_period_sec: number | null;
    priority_enabled: boolean;
    partition_queue: boolean;
    polling_interval_sec: number;
}
export declare class ListQueuesRequest implements BaseMessage {
    type: MessageType;
    request_id: string;
    constructor(request_id: string);
}
export declare class ListQueuesResponse extends BaseResponse {
    output: QueueOutput[];
    constructor(request_id: string, output: QueueOutput[], error_message?: string);
}
export declare class GetQueueRequest implements BaseMessage {
    type: MessageType;
    request_id: string;
    name: string;
    constructor(request_id: string, name: string);
}
export declare class GetQueueResponse extends BaseResponse {
    output: QueueOutput | null;
    constructor(request_id: string, output: QueueOutput | null, error_message?: string);
}
//# sourceMappingURL=protocol.d.ts.map