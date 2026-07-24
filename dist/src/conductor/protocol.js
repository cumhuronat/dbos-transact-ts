"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.BackfillScheduleRequest = exports.TriggerScheduleResponse = exports.TriggerScheduleRequest = exports.ResumeScheduleResponse = exports.ResumeScheduleRequest = exports.PauseScheduleResponse = exports.PauseScheduleRequest = exports.GetScheduleResponse = exports.GetScheduleRequest = exports.ListSchedulesResponse = exports.ListSchedulesRequest = exports.AlertResponse = exports.ImportWorkflowResponse = exports.ImportWorkflowRequest = exports.ExportWorkflowResponse = exports.ExportWorkflowRequest = exports.GetMetricsResponse = exports.MetricDataOutput = exports.GetMetricsRequest = exports.RetentionResponse = exports.RetentionRequest = exports.ForkFromFailureResponse = exports.ForkFromFailureRequest = exports.ForkWorkflowResponse = exports.ForkWorkflowRequest = exports.ListStepsResponse = exports.ListStepsRequest = exports.ExistPendingWorkflowsResponse = exports.ExistPendingWorkflowsRequest = exports.GetWorkflowResponse = exports.GetWorkflowRequest = exports.ListQueuedWorkflowsResponse = exports.ListQueuedWorkflowsRequest = exports.ListWorkflowsResponse = exports.ListWorkflowsRequest = exports.WorkflowSteps = exports.WorkflowsOutput = exports.RestartResponse = exports.RestartRequest = exports.ResumeResponse = exports.ResumeRequest = exports.DeleteResponse = exports.DeleteRequest = exports.CancelResponse = exports.CancelRequest = exports.RecoveryResponse = exports.RecoveryRequest = exports.ExecutorInfoResponse = exports.BaseResponse = exports.MessageType = void 0;
exports.GetQueueResponse = exports.GetQueueRequest = exports.ListQueuesResponse = exports.ListQueuesRequest = exports.GetStepAggregatesResponse = exports.GetStepAggregatesRequest = exports.GetWorkflowAggregatesResponse = exports.GetWorkflowAggregatesRequest = exports.GetWorkflowStreamsResponse = exports.GetWorkflowStreamsRequest = exports.GetWorkflowNotificationsResponse = exports.GetWorkflowNotificationsRequest = exports.GetWorkflowEventsResponse = exports.GetWorkflowEventsRequest = exports.SetLatestApplicationVersionResponse = exports.SetLatestApplicationVersionRequest = exports.ListApplicationVersionsResponse = exports.ListApplicationVersionsRequest = exports.BackfillScheduleResponse = void 0;
const node_util_1 = require("node:util");
const serialize_error_1 = require("serialize-error");
var MessageType;
(function (MessageType) {
    MessageType["EXECUTOR_INFO"] = "executor_info";
    MessageType["RECOVERY"] = "recovery";
    MessageType["CANCEL"] = "cancel";
    MessageType["DELETE"] = "delete";
    MessageType["LIST_WORKFLOWS"] = "list_workflows";
    MessageType["LIST_QUEUED_WORKFLOWS"] = "list_queued_workflows";
    MessageType["RESUME"] = "resume";
    MessageType["RESTART"] = "restart";
    MessageType["GET_WORKFLOW"] = "get_workflow";
    MessageType["EXIST_PENDING_WORKFLOWS"] = "exist_pending_workflows";
    MessageType["LIST_STEPS"] = "list_steps";
    MessageType["FORK_WORKFLOW"] = "fork_workflow";
    MessageType["RETENTION"] = "retention";
    MessageType["GET_METRICS"] = "get_metrics";
    MessageType["EXPORT_WORKFLOW"] = "export_workflow";
    MessageType["IMPORT_WORKFLOW"] = "import_workflow";
    MessageType["ALERT"] = "alert";
    MessageType["LIST_SCHEDULES"] = "list_schedules";
    MessageType["GET_SCHEDULE"] = "get_schedule";
    MessageType["PAUSE_SCHEDULE"] = "pause_schedule";
    MessageType["RESUME_SCHEDULE"] = "resume_schedule";
    MessageType["BACKFILL_SCHEDULE"] = "backfill_schedule";
    MessageType["TRIGGER_SCHEDULE"] = "trigger_schedule";
    MessageType["LIST_APPLICATION_VERSIONS"] = "list_application_versions";
    MessageType["SET_LATEST_APPLICATION_VERSION"] = "set_latest_application_version";
    MessageType["GET_WORKFLOW_EVENTS"] = "get_workflow_events";
    MessageType["GET_WORKFLOW_NOTIFICATIONS"] = "get_workflow_notifications";
    MessageType["GET_WORKFLOW_STREAMS"] = "get_workflow_streams";
    MessageType["GET_WORKFLOW_AGGREGATES"] = "get_workflow_aggregates";
    MessageType["GET_STEP_AGGREGATES"] = "get_step_aggregates";
    MessageType["FORK_FROM_FAILURE"] = "fork_from_failure";
    MessageType["LIST_QUEUES"] = "list_queues";
    MessageType["GET_QUEUE"] = "get_queue";
})(MessageType || (exports.MessageType = MessageType = {}));
class BaseResponse {
    type;
    request_id;
    error_message;
    constructor(type, request_id, error_message) {
        this.type = type;
        this.request_id = request_id;
        this.error_message = error_message;
    }
}
exports.BaseResponse = BaseResponse;
class ExecutorInfoResponse extends BaseResponse {
    executor_id;
    application_version;
    hostname;
    language;
    dbos_version;
    executor_metadata;
    constructor(request_id, executor_id, application_version, hostname, language, dbos_version, error_message, executor_metadata) {
        super(MessageType.EXECUTOR_INFO, request_id, error_message);
        this.executor_id = executor_id;
        this.application_version = application_version;
        this.hostname = hostname;
        this.language = language;
        this.dbos_version = dbos_version;
        this.executor_metadata = executor_metadata;
    }
}
exports.ExecutorInfoResponse = ExecutorInfoResponse;
class RecoveryRequest {
    type = MessageType.RECOVERY;
    request_id;
    executor_ids;
    constructor(request_id, executor_ids) {
        this.request_id = request_id;
        this.executor_ids = executor_ids;
    }
}
exports.RecoveryRequest = RecoveryRequest;
class RecoveryResponse extends BaseResponse {
    success;
    constructor(request_id, success, error_message) {
        super(MessageType.RECOVERY, request_id, error_message);
        this.success = success;
    }
}
exports.RecoveryResponse = RecoveryResponse;
class CancelRequest {
    type = MessageType.CANCEL;
    request_id;
    workflow_id;
    cancel_children = false;
    workflow_ids;
    constructor(request_id, workflow_id) {
        this.request_id = request_id;
        this.workflow_id = workflow_id;
    }
}
exports.CancelRequest = CancelRequest;
class CancelResponse extends BaseResponse {
    success;
    constructor(request_id, success, error_message) {
        super(MessageType.CANCEL, request_id, error_message);
        this.success = success;
    }
}
exports.CancelResponse = CancelResponse;
class DeleteRequest {
    type = MessageType.DELETE;
    request_id;
    workflow_id;
    delete_children;
    workflow_ids;
    constructor(request_id, workflow_id, delete_children = false) {
        this.request_id = request_id;
        this.workflow_id = workflow_id;
        this.delete_children = delete_children;
    }
}
exports.DeleteRequest = DeleteRequest;
class DeleteResponse extends BaseResponse {
    success;
    constructor(request_id, success, error_message) {
        super(MessageType.DELETE, request_id, error_message);
        this.success = success;
    }
}
exports.DeleteResponse = DeleteResponse;
class ResumeRequest {
    type = MessageType.RESUME;
    request_id;
    workflow_id;
    workflow_ids;
    queue_name;
    constructor(request_id, workflow_id) {
        this.request_id = request_id;
        this.workflow_id = workflow_id;
    }
}
exports.ResumeRequest = ResumeRequest;
class ResumeResponse extends BaseResponse {
    success;
    constructor(request_id, success, error_message) {
        super(MessageType.RESUME, request_id, error_message);
        this.success = success;
    }
}
exports.ResumeResponse = ResumeResponse;
class RestartRequest {
    type = MessageType.RESTART;
    request_id;
    workflow_id;
    constructor(request_id, workflow_id) {
        this.request_id = request_id;
        this.workflow_id = workflow_id;
    }
}
exports.RestartRequest = RestartRequest;
class RestartResponse extends BaseResponse {
    success;
    constructor(request_id, success, error_message) {
        super(MessageType.RESTART, request_id, error_message);
        this.success = success;
    }
}
exports.RestartResponse = RestartResponse;
class WorkflowsOutput {
    WorkflowUUID;
    Status;
    WorkflowName;
    WorkflowClassName;
    WorkflowConfigName;
    AuthenticatedUser;
    AssumedRole;
    AuthenticatedRoles;
    Input;
    Output;
    Error;
    CreatedAt;
    UpdatedAt;
    QueueName;
    ApplicationVersion;
    ExecutorID;
    WorkflowTimeoutMS;
    WorkflowDeadlineEpochMS;
    DeduplicationID;
    Priority;
    QueuePartitionKey;
    DequeuedAt;
    ForkedFrom;
    WasForkedFrom;
    ParentWorkflowID;
    DelayUntilEpochMS;
    CompletedAt;
    Attributes;
    ScheduleName;
    constructor(info) {
        // Mark empty fields as undefined
        this.WorkflowUUID = info.workflowID;
        this.Status = info.status;
        this.WorkflowName = info.workflowName;
        this.WorkflowClassName = info.workflowClassName ? info.workflowClassName : undefined;
        this.WorkflowConfigName = info.workflowConfigName ? info.workflowConfigName : undefined;
        this.AuthenticatedUser = info.authenticatedUser ? info.authenticatedUser : undefined;
        this.AssumedRole = info.assumedRole ? info.assumedRole : undefined;
        this.AuthenticatedRoles =
            (info.authenticatedRoles ?? []).length > 0 ? JSON.stringify(info.authenticatedRoles) : undefined;
        this.Input = info.input ? (0, node_util_1.inspect)(info.input) : undefined;
        this.Output = info.output ? (0, node_util_1.inspect)(info.output) : undefined;
        this.Error = info.error ? JSON.stringify((0, serialize_error_1.serializeError)(info.error)) : undefined;
        this.CreatedAt = info.createdAt ? String(info.createdAt) : undefined;
        this.UpdatedAt = info.updatedAt ? String(info.updatedAt) : undefined;
        this.QueueName = info.queueName ? info.queueName : undefined;
        this.ApplicationVersion = info.applicationVersion;
        this.ExecutorID = info.executorId;
        this.WorkflowTimeoutMS = info.timeoutMS !== undefined ? String(info.timeoutMS) : undefined;
        this.WorkflowDeadlineEpochMS = info.deadlineEpochMS !== undefined ? String(info.deadlineEpochMS) : undefined;
        this.DeduplicationID = info.deduplicationID;
        this.Priority = String(info.priority);
        this.QueuePartitionKey = info.queuePartitionKey;
        this.DequeuedAt = info.dequeuedAt !== undefined ? String(info.dequeuedAt) : undefined;
        this.ForkedFrom = info.forkedFrom;
        this.WasForkedFrom = info.wasForkedFrom ?? false;
        this.ParentWorkflowID = info.parentWorkflowID;
        this.DelayUntilEpochMS = info.delayUntilEpochMS !== undefined ? String(info.delayUntilEpochMS) : undefined;
        this.CompletedAt = info.completedAt !== undefined ? String(info.completedAt) : undefined;
        // JSON rather than inspect() so the wire format stays parseable by Conductor.
        this.Attributes = info.attributes !== undefined ? JSON.stringify(info.attributes) : undefined;
        this.ScheduleName = info.scheduleName;
    }
}
exports.WorkflowsOutput = WorkflowsOutput;
class WorkflowSteps {
    function_id;
    function_name;
    output;
    error;
    child_workflow_id;
    started_at_epoch_ms;
    completed_at_epoch_ms;
    constructor(info) {
        this.function_id = info.functionID;
        this.function_name = info.name;
        this.output = info.output ? (0, node_util_1.inspect)(info.output) : undefined;
        this.error = info.error ? JSON.stringify((0, serialize_error_1.serializeError)(info.error)) : undefined;
        this.child_workflow_id = info.childWorkflowID ?? undefined;
        this.started_at_epoch_ms = info.startedAtEpochMs !== undefined ? String(info.startedAtEpochMs) : undefined;
        this.completed_at_epoch_ms = info.completedAtEpochMs !== undefined ? String(info.completedAtEpochMs) : undefined;
    }
}
exports.WorkflowSteps = WorkflowSteps;
class ListWorkflowsRequest {
    type = MessageType.LIST_WORKFLOWS;
    request_id;
    body;
    constructor(request_id, body) {
        this.request_id = request_id;
        this.body = body;
    }
}
exports.ListWorkflowsRequest = ListWorkflowsRequest;
class ListWorkflowsResponse extends BaseResponse {
    output;
    constructor(request_id, output, error_message) {
        super(MessageType.LIST_WORKFLOWS, request_id, error_message);
        this.output = output;
    }
}
exports.ListWorkflowsResponse = ListWorkflowsResponse;
class ListQueuedWorkflowsRequest {
    type = MessageType.LIST_QUEUED_WORKFLOWS;
    request_id;
    body;
    constructor(request_id, body) {
        this.request_id = request_id;
        this.body = body;
    }
}
exports.ListQueuedWorkflowsRequest = ListQueuedWorkflowsRequest;
class ListQueuedWorkflowsResponse extends BaseResponse {
    output;
    constructor(request_id, output, error_message) {
        super(MessageType.LIST_QUEUED_WORKFLOWS, request_id, error_message);
        this.output = output;
    }
}
exports.ListQueuedWorkflowsResponse = ListQueuedWorkflowsResponse;
class GetWorkflowRequest {
    type = MessageType.GET_WORKFLOW;
    request_id;
    workflow_id;
    load_input;
    load_output;
    constructor(request_id, workflow_id) {
        this.request_id = request_id;
        this.workflow_id = workflow_id;
    }
}
exports.GetWorkflowRequest = GetWorkflowRequest;
class GetWorkflowResponse extends BaseResponse {
    output;
    constructor(request_id, output, error_message) {
        super(MessageType.GET_WORKFLOW, request_id, error_message);
        this.output = output;
    }
}
exports.GetWorkflowResponse = GetWorkflowResponse;
class ExistPendingWorkflowsRequest {
    type = MessageType.EXIST_PENDING_WORKFLOWS;
    request_id;
    executor_id;
    application_version;
    constructor(request_id, executor_id, application_version) {
        this.request_id = request_id;
        this.executor_id = executor_id;
        this.application_version = application_version;
    }
}
exports.ExistPendingWorkflowsRequest = ExistPendingWorkflowsRequest;
class ExistPendingWorkflowsResponse extends BaseResponse {
    exist;
    constructor(request_id, exist, error_message) {
        super(MessageType.EXIST_PENDING_WORKFLOWS, request_id, error_message);
        this.exist = exist;
    }
}
exports.ExistPendingWorkflowsResponse = ExistPendingWorkflowsResponse;
class ListStepsRequest {
    type = MessageType.LIST_STEPS;
    request_id;
    workflow_id;
    load_output;
    limit;
    offset;
    constructor(request_id, workflow_id) {
        this.request_id = request_id;
        this.workflow_id = workflow_id;
    }
}
exports.ListStepsRequest = ListStepsRequest;
class ListStepsResponse extends BaseResponse {
    output;
    constructor(request_id, output, error_message) {
        super(MessageType.LIST_STEPS, request_id, error_message);
        this.output = output;
    }
}
exports.ListStepsResponse = ListStepsResponse;
class ForkWorkflowRequest {
    type = MessageType.FORK_WORKFLOW;
    request_id;
    body;
    constructor(request_id, body) {
        this.request_id = request_id;
        this.body = body;
    }
}
exports.ForkWorkflowRequest = ForkWorkflowRequest;
class ForkWorkflowResponse extends BaseResponse {
    new_workflow_id;
    constructor(request_id, new_workflow_id, error_message) {
        super(MessageType.FORK_WORKFLOW, request_id, error_message);
        this.new_workflow_id = new_workflow_id;
    }
}
exports.ForkWorkflowResponse = ForkWorkflowResponse;
class ForkFromFailureRequest {
    type = MessageType.FORK_FROM_FAILURE;
    request_id;
    body;
    constructor(request_id, body) {
        this.request_id = request_id;
        this.body = body;
    }
}
exports.ForkFromFailureRequest = ForkFromFailureRequest;
class ForkFromFailureResponse extends BaseResponse {
    forked_workflow_ids;
    constructor(request_id, forked_workflow_ids, error_message) {
        super(MessageType.FORK_FROM_FAILURE, request_id, error_message);
        this.forked_workflow_ids = forked_workflow_ids;
    }
}
exports.ForkFromFailureResponse = ForkFromFailureResponse;
class RetentionRequest {
    type = MessageType.RETENTION;
    request_id;
    body;
    constructor(request_id, body) {
        this.request_id = request_id;
        this.body = body;
    }
}
exports.RetentionRequest = RetentionRequest;
class RetentionResponse extends BaseResponse {
    success;
    constructor(request_id, success, error_message) {
        super(MessageType.RETENTION, request_id, error_message);
        this.success = success;
    }
}
exports.RetentionResponse = RetentionResponse;
class GetMetricsRequest {
    type = MessageType.GET_METRICS;
    request_id;
    start_time;
    end_time;
    metric_class;
    constructor(request_id, start_time, end_time, metric_class) {
        this.request_id = request_id;
        this.start_time = start_time;
        this.end_time = end_time;
        this.metric_class = metric_class;
    }
}
exports.GetMetricsRequest = GetMetricsRequest;
class MetricDataOutput {
    metric_type;
    metric_name;
    value;
    constructor(metric_type, metric_name, value) {
        this.metric_type = metric_type;
        this.metric_name = metric_name;
        this.value = value;
    }
}
exports.MetricDataOutput = MetricDataOutput;
class GetMetricsResponse extends BaseResponse {
    metrics;
    constructor(request_id, metrics, error_message) {
        super(MessageType.GET_METRICS, request_id, error_message);
        this.metrics = metrics;
    }
}
exports.GetMetricsResponse = GetMetricsResponse;
class ExportWorkflowRequest {
    type = MessageType.EXPORT_WORKFLOW;
    request_id;
    workflow_id;
    export_children;
    constructor(request_id, workflow_id, export_children = false) {
        this.request_id = request_id;
        this.workflow_id = workflow_id;
        this.export_children = export_children;
    }
}
exports.ExportWorkflowRequest = ExportWorkflowRequest;
class ExportWorkflowResponse extends BaseResponse {
    serialized_workflow;
    constructor(request_id, serialized_workflow, error_message) {
        super(MessageType.EXPORT_WORKFLOW, request_id, error_message);
        this.serialized_workflow = serialized_workflow;
    }
}
exports.ExportWorkflowResponse = ExportWorkflowResponse;
class ImportWorkflowRequest {
    type = MessageType.IMPORT_WORKFLOW;
    request_id;
    serialized_workflow;
    constructor(request_id, serialized_workflow) {
        this.request_id = request_id;
        this.serialized_workflow = serialized_workflow;
    }
}
exports.ImportWorkflowRequest = ImportWorkflowRequest;
class ImportWorkflowResponse extends BaseResponse {
    success;
    constructor(request_id, success, error_message) {
        super(MessageType.IMPORT_WORKFLOW, request_id, error_message);
        this.success = success;
    }
}
exports.ImportWorkflowResponse = ImportWorkflowResponse;
class AlertResponse extends BaseResponse {
    success;
    constructor(request_id, success, error_message) {
        super(MessageType.ALERT, request_id, error_message);
        this.success = success;
    }
}
exports.AlertResponse = AlertResponse;
class ListSchedulesRequest {
    type = MessageType.LIST_SCHEDULES;
    request_id;
    body;
    constructor(request_id, body) {
        this.request_id = request_id;
        this.body = body;
    }
}
exports.ListSchedulesRequest = ListSchedulesRequest;
class ListSchedulesResponse extends BaseResponse {
    output;
    constructor(request_id, output, error_message) {
        super(MessageType.LIST_SCHEDULES, request_id, error_message);
        this.output = output;
    }
}
exports.ListSchedulesResponse = ListSchedulesResponse;
class GetScheduleRequest {
    type = MessageType.GET_SCHEDULE;
    request_id;
    schedule_name;
    load_context;
    constructor(request_id, schedule_name) {
        this.request_id = request_id;
        this.schedule_name = schedule_name;
    }
}
exports.GetScheduleRequest = GetScheduleRequest;
class GetScheduleResponse extends BaseResponse {
    output;
    constructor(request_id, output, error_message) {
        super(MessageType.GET_SCHEDULE, request_id, error_message);
        this.output = output;
    }
}
exports.GetScheduleResponse = GetScheduleResponse;
class PauseScheduleRequest {
    type = MessageType.PAUSE_SCHEDULE;
    request_id;
    schedule_name;
    constructor(request_id, schedule_name) {
        this.request_id = request_id;
        this.schedule_name = schedule_name;
    }
}
exports.PauseScheduleRequest = PauseScheduleRequest;
class PauseScheduleResponse extends BaseResponse {
    success;
    constructor(request_id, success, error_message) {
        super(MessageType.PAUSE_SCHEDULE, request_id, error_message);
        this.success = success;
    }
}
exports.PauseScheduleResponse = PauseScheduleResponse;
class ResumeScheduleRequest {
    type = MessageType.RESUME_SCHEDULE;
    request_id;
    schedule_name;
    constructor(request_id, schedule_name) {
        this.request_id = request_id;
        this.schedule_name = schedule_name;
    }
}
exports.ResumeScheduleRequest = ResumeScheduleRequest;
class ResumeScheduleResponse extends BaseResponse {
    success;
    constructor(request_id, success, error_message) {
        super(MessageType.RESUME_SCHEDULE, request_id, error_message);
        this.success = success;
    }
}
exports.ResumeScheduleResponse = ResumeScheduleResponse;
class TriggerScheduleRequest {
    type = MessageType.TRIGGER_SCHEDULE;
    request_id;
    schedule_name;
    constructor(request_id, schedule_name) {
        this.request_id = request_id;
        this.schedule_name = schedule_name;
    }
}
exports.TriggerScheduleRequest = TriggerScheduleRequest;
class TriggerScheduleResponse extends BaseResponse {
    workflow_id;
    constructor(request_id, workflow_id, error_message) {
        super(MessageType.TRIGGER_SCHEDULE, request_id, error_message);
        this.workflow_id = workflow_id;
    }
}
exports.TriggerScheduleResponse = TriggerScheduleResponse;
class BackfillScheduleRequest {
    type = MessageType.BACKFILL_SCHEDULE;
    request_id;
    schedule_name;
    start;
    end;
    constructor(request_id, schedule_name, start, end) {
        this.request_id = request_id;
        this.schedule_name = schedule_name;
        this.start = start;
        this.end = end;
    }
}
exports.BackfillScheduleRequest = BackfillScheduleRequest;
class BackfillScheduleResponse extends BaseResponse {
    workflow_ids;
    constructor(request_id, workflow_ids, error_message) {
        super(MessageType.BACKFILL_SCHEDULE, request_id, error_message);
        this.workflow_ids = workflow_ids;
    }
}
exports.BackfillScheduleResponse = BackfillScheduleResponse;
class ListApplicationVersionsRequest {
    type = MessageType.LIST_APPLICATION_VERSIONS;
    request_id;
    constructor(request_id) {
        this.request_id = request_id;
    }
}
exports.ListApplicationVersionsRequest = ListApplicationVersionsRequest;
class ListApplicationVersionsResponse extends BaseResponse {
    output;
    constructor(request_id, output, error_message) {
        super(MessageType.LIST_APPLICATION_VERSIONS, request_id, error_message);
        this.output = output;
    }
}
exports.ListApplicationVersionsResponse = ListApplicationVersionsResponse;
class SetLatestApplicationVersionRequest {
    type = MessageType.SET_LATEST_APPLICATION_VERSION;
    request_id;
    version_name;
    constructor(request_id, version_name) {
        this.request_id = request_id;
        this.version_name = version_name;
    }
}
exports.SetLatestApplicationVersionRequest = SetLatestApplicationVersionRequest;
class SetLatestApplicationVersionResponse extends BaseResponse {
    success;
    constructor(request_id, success, error_message) {
        super(MessageType.SET_LATEST_APPLICATION_VERSION, request_id, error_message);
        this.success = success;
    }
}
exports.SetLatestApplicationVersionResponse = SetLatestApplicationVersionResponse;
class GetWorkflowEventsRequest {
    type = MessageType.GET_WORKFLOW_EVENTS;
    request_id;
    workflow_id;
    constructor(request_id, workflow_id) {
        this.request_id = request_id;
        this.workflow_id = workflow_id;
    }
}
exports.GetWorkflowEventsRequest = GetWorkflowEventsRequest;
class GetWorkflowEventsResponse extends BaseResponse {
    events;
    constructor(request_id, events, error_message) {
        super(MessageType.GET_WORKFLOW_EVENTS, request_id, error_message);
        this.events = events;
    }
}
exports.GetWorkflowEventsResponse = GetWorkflowEventsResponse;
class GetWorkflowNotificationsRequest {
    type = MessageType.GET_WORKFLOW_NOTIFICATIONS;
    request_id;
    workflow_id;
    constructor(request_id, workflow_id) {
        this.request_id = request_id;
        this.workflow_id = workflow_id;
    }
}
exports.GetWorkflowNotificationsRequest = GetWorkflowNotificationsRequest;
class GetWorkflowNotificationsResponse extends BaseResponse {
    notifications;
    constructor(request_id, notifications, error_message) {
        super(MessageType.GET_WORKFLOW_NOTIFICATIONS, request_id, error_message);
        this.notifications = notifications;
    }
}
exports.GetWorkflowNotificationsResponse = GetWorkflowNotificationsResponse;
class GetWorkflowStreamsRequest {
    type = MessageType.GET_WORKFLOW_STREAMS;
    request_id;
    workflow_id;
    constructor(request_id, workflow_id) {
        this.request_id = request_id;
        this.workflow_id = workflow_id;
    }
}
exports.GetWorkflowStreamsRequest = GetWorkflowStreamsRequest;
class GetWorkflowStreamsResponse extends BaseResponse {
    streams;
    constructor(request_id, streams, error_message) {
        super(MessageType.GET_WORKFLOW_STREAMS, request_id, error_message);
        this.streams = streams;
    }
}
exports.GetWorkflowStreamsResponse = GetWorkflowStreamsResponse;
class GetWorkflowAggregatesRequest {
    type = MessageType.GET_WORKFLOW_AGGREGATES;
    request_id;
    body;
    constructor(request_id, body) {
        this.request_id = request_id;
        this.body = body;
    }
}
exports.GetWorkflowAggregatesRequest = GetWorkflowAggregatesRequest;
class GetWorkflowAggregatesResponse extends BaseResponse {
    output;
    constructor(request_id, output, error_message) {
        super(MessageType.GET_WORKFLOW_AGGREGATES, request_id, error_message);
        this.output = output;
    }
}
exports.GetWorkflowAggregatesResponse = GetWorkflowAggregatesResponse;
class GetStepAggregatesRequest {
    type = MessageType.GET_STEP_AGGREGATES;
    request_id;
    body;
    constructor(request_id, body) {
        this.request_id = request_id;
        this.body = body;
    }
}
exports.GetStepAggregatesRequest = GetStepAggregatesRequest;
class GetStepAggregatesResponse extends BaseResponse {
    output;
    constructor(request_id, output, error_message) {
        super(MessageType.GET_STEP_AGGREGATES, request_id, error_message);
        this.output = output;
    }
}
exports.GetStepAggregatesResponse = GetStepAggregatesResponse;
class ListQueuesRequest {
    type = MessageType.LIST_QUEUES;
    request_id;
    constructor(request_id) {
        this.request_id = request_id;
    }
}
exports.ListQueuesRequest = ListQueuesRequest;
class ListQueuesResponse extends BaseResponse {
    output;
    constructor(request_id, output, error_message) {
        super(MessageType.LIST_QUEUES, request_id, error_message);
        this.output = output;
    }
}
exports.ListQueuesResponse = ListQueuesResponse;
class GetQueueRequest {
    type = MessageType.GET_QUEUE;
    request_id;
    name;
    constructor(request_id, name) {
        this.request_id = request_id;
        this.name = name;
    }
}
exports.GetQueueRequest = GetQueueRequest;
class GetQueueResponse extends BaseResponse {
    output;
    constructor(request_id, output, error_message) {
        super(MessageType.GET_QUEUE, request_id, error_message);
        this.output = output;
    }
}
exports.GetQueueResponse = GetQueueResponse;
//# sourceMappingURL=protocol.js.map