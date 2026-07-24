"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.globalTimeout = exports.toWorkflowStatus = exports.forkWorkflow = exports.listWorkflowSteps = exports.getWorkflow = exports.listQueuedWorkflows = exports.listWorkflows = void 0;
const serialization_1 = require("./serialization");
const node_crypto_1 = require("node:crypto");
async function listWorkflows(sysdb, input) {
    const workflows = await sysdb.listWorkflows(input);
    return await Promise.all(workflows.map((wf) => toWorkflowStatus(wf, sysdb.getSerializer())));
}
exports.listWorkflows = listWorkflows;
async function listQueuedWorkflows(sysdb, input) {
    input.queuesOnly = true;
    input.loadOutput = false;
    const workflows = await sysdb.listWorkflows(input);
    return await Promise.all(workflows.map((wf) => toWorkflowStatus(wf, sysdb.getSerializer())));
}
exports.listQueuedWorkflows = listQueuedWorkflows;
async function getWorkflow(sysdb, workflowID) {
    const status = await sysdb.getWorkflowStatus(workflowID);
    return status ? await toWorkflowStatus(status, sysdb.getSerializer()) : undefined;
}
exports.getWorkflow = getWorkflow;
async function listWorkflowSteps(sysdb, workflowID, loadOutput = true, options) {
    const status = await sysdb.getWorkflowStatus(workflowID);
    if (!status) {
        return undefined;
    }
    const $steps = await sysdb.getAllOperationResults(workflowID, options?.limit, options?.offset);
    const steps = await Promise.all($steps.map(async (step) => ({
        functionID: step.function_id,
        name: step.function_name ?? '',
        output: loadOutput && step.output ? await (0, serialization_1.safeParse)(sysdb.getSerializer(), step.output, step.serialization) : null,
        error: loadOutput && step.error ? await (0, serialization_1.safeParseError)(sysdb.getSerializer(), step.error, step.serialization) : null,
        childWorkflowID: step.child_workflow_id,
        startedAtEpochMs: step.started_at_epoch_ms ? Number(step.started_at_epoch_ms) : undefined,
        completedAtEpochMs: step.completed_at_epoch_ms ? Number(step.completed_at_epoch_ms) : undefined,
    })));
    return steps.toSorted((a, b) => a.functionID - b.functionID);
}
exports.listWorkflowSteps = listWorkflowSteps;
async function forkWorkflow(sysdb, workflowID, startStep, options = {}) {
    const newWorkflowID = options.newWorkflowID ?? (0, node_crypto_1.randomUUID)();
    await sysdb.forkWorkflow(workflowID, startStep, { ...options, newWorkflowID });
    return newWorkflowID;
}
exports.forkWorkflow = forkWorkflow;
async function toWorkflowStatus(internal, serializer) {
    return {
        workflowID: internal.workflowUUID,
        status: internal.status,
        workflowName: internal.workflowName,
        workflowClassName: internal.workflowClassName,
        workflowConfigName: internal.workflowConfigName,
        queueName: internal.queueName,
        authenticatedUser: internal.authenticatedUser,
        assumedRole: internal.assumedRole,
        authenticatedRoles: internal.authenticatedRoles,
        input: internal.input
            ? (await (0, serialization_1.safeParsePositionalArgs)(serializer, internal.input, internal.serialization))
            : undefined,
        output: internal.output ? await (0, serialization_1.safeParse)(serializer, internal.output ?? null, internal.serialization) : undefined,
        error: internal.error ? await (0, serialization_1.safeParseError)(serializer, internal.error, internal.serialization) : undefined,
        request: internal.request,
        executorId: internal.executorId,
        applicationVersion: internal.applicationVersion,
        applicationID: internal.applicationID,
        recoveryAttempts: internal.recoveryAttempts,
        createdAt: internal.createdAt,
        updatedAt: internal.updatedAt,
        timeoutMS: internal.timeoutMS,
        deadlineEpochMS: internal.deadlineEpochMS,
        deduplicationID: internal.deduplicationID,
        priority: internal.priority,
        queuePartitionKey: internal.queuePartitionKey,
        dequeuedAt: internal.startedAtEpochMs,
        forkedFrom: internal.forkedFrom,
        wasForkedFrom: internal.wasForkedFrom ?? false,
        parentWorkflowID: internal.parentWorkflowID,
        delayUntilEpochMS: internal.delayUntilEpochMS,
        completedAt: internal.completedAt,
        attributes: internal.attributes,
        scheduleName: internal.scheduleName,
    };
}
exports.toWorkflowStatus = toWorkflowStatus;
async function globalTimeout(sysdb, cutoffEpochTimestampMs) {
    const cutoffIso = new Date(cutoffEpochTimestampMs).toISOString();
    for (const workflow of await listWorkflows(sysdb, { status: 'PENDING', endTime: cutoffIso })) {
        await sysdb.cancelWorkflows([workflow.workflowID]);
    }
    for (const workflow of await listWorkflows(sysdb, { status: 'ENQUEUED', endTime: cutoffIso })) {
        await sysdb.cancelWorkflows([workflow.workflowID]);
    }
    for (const workflow of await listWorkflows(sysdb, { status: 'DELAYED', endTime: cutoffIso })) {
        await sysdb.cancelWorkflows([workflow.workflowID]);
    }
}
exports.globalTimeout = globalTimeout;
//# sourceMappingURL=workflow_management.js.map