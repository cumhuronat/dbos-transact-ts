"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.getQueue = exports.getOrCreateQueue = exports.registerPollerQueue = exports.enqueueWorkflows = exports.prepareEnqueuedWorkflow = void 0;
/**
 * Entry point for event receivers (Kafka, SQS, ...) that live in their own packages.
 *
 * This is not part of the user-facing API: it exists so a receiver can reach the internals it
 * needs — chiefly durable batch enqueue — without those internals landing on the `DBOS` class.
 */
const dbos_executor_1 = require("./dbos-executor");
const decorators_1 = require("./decorators");
const wfqueue_1 = require("./wfqueue");
/**
 * Build, without persisting, an ENQUEUED row for `workflow`, to be durably enqueued in bulk by
 * {@link enqueueWorkflows}. Together they let a receiver enqueue a batch of workflows in one
 * transaction instead of one transaction per workflow.
 *
 * Any ambient DBOS context is ignored: the row inherits no parent, auth, or attributes.
 */
async function prepareEnqueuedWorkflow(workflow, args, options) {
    (0, decorators_1.ensureDBOSIsLaunched)('prepareEnqueuedWorkflow');
    return await dbos_executor_1.DBOSExecutor.globalInstance.prepareEnqueuedWorkflow(workflow, args, options);
}
exports.prepareEnqueuedWorkflow = prepareEnqueuedWorkflow;
/**
 * Durably enqueue a batch of workflows built by {@link prepareEnqueuedWorkflow}, in a single
 * transaction. Workflows whose ID already exists are skipped rather than updated, so redelivering
 * the same batch is a no-op and each workflow runs exactly once.
 *
 * Throws rather than retrying if the database is unreachable, so the caller keeps control: retry
 * the same batch until it succeeds, and commit nothing to the source until it does.
 *
 * @returns The IDs of the workflows actually enqueued by this call.
 */
async function enqueueWorkflows(workflows) {
    (0, decorators_1.ensureDBOSIsLaunched)('enqueueWorkflows');
    return await dbos_executor_1.DBOSExecutor.globalInstance.systemDatabase.enqueueWorkflows(workflows);
}
exports.enqueueWorkflows = enqueueWorkflows;
/**
 * Mark a queue as fed by this process's own poller (e.g. a Kafka consumer), so it is always
 * dispatched even when a `listenQueues` filter names only other queues. Without this, workflows
 * the poller enqueues would sit ENQUEUED forever.
 *
 * Must be called before `DBOS.launch`, when the queue dispatcher takes its snapshot.
 */
function registerPollerQueue(name) {
    wfqueue_1.wfQueueRunner.pollerQueueNames.add(name);
}
exports.registerPollerQueue = registerPollerQueue;
/**
 * Get the in-memory queue registered under `name` in this process, creating it if there is none.
 *
 * A receiver must resolve its internal queues through this rather than caching them: a registry
 * clear (`DBOS.shutdown({ deregister: true })`) drops the registration, and a cached queue would
 * silently stop being dispatched, leaving its workflows ENQUEUED forever.
 */
function getOrCreateQueue(name, params = {}) {
    return wfqueue_1.wfQueueRunner.wfQueuesByName.get(name) ?? new wfqueue_1.WorkflowQueue(name, params);
}
exports.getOrCreateQueue = getOrCreateQueue;
/**
 * Look up a queue by name: an in-memory queue registered in this process if there is one,
 * otherwise a database-backed queue. Returns `null` if neither exists.
 */
async function getQueue(name) {
    const inMemory = wfqueue_1.wfQueueRunner.wfQueuesByName.get(name);
    if (inMemory)
        return inMemory;
    (0, decorators_1.ensureDBOSIsLaunched)('getQueue');
    const record = await dbos_executor_1.DBOSExecutor.globalInstance.systemDatabase.getQueue(name);
    return record === null ? null : wfqueue_1.WorkflowQueue._fromRecord(record);
}
exports.getQueue = getQueue;
//# sourceMappingURL=eventreceiver.js.map