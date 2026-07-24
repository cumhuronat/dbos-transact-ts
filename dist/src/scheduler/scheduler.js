"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.backfillSchedule = exports.triggerSchedule = exports.DynamicSchedulerLoop = exports.createScheduleId = exports.toWorkflowSchedule = void 0;
const serialization_1 = require("../serialization");
const crypto_1 = require("crypto");
const __1 = require("..");
const utils_1 = require("../utils");
const crontab_1 = require("./crontab");
const dbos_executor_1 = require("../dbos-executor");
const error_1 = require("../error");
const workflow_1 = require("../workflow");
async function toWorkflowSchedule(internal, serializer) {
    const context = await serializer.parse(internal.context);
    return {
        scheduleId: internal.scheduleId,
        scheduleName: internal.scheduleName,
        workflowName: internal.workflowName,
        workflowClassName: internal.workflowClassName,
        schedule: internal.schedule,
        status: internal.status,
        context,
        lastFiredAt: internal.lastFiredAt,
        automaticBackfill: internal.automaticBackfill,
        cronTimezone: internal.cronTimezone,
        queueName: internal.queueName,
    };
}
exports.toWorkflowSchedule = toWorkflowSchedule;
function createScheduleId() {
    return (0, crypto_1.randomUUID)();
}
exports.createScheduleId = createScheduleId;
// Definition fields a running loop captures at start; a change requires restarting it. Runtime state (schedule_id, status, last_fired_at) is excluded so an unchanged re-apply doesn't restart the loop.
function scheduleSignature(sched) {
    return JSON.stringify([
        sched.workflowName,
        sched.workflowClassName,
        sched.schedule,
        sched.context,
        sched.cronTimezone ?? null,
        sched.queueName ?? null,
    ]);
}
// During the first minute after startup, poll for schedules every second so schedules
// registered around launch are picked up promptly rather than after a full polling interval.
const STARTUP_FAST_POLL_DURATION_MS = 60_000;
const STARTUP_FAST_POLL_INTERVAL_MS = 1_000;
class DynamicSchedulerLoop {
    #mainController = new AbortController();
    #pollingPromise;
    #scheduleLoops = new Map();
    #pollingIntervalMs;
    constructor(pollingIntervalMs) {
        this.#pollingIntervalMs = pollingIntervalMs ?? 30000;
        __1.DBOS.registerLifecycleCallback(this);
    }
    async initialize() {
        this.#pollingPromise = this.#pollingLoop(this.#mainController.signal);
        await Promise.resolve();
    }
    async destroy() {
        this.#mainController.abort();
        // Abort all per-schedule loops
        for (const entry of this.#scheduleLoops.values()) {
            entry.controller.abort();
        }
        const allPromises = [];
        if (this.#pollingPromise) {
            allPromises.push(this.#pollingPromise);
        }
        for (const entry of this.#scheduleLoops.values()) {
            allPromises.push(entry.promise);
        }
        await Promise.allSettled(allPromises);
        this.#scheduleLoops.clear();
    }
    async #pollingLoop(signal) {
        const startupDeadline = Date.now() + STARTUP_FAST_POLL_DURATION_MS;
        const pollTimeout = () => Date.now() < startupDeadline
            ? Math.min(STARTUP_FAST_POLL_INTERVAL_MS, this.#pollingIntervalMs)
            : this.#pollingIntervalMs;
        while (!signal.aborted) {
            let schedules;
            try {
                const executor = dbos_executor_1.DBOSExecutor.globalInstance;
                schedules = await executor.systemDatabase.listSchedules();
            }
            catch (e) {
                __1.DBOS.logger.warn(`Dynamic scheduler: error listing schedules: ${e.message}`);
                await (0, utils_1.interruptibleSleep)(pollTimeout(), signal);
                continue;
            }
            // Build set of current schedule names
            const currentNames = new Set(schedules.map((s) => s.scheduleName));
            // Stop loops for deleted schedules
            for (const [name, entry] of this.#scheduleLoops) {
                if (!currentNames.has(name)) {
                    entry.controller.abort();
                    this.#scheduleLoops.delete(name);
                }
            }
            // Process each schedule
            for (const sched of schedules) {
                const existing = this.#scheduleLoops.get(sched.scheduleName);
                const signature = scheduleSignature(sched);
                if (sched.status === 'PAUSED' && existing) {
                    // Paused but has a running loop — stop it
                    existing.controller.abort();
                    this.#scheduleLoops.delete(sched.scheduleName);
                }
                else if (sched.status === 'ACTIVE') {
                    // If the schedule's definition changed, restart the loop so it fires with the new definition.
                    if (existing && existing.signature !== signature) {
                        existing.controller.abort();
                        this.#scheduleLoops.delete(sched.scheduleName);
                    }
                    if (!this.#scheduleLoops.has(sched.scheduleName)) {
                        // Automatic backfill: if enabled and lastFiredAt is set,
                        // backfill missed executions before starting the thread.
                        if (sched.automaticBackfill && sched.lastFiredAt) {
                            try {
                                const lastFired = new Date(sched.lastFiredAt);
                                const now = new Date();
                                if (lastFired < now) {
                                    const executor = dbos_executor_1.DBOSExecutor.globalInstance;
                                    await backfillSchedule(executor.systemDatabase, executor.serializer, sched.scheduleName, lastFired, now);
                                }
                            }
                            catch (e) {
                                __1.DBOS.logger.warn(`Dynamic scheduler: error backfilling schedule "${sched.scheduleName}": ${e.message}`);
                            }
                        }
                        // Active and no running loop — start one
                        const controller = new AbortController();
                        const executor = dbos_executor_1.DBOSExecutor.globalInstance;
                        const promise = DynamicSchedulerLoop.#scheduleLoop(sched.scheduleName, sched.workflowName, sched.workflowClassName, sched.schedule, sched.context, executor.serializer, controller.signal, sched.cronTimezone ?? undefined, sched.queueName ?? undefined);
                        this.#scheduleLoops.set(sched.scheduleName, { controller, promise, signature });
                    }
                }
            }
            await (0, utils_1.interruptibleSleep)(pollTimeout(), signal);
        }
    }
    static async #scheduleLoop(scheduleName, workflowName, workflowClassName, cronExpression, serializedContext, serializer, signal, cronTimezone, queueName) {
        const timeMatcher = new crontab_1.TimeMatcher(cronExpression, cronTimezone);
        const sched = {
            scheduleId: '',
            scheduleName,
            workflowName,
            workflowClassName,
            schedule: cronExpression,
            status: 'ACTIVE',
            context: serializedContext,
            lastFiredAt: null,
            automaticBackfill: false,
            cronTimezone: cronTimezone ?? null,
            queueName: queueName ?? null,
        };
        let lastExec = new Date().setMilliseconds(0);
        while (!signal.aborted) {
            const nextExec = timeMatcher.nextWakeupTime(lastExec).getTime();
            let sleepTime = nextExec - Date.now();
            // Apply jitter to prevent thundering herd
            if (sleepTime > 0) {
                const maxJitter = Math.min(sleepTime / 10, 10000);
                sleepTime += Math.random() * maxJitter;
            }
            if (sleepTime > 0) {
                await (0, utils_1.interruptibleSleep)(sleepTime, signal);
            }
            if (signal.aborted) {
                break;
            }
            // If TimeMatcher did not find the next occurrence of the schedule yet,
            // we must have it determine the next wakeup time.
            if (!timeMatcher.match(nextExec)) {
                lastExec = nextExec;
                continue;
            }
            const date = new Date(nextExec);
            const workflowID = `sched-${scheduleName}-${date.toISOString()}`;
            try {
                // Idempotency check -- for performance only, not needed for correctness
                const existing = await __1.DBOS.getWorkflowStatus(workflowID);
                if (existing) {
                    lastExec = nextExec;
                    continue;
                }
                const context = await serializer.parse(serializedContext);
                const systemDatabase = dbos_executor_1.DBOSExecutor.globalInstance.systemDatabase;
                await enqueueScheduledWorkflow(systemDatabase, serializer, sched, workflowID, date, context);
                await systemDatabase.updateLastFiredAt(scheduleName, date.toISOString());
            }
            catch (e) {
                __1.DBOS.logger.warn(`Dynamic scheduler: error firing workflow for schedule "${scheduleName}": ${e.message}`);
            }
            lastExec = nextExec;
        }
    }
}
exports.DynamicSchedulerLoop = DynamicSchedulerLoop;
async function enqueueScheduledWorkflow(systemDatabase, serializer, sched, workflowID, scheduledDate, context) {
    const serparam = await (0, serialization_1.serializeArgs)([scheduledDate, context], undefined, serializer, undefined);
    // Always enqueue scheduled workflows to the latest application version
    const latestVersion = await __1.DBOS.getLatestApplicationVersion();
    const internalStatus = {
        workflowUUID: workflowID,
        status: workflow_1.StatusString.ENQUEUED,
        workflowName: sched.workflowName,
        workflowClassName: sched.workflowClassName,
        workflowConfigName: '',
        applicationVersion: latestVersion.versionName,
        queueName: sched.queueName || utils_1.INTERNAL_QUEUE_NAME,
        authenticatedUser: '',
        output: null,
        error: null,
        assumedRole: '',
        authenticatedRoles: [],
        request: {},
        executorId: '',
        applicationID: '',
        createdAt: Date.now(),
        input: serparam.serializedValue,
        deduplicationID: undefined,
        priority: 0,
        queuePartitionKey: undefined,
        serialization: serparam.serialization,
        scheduleName: sched.scheduleName,
    };
    await systemDatabase.initWorkflowStatus(internalStatus, null);
    return;
}
async function triggerSchedule(systemDatabase, serializer, name) {
    const sched = await systemDatabase.getSchedule(name);
    if (!sched) {
        throw new error_1.DBOSError(`Schedule "${name}" not found`);
    }
    const context = await serializer.parse(sched.context);
    const now = new Date();
    const workflowID = `sched-${name}-trigger-${now.toISOString()}`;
    await enqueueScheduledWorkflow(systemDatabase, serializer, sched, workflowID, now, context);
    return workflowID;
}
exports.triggerSchedule = triggerSchedule;
async function backfillSchedule(systemDatabase, serializer, name, start, end) {
    const sched = await systemDatabase.getSchedule(name);
    if (!sched) {
        throw new error_1.DBOSError(`Schedule "${name}" not found`);
    }
    const context = await serializer.parse(sched.context);
    const timeMatcher = new crontab_1.TimeMatcher(sched.schedule, sched.cronTimezone ?? undefined);
    const workflowIDs = [];
    let current = start.getTime();
    while (current < end.getTime()) {
        const next = timeMatcher.nextWakeupTime(current);
        // If TimeMatcher did not find the next occurrence of the schedule yet,
        // we must have it determine the next wakeup time.
        if (!timeMatcher.match(next)) {
            current = next.getTime();
            continue;
        }
        if (next.getTime() >= end.getTime())
            break;
        const workflowID = `sched-${name}-${next.toISOString()}`;
        await enqueueScheduledWorkflow(systemDatabase, serializer, sched, workflowID, next, context);
        workflowIDs.push(workflowID);
        current = next.getTime();
    }
    return workflowIDs;
}
exports.backfillSchedule = backfillSchedule;
//# sourceMappingURL=scheduler.js.map