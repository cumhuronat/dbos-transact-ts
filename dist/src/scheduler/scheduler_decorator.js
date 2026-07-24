"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.ScheduledReceiver = exports.SchedulerMode = void 0;
const __1 = require("..");
const utils_1 = require("../utils");
const crontab_1 = require("./crontab");
////
// Configuration
////
/**
 * Choices for scheduler mode for `@DBOS.scheduled` workflows
 */
var SchedulerMode;
(function (SchedulerMode) {
    /**
     * Using `ExactlyOncePerInterval` causes the scheduler to add "make-up work" for any
     *  schedule slots that occurred when the app was not running
     */
    SchedulerMode["ExactlyOncePerInterval"] = "ExactlyOncePerInterval";
    /**
     * Using `ExactlyOncePerIntervalWhenActive` causes the scheduler to run the workflow once
     *  per interval when the application is active.  If the app is not running at a time
     *  otherwise indicated by the schedule, no workflow will be run.
     */
    SchedulerMode["ExactlyOncePerIntervalWhenActive"] = "ExactlyOncePerIntervalWhenActive";
})(SchedulerMode || (exports.SchedulerMode = SchedulerMode = {}));
///////////////////////////
// Scheduler Management
///////////////////////////
const SCHEDULER_EVENT_SERVICE_NAME = 'dbos.scheduler';
class ScheduledReceiver {
    #controller = new AbortController();
    #disposables = new Array();
    constructor() {
        __1.DBOS.registerLifecycleCallback(this);
    }
    // eslint-disable-next-line @typescript-eslint/require-await
    async initialize() {
        for (const regOp of __1.DBOS.getAssociatedInfo(SCHEDULER_EVENT_SERVICE_NAME)) {
            if (regOp.methodReg.registeredFunction === undefined) {
                __1.DBOS.logger.warn(`Scheduled workflow ${regOp.methodReg.className}.${regOp.methodReg.name} is missing registered function; skipping`);
                continue;
            }
            const { crontab, mode, queueName } = regOp.methodConfig;
            if (!crontab) {
                __1.DBOS.logger.warn(`Scheduled workflow ${regOp.methodReg.className}.${regOp.methodReg.name} is missing crontab; skipping`);
                continue;
            }
            const timeMatcher = new crontab_1.TimeMatcher(crontab);
            const promise = ScheduledReceiver.#schedulerLoop(regOp.methodReg, timeMatcher, mode ?? SchedulerMode.ExactlyOncePerIntervalWhenActive, queueName, this.#controller.signal);
            this.#disposables.push(promise);
        }
    }
    async destroy() {
        this.#controller.abort();
        const promises = this.#disposables.splice(0);
        await Promise.allSettled(promises);
    }
    logRegisteredEndpoints() {
        __1.DBOS.logger.info('Scheduled endpoints:');
        for (const regOp of __1.DBOS.getAssociatedInfo(SCHEDULER_EVENT_SERVICE_NAME)) {
            const name = `${regOp.methodReg.className}.${regOp.methodReg.name}`;
            const { crontab, mode } = regOp.methodConfig;
            if (crontab) {
                __1.DBOS.logger.info(`    ${name} @ ${crontab}; ${mode ?? SchedulerMode.ExactlyOncePerIntervalWhenActive}`);
            }
            else {
                __1.DBOS.logger.info(`    ${name} is missing crontab; skipping`);
            }
        }
    }
    static async #schedulerLoop(methodReg, timeMatcher, mode, queueName, signal) {
        const name = `${methodReg.className}.${methodReg.name}`;
        let lastExec = new Date().setMilliseconds(0);
        if (mode === SchedulerMode.ExactlyOncePerInterval) {
            const lastState = await __1.DBOS.getEventDispatchState(SCHEDULER_EVENT_SERVICE_NAME, name, 'lastState');
            if (lastState?.value) {
                lastExec = parseFloat(lastState.value);
            }
        }
        while (!signal.aborted) {
            const nextExec = timeMatcher.nextWakeupTime(lastExec).getTime();
            let sleepTime = nextExec - Date.now();
            // To prevent a "thundering herd" problem in a distributed setting,
            // apply jitter of up to 10% the sleep time, capped at 10 seconds
            if (sleepTime > 0) {
                const maxJitter = Math.min(sleepTime / 10, 10000);
                const jitter = Math.random() * maxJitter;
                sleepTime += jitter;
            }
            if (sleepTime > 0) {
                await new Promise((resolve, reject) => {
                    // eslint-disable-next-line prefer-const
                    let timeoutID;
                    const onAbort = () => {
                        clearTimeout(timeoutID);
                        reject(new Error('Abort signal received'));
                    };
                    signal.addEventListener('abort', onAbort, { once: true });
                    if (signal.aborted) {
                        signal.removeEventListener('abort', onAbort);
                        reject(new Error('Abort signal received'));
                    }
                    timeoutID = setTimeout(() => {
                        signal.removeEventListener('abort', onAbort);
                        resolve();
                    }, sleepTime);
                });
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
            if (methodReg.workflowConfig && methodReg.registeredFunction) {
                const workflowID = `sched-${name}-${date.toISOString()}`;
                const wfParams = { workflowID, queueName: queueName ?? utils_1.INTERNAL_QUEUE_NAME };
                __1.DBOS.logger.debug(`Executing scheduled workflow ${workflowID}`);
                await __1.DBOS.startWorkflow(methodReg.registeredFunction, wfParams)(date, new Date());
            }
            else {
                __1.DBOS.logger.error(`${name} is @scheduled but not a workflow`);
            }
            lastExec = await ScheduledReceiver.#setLastExecTime(name, nextExec);
        }
    }
    static async #setLastExecTime(name, time) {
        // Record the time of the wf kicked off
        try {
            const state = {
                service: SCHEDULER_EVENT_SERVICE_NAME,
                workflowFnName: name,
                key: 'lastState',
                value: `${time}`,
                updateTime: time,
            };
            const newState = await __1.DBOS.upsertEventDispatchState(state);
            const dbTime = parseFloat(newState.value);
            if (dbTime && dbTime > time) {
                return dbTime;
            }
        }
        catch (e) {
            // This write is not strictly essential and the scheduler is often the "canary in the coal mine"
            //  We will simply continue after giving full details.
            const err = e;
            __1.DBOS.logger.warn(`Scheduler caught an error writing to system DB: ${err.message}`);
            __1.DBOS.logger.error(e);
        }
        return time;
    }
    // registerScheduled is static so it can be called before an instance is created during DBOS.launch.
    // This means we can't use the instance as the external info key for associateFunctionWithInfo below
    // or in getAssociatedInfo above...which means we can only have one scheduled receiver instance.
    // However, since this is an internal receiver, it's safe to assume there is ever only one instnace.
    static registerScheduled(func, config) {
        const { regInfo } = __1.DBOS.associateFunctionWithInfo(SCHEDULER_EVENT_SERVICE_NAME, func, {
            ctorOrProto: config.ctorOrProto,
            className: config.className,
            name: config.name ?? func.name,
        });
        const schedRegInfo = regInfo;
        schedRegInfo.crontab = config.crontab;
        schedRegInfo.mode = config.mode;
        schedRegInfo.queueName = config.queueName;
    }
}
exports.ScheduledReceiver = ScheduledReceiver;
//# sourceMappingURL=scheduler_decorator.js.map