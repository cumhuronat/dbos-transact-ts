"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.DEBUG_TRIGGER_FIND_AND_MARK_AFTER_SELECT = exports.DEBUG_TRIGGER_BETWEEN_PARTITION_DISPATCHES = exports.DEBUG_TRIGGER_INITWF_COMMIT = exports.DEBUG_TRIGGER_STEP_COMMIT = exports.DEBUG_TRIGGER_WORKFLOW_ENQUEUE = exports.DEBUG_TRIGGER_WORKFLOW_QUEUE_START = exports.clearDebugTriggers = exports.setDebugTrigger = exports.debugTriggerPoint = exports.pointLocations = exports.pointTriggers = exports.DebugAction = exports.getCallSiteInfo = void 0;
const utils_1 = require("./utils");
function getCallSiteInfo() {
    const err = new Error();
    const stack = err.stack?.split('\n');
    if (stack && stack.length > 2) {
        // The third line usually contains the callsite information.
        // Different environments (Node, browser) format the stack trace differently.
        // Adjust the regex to your environment as needed.
        const match = stack[2].match(/at\s+(.*):(\d+):(\d+)/);
        if (match) {
            const fileName = match[1];
            const lineNumber = parseInt(match[2], 10);
            return { fileName, lineNumber };
        }
    }
    return { fileName: 'unknown', lineNumber: -1 };
}
exports.getCallSiteInfo = getCallSiteInfo;
class DebugAction {
    sleepms; // Sleep at point
    awaitEvent; // Wait at point
    callback;
    asyncCallback;
}
exports.DebugAction = DebugAction;
exports.pointTriggers = new Map();
exports.pointLocations = new Map();
async function debugTriggerPoint(name) {
    const cpi = getCallSiteInfo();
    if (!exports.pointLocations.has(name)) {
        exports.pointLocations.set(name, { name, ...cpi, hitCount: 0 });
    }
    if (exports.pointTriggers.has(name)) {
        const pt = exports.pointTriggers.get(name);
        if (pt.sleepms) {
            await (0, utils_1.sleepms)(pt.sleepms);
        }
        if (pt.asyncCallback) {
            await pt.asyncCallback();
        }
        if (pt.callback) {
            pt.callback();
        }
        if (pt.awaitEvent) {
            await pt.awaitEvent;
        }
    }
}
exports.debugTriggerPoint = debugTriggerPoint;
function setDebugTrigger(name, action) {
    exports.pointTriggers.set(name, action);
}
exports.setDebugTrigger = setDebugTrigger;
function clearDebugTriggers() {
    exports.pointTriggers.clear();
}
exports.clearDebugTriggers = clearDebugTriggers;
exports.DEBUG_TRIGGER_WORKFLOW_QUEUE_START = 'DEBUG_TRIGGER_WORKFLOW_QUEUE_START';
exports.DEBUG_TRIGGER_WORKFLOW_ENQUEUE = 'DEBUG_TRIGGER_WORKFLOW_ENQUEUE';
exports.DEBUG_TRIGGER_STEP_COMMIT = 'DEBUG_TRIGGER_STEP_COMMIT';
exports.DEBUG_TRIGGER_INITWF_COMMIT = 'DEBUG_TRIGGER_INITWF_COMMIT';
// Fires inside runQueue between dispatching consecutive partition keys.
exports.DEBUG_TRIGGER_BETWEEN_PARTITION_DISPATCHES = 'DEBUG_TRIGGER_BETWEEN_PARTITION_DISPATCHES';
// Fires inside findAndMarkStartableWorkflows after the SELECT FOR UPDATE NOWAIT
// but before COMMIT (i.e. while the row lock is held). Tests can throw a
// synthetic 55P03 here to simulate a concurrent executor winning the lock race,
// which is the exact condition that triggers the orphan-PENDING bug.
exports.DEBUG_TRIGGER_FIND_AND_MARK_AFTER_SELECT = 'DEBUG_TRIGGER_FIND_AND_MARK_AFTER_SELECT';
//# sourceMappingURL=debugpoint.js.map