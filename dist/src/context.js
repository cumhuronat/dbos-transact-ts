"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.runInStepContext = exports.runWithDataSourceContext = exports.runWithParentContext = exports.runWithTopContext = exports.functionIDGet = exports.functionIDGetIncrement = exports.getNextWFID = exports.getCurrentContextStore = exports.isInWorkflowCtx = void 0;
const async_hooks_1 = require("async_hooks");
const error_1 = require("./error");
const dbos_executor_1 = require("./dbos-executor");
function isWithinWorkflowCtx(ctx) {
    if (ctx.workflowId === undefined)
        return false;
    return true;
}
function isInStepCtx(ctx) {
    if (ctx.workflowId === undefined)
        return false;
    if (ctx.curStepFunctionId)
        return true;
    return false;
}
function isInTxnCtx(ctx) {
    if (ctx.workflowId === undefined)
        return false;
    if (ctx.curTxFunctionId)
        return true;
    return false;
}
function isInWorkflowCtx(ctx) {
    if (!isWithinWorkflowCtx(ctx))
        return false;
    if (isInStepCtx(ctx))
        return false;
    if (isInTxnCtx(ctx))
        return false;
    return true;
}
exports.isInWorkflowCtx = isInWorkflowCtx;
const asyncLocalCtx = new async_hooks_1.AsyncLocalStorage();
function getCurrentContextStore() {
    return asyncLocalCtx.getStore();
}
exports.getCurrentContextStore = getCurrentContextStore;
function getNextWFID(assignedID) {
    let wfId = assignedID;
    if (!wfId) {
        const pctx = getCurrentContextStore();
        const nextID = pctx?.idAssignedForNextWorkflow;
        if (nextID) {
            wfId = nextID;
            pctx.idAssignedForNextWorkflow = undefined;
        }
    }
    return wfId;
}
exports.getNextWFID = getNextWFID;
function functionIDGetIncrement() {
    const pctx = getCurrentContextStore();
    if (!pctx)
        throw new error_1.DBOSInvalidWorkflowTransitionError(`Attempt to get a call ID number outside of a workflow`);
    if (!isInWorkflowCtx(pctx))
        throw new error_1.DBOSInvalidWorkflowTransitionError(`Attempt to get a call ID number in a workflow that is already in a call`);
    if (pctx.curWFFunctionId === undefined)
        pctx.curWFFunctionId = 0;
    return pctx.curWFFunctionId++;
}
exports.functionIDGetIncrement = functionIDGetIncrement;
function functionIDGet() {
    const pctx = getCurrentContextStore();
    if (!pctx)
        throw new error_1.DBOSInvalidWorkflowTransitionError(`Attempt to get a call ID number outside of a workflow`);
    if (!isInWorkflowCtx(pctx))
        throw new error_1.DBOSInvalidWorkflowTransitionError(`Attempt to get a call ID number in a workflow that is already in a call`);
    if (pctx.curWFFunctionId === undefined)
        pctx.curWFFunctionId = 0;
    return pctx.curWFFunctionId;
}
exports.functionIDGet = functionIDGet;
async function runWithTopContext(ctx, callback) {
    return await asyncLocalCtx.run(ctx, callback);
}
exports.runWithTopContext = runWithTopContext;
async function runWithParentContext(pctx, ctx, callback) {
    return await asyncLocalCtx.run({
        ...pctx,
        ...ctx,
        parentCtx: pctx,
    }, callback);
}
exports.runWithParentContext = runWithParentContext;
async function runWithDataSourceContext(callnum, callback) {
    // Check we are in a workflow context and not in a step / transaction already
    const pctx = getCurrentContextStore() ?? {};
    return await asyncLocalCtx.run({
        ...pctx,
        curTxFunctionId: callnum,
        parentCtx: pctx,
        logger: dbos_executor_1.DBOSExecutor.globalInstance.ctxLogger,
    }, callback);
}
exports.runWithDataSourceContext = runWithDataSourceContext;
async function runInStepContext(pctx, stepID, maxAttempts, currentAttempt, timeoutSignal, callback) {
    // Check we are in a workflow context and not in a step / transaction already
    if (!pctx)
        throw new error_1.DBOSInvalidWorkflowTransitionError();
    if (!isInWorkflowCtx(pctx))
        throw new error_1.DBOSInvalidWorkflowTransitionError();
    const stepStatus = {
        stepID: stepID,
        currentAttempt: currentAttempt,
        maxAttempts: currentAttempt ? maxAttempts : undefined,
        timeoutSignal: timeoutSignal,
    };
    return await runWithParentContext(pctx, {
        stepStatus: stepStatus,
        curStepFunctionId: stepID,
        parentCtx: pctx,
        logger: dbos_executor_1.DBOSExecutor.globalInstance.ctxLogger,
    }, callback);
}
exports.runInStepContext = runInStepContext;
//# sourceMappingURL=context.js.map