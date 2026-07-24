"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.ensurePGDatabase = exports.dropPGDatabase = exports.connectToPGAndReportOutcome = exports.connectToPGDatabase = exports.getPGClientConfig = exports.deriveDatabaseUrl = exports.getDatabaseNameFromUrl = exports.maskDatabaseUrl = exports.isPGFailedSqlTransactionError = exports.isPGKeyConflictError = exports.isPGRetriableTransactionError = exports.createTransactionCompletionTablePG = exports.createTransactionCompletionSchemaPG = exports.checkSchemaInstallationPG = exports.PGIsolationLevel = exports.registerDataSource = exports.registerTransaction = exports.runTransaction = void 0;
const context_1 = require("./context");
const dbos_1 = require("./dbos");
const dbos_executor_1 = require("./dbos-executor");
const decorators_1 = require("./decorators");
const error_1 = require("./error");
const traces_1 = require("./telemetry/traces");
/// Calling into DBOS
/**
 * This function is to be called by `DataSourceTransactionHandler` instances,
 *   with bits of user code to be run as transactions.
 * 1. The DS validates the type of config and provides the name
 * 2. The transaction will be started inside here, with a durable sysdb checkpoint.
 * 3. The DS will in turn be called upon to run the callback in a transaction context
 * @param callback - User callback function
 * @param funcName - Function name, for recording in system DB
 * @param options - Data source name and configuration
 * @returns the return from `callback`
 */
async function runTransaction(callback, funcName, options = {}) {
    (0, decorators_1.ensureDBOSIsLaunched)('transactions');
    const dsn = options.dsName ?? '<default>';
    const ds = (0, decorators_1.getTransactionalDataSource)(dsn);
    if (!dbos_1.DBOS.isWithinWorkflow()) {
        if ((0, context_1.getNextWFID)(undefined)) {
            throw new error_1.DBOSInvalidWorkflowTransitionError(`Invalid call to transaction '${funcName}' outside of a workflow; with directive to start a workflow.`);
        }
        return await (0, context_1.runWithDataSourceContext)(0, async () => {
            return await ds.invokeTransactionFunction(options.config ?? {}, undefined, callback);
        });
    }
    if (!dbos_1.DBOS.isInWorkflow()) {
        throw new error_1.DBOSInvalidWorkflowTransitionError(`Invalid call to \`${funcName}\` inside a \`step\` or \`transaction\``);
    }
    const callnum = (0, context_1.functionIDGetIncrement)();
    const tracer = dbos_executor_1.DBOSExecutor.globalInstance.tracer;
    const span = tracer.startSpan(funcName, {
        operationUUID: dbos_1.DBOS.workflowID,
        operationType: dbos_executor_1.OperationType.TRANSACTION,
        operationName: funcName,
        authenticatedUser: dbos_1.DBOS.authenticatedUser ?? '',
        assumedRole: dbos_1.DBOS.assumedRole ?? '',
        authenticatedRoles: dbos_1.DBOS.authenticatedRoles ?? [],
        // isolationLevel: txnInfo.config.isolationLevel, // TODO: Pluggable
    }, dbos_1.DBOS.span);
    try {
        const res = await (0, traces_1.runWithTrace)(span, async () => {
            return await dbos_executor_1.DBOSExecutor.globalInstance.runInternalStep(async () => {
                return await (0, context_1.runWithDataSourceContext)(callnum, async () => {
                    return await ds.invokeTransactionFunction(options.config ?? {}, undefined, callback);
                });
            }, funcName, 
            // we can be sure workflowID is set because of previous call to assertCurrentWorkflowContext
            dbos_1.DBOS.workflowID, callnum);
        });
        span.setStatus({ code: traces_1.SpanStatusCode.OK });
        dbos_executor_1.DBOSExecutor.globalInstance.tracer.endSpan(span);
        return res;
    }
    catch (err) {
        const e = err;
        span.setStatus({ code: traces_1.SpanStatusCode.ERROR, message: e.message });
        dbos_executor_1.DBOSExecutor.globalInstance.tracer.endSpan(span);
        throw err;
    }
}
exports.runTransaction = runTransaction;
// Transaction wrapper
function registerTransaction(dsName, func, config) {
    const dsn = dsName ?? '<default>';
    const funcName = config?.name ?? func.name;
    const reg = (0, decorators_1.wrapDBOSFunctionAndRegister)(config?.ctorOrProto, config?.className, funcName, funcName, func);
    const invokeWrapper = async function (...rawArgs) {
        (0, decorators_1.ensureDBOSIsLaunched)('transactions');
        const ds = (0, decorators_1.getTransactionalDataSource)(dsn);
        const callFunc = reg.registeredFunction ?? reg.origFunction;
        if (!dbos_1.DBOS.isWithinWorkflow()) {
            if ((0, context_1.getNextWFID)(undefined)) {
                throw new error_1.DBOSInvalidWorkflowTransitionError(`Call to transaction '${funcName}' made without starting workflow`);
            }
            return await (0, context_1.runWithDataSourceContext)(0, async () => {
                return await ds.invokeTransactionFunction(config, this, callFunc, ...rawArgs);
            });
        }
        if (dbos_1.DBOS.isInTransaction() || dbos_1.DBOS.isInStep()) {
            throw new error_1.DBOSInvalidWorkflowTransitionError('Invalid call to a `transaction` function from within a `step` or `transaction`');
        }
        const tracer = dbos_executor_1.DBOSExecutor.globalInstance.tracer;
        const span = tracer.startSpan(funcName, {
            operationUUID: dbos_1.DBOS.workflowID,
            operationType: dbos_executor_1.OperationType.TRANSACTION,
            operationName: funcName,
            authenticatedUser: dbos_1.DBOS.authenticatedUser ?? '',
            assumedRole: dbos_1.DBOS.assumedRole ?? '',
            authenticatedRoles: dbos_1.DBOS.authenticatedRoles ?? [],
            // isolationLevel: txnInfo.config.isolationLevel, // TODO: Pluggable
        }, dbos_1.DBOS.span);
        const callnum = (0, context_1.functionIDGetIncrement)();
        try {
            const res = await (0, traces_1.runWithTrace)(span, async () => {
                return await dbos_executor_1.DBOSExecutor.globalInstance.runInternalStep(async () => {
                    return await (0, context_1.runWithDataSourceContext)(callnum, async () => {
                        return await ds.invokeTransactionFunction(config, this, callFunc, ...rawArgs);
                    });
                }, funcName, dbos_1.DBOS.workflowID, callnum);
            });
            span.setStatus({ code: traces_1.SpanStatusCode.OK });
            dbos_executor_1.DBOSExecutor.globalInstance.tracer.endSpan(span);
            return res;
        }
        catch (err) {
            const e = err;
            span.setStatus({ code: traces_1.SpanStatusCode.ERROR, message: e.message });
            dbos_executor_1.DBOSExecutor.globalInstance.tracer.endSpan(span);
            throw err;
        }
    };
    (0, decorators_1.registerFunctionWrapper)(invokeWrapper, reg);
    Object.defineProperty(invokeWrapper, 'name', {
        value: funcName,
    });
    return invokeWrapper;
}
exports.registerTransaction = registerTransaction;
/**
 * Register a transactional data source, that helps DBOS provide
 *  transactional access to user databases
 * @param name - Registered name for the data source
 * @param ds - Transactional data source provider
 */
function registerDataSource(ds) {
    (0, decorators_1.registerTransactionalDataSource)(ds.name, ds);
}
exports.registerDataSource = registerDataSource;
/// Postgres helper routines
/** Isolation typically supported by application databases */
exports.PGIsolationLevel = Object.freeze({
    ReadUncommitted: 'READ UNCOMMITTED',
    ReadCommitted: 'READ COMMITTED',
    RepeatableRead: 'REPEATABLE READ',
    Serializable: 'SERIALIZABLE',
});
function checkSchemaInstallationPG(schemaName = 'dbos') {
    return `
SELECT
  EXISTS (
    SELECT 1
    FROM information_schema.schemata
    WHERE schema_name = '${schemaName}'
  ) AS schema_exists,
  EXISTS (
    SELECT 1
    FROM information_schema.tables
    WHERE table_schema = '${schemaName}'
      AND table_name = 'transaction_completion'
  ) AS table_exists;
`;
}
exports.checkSchemaInstallationPG = checkSchemaInstallationPG;
function createTransactionCompletionSchemaPG(schemaName = 'dbos') {
    return `CREATE SCHEMA IF NOT EXISTS "${schemaName}";`;
}
exports.createTransactionCompletionSchemaPG = createTransactionCompletionSchemaPG;
function createTransactionCompletionTablePG(schemaName = 'dbos') {
    return `
  CREATE TABLE IF NOT EXISTS "${schemaName}".transaction_completion (
    workflow_id TEXT NOT NULL,
    function_num INT NOT NULL,
    output TEXT,
    error TEXT,
    created_at BIGINT NOT NULL DEFAULT (EXTRACT(EPOCH FROM now())*1000)::bigint,
    PRIMARY KEY (workflow_id, function_num)
  );
`;
}
exports.createTransactionCompletionTablePG = createTransactionCompletionTablePG;
function getPGErrorCode(error) {
    return error && typeof error === 'object' && 'code' in error ? error.code : undefined;
}
function isPGRetriableTransactionError(error) {
    return getPGErrorCode(error) === '40001';
}
exports.isPGRetriableTransactionError = isPGRetriableTransactionError;
function isPGKeyConflictError(error) {
    return getPGErrorCode(error) === '23505';
}
exports.isPGKeyConflictError = isPGKeyConflictError;
function isPGFailedSqlTransactionError(error) {
    return getPGErrorCode(error) === '25P02';
}
exports.isPGFailedSqlTransactionError = isPGFailedSqlTransactionError;
var database_utils_1 = require("./database_utils");
Object.defineProperty(exports, "maskDatabaseUrl", { enumerable: true, get: function () { return database_utils_1.maskDatabaseUrl; } });
Object.defineProperty(exports, "getDatabaseNameFromUrl", { enumerable: true, get: function () { return database_utils_1.getDatabaseNameFromUrl; } });
Object.defineProperty(exports, "deriveDatabaseUrl", { enumerable: true, get: function () { return database_utils_1.deriveDatabaseUrl; } });
Object.defineProperty(exports, "getPGClientConfig", { enumerable: true, get: function () { return database_utils_1.getPGClientConfig; } });
Object.defineProperty(exports, "connectToPGDatabase", { enumerable: true, get: function () { return database_utils_1.connectToPGDatabase; } });
Object.defineProperty(exports, "connectToPGAndReportOutcome", { enumerable: true, get: function () { return database_utils_1.connectToPGAndReportOutcome; } });
Object.defineProperty(exports, "dropPGDatabase", { enumerable: true, get: function () { return database_utils_1.dropPGDatabase; } });
Object.defineProperty(exports, "ensurePGDatabase", { enumerable: true, get: function () { return database_utils_1.ensurePGDatabase; } });
//# sourceMappingURL=datasource.js.map