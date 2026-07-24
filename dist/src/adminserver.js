"use strict";
var __createBinding = (this && this.__createBinding) || (Object.create ? (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    var desc = Object.getOwnPropertyDescriptor(m, k);
    if (!desc || ("get" in desc ? !m.__esModule : desc.writable || desc.configurable)) {
      desc = { enumerable: true, get: function() { return m[k]; } };
    }
    Object.defineProperty(o, k2, desc);
}) : (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    o[k2] = m[k];
}));
var __setModuleDefault = (this && this.__setModuleDefault) || (Object.create ? (function(o, v) {
    Object.defineProperty(o, "default", { enumerable: true, value: v });
}) : function(o, v) {
    o["default"] = v;
});
var __importStar = (this && this.__importStar) || function (mod) {
    if (mod && mod.__esModule) return mod;
    var result = {};
    if (mod != null) for (var k in mod) if (k !== "default" && Object.prototype.hasOwnProperty.call(mod, k)) __createBinding(result, mod, k);
    __setModuleDefault(result, mod);
    return result;
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.DBOSAdminServer = exports.WorkflowQueuesMetadataUrl = exports.ConductorUrl = exports.DeactivateUrl = exports.PerfUrl = exports.HealthUrl = exports.WorkflowRecoveryUrl = exports.WorkflowUUIDHeader = void 0;
const http = __importStar(require("http"));
const url = __importStar(require("url"));
const error_1 = require("./error");
const net = __importStar(require("net"));
const perf_hooks_1 = require("perf_hooks");
const utils_1 = require("./utils");
const wfqueue_1 = require("./wfqueue");
const workflow_management_1 = require("./workflow_management");
const protocol = __importStar(require("./conductor/protocol"));
exports.WorkflowUUIDHeader = 'dbos-idempotency-key';
exports.WorkflowRecoveryUrl = '/dbos-workflow-recovery';
exports.HealthUrl = '/dbos-healthz';
exports.PerfUrl = '/dbos-perf';
exports.DeactivateUrl = '/deactivate';
exports.ConductorUrl = '/conductor';
exports.WorkflowQueuesMetadataUrl = '/dbos-workflow-queues-metadata';
// Helper to parse JSON body
async function parseJsonBody(req) {
    return new Promise((resolve, reject) => {
        let body = '';
        req.on('data', (chunk) => {
            body += String(chunk);
        });
        req.on('end', () => {
            try {
                resolve(body ? JSON.parse(body) : {});
            }
            catch (e) {
                reject(new Error('Invalid JSON'));
            }
        });
        req.on('error', reject);
    });
}
// Helper to send JSON response
function sendJson(res, statusCode, data) {
    res.writeHead(statusCode, {
        'Content-Type': 'application/json',
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
        'Access-Control-Allow-Headers': 'Content-Type',
    });
    res.end(JSON.stringify(data));
}
// Helper to send text response
function sendText(res, statusCode, text) {
    res.writeHead(statusCode, {
        'Content-Type': 'text/plain',
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
        'Access-Control-Allow-Headers': 'Content-Type',
    });
    res.end(text);
}
// Helper to send no content response
function sendNoContent(res) {
    res.writeHead(204, {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
        'Access-Control-Allow-Headers': 'Content-Type',
    });
    res.end();
}
// Helper to extract path params
function matchPath(pattern, pathname) {
    const patternParts = pattern.split('/');
    const pathParts = pathname.split('/');
    if (patternParts.length !== pathParts.length)
        return null;
    const params = {};
    for (let i = 0; i < patternParts.length; i++) {
        const patternPart = patternParts[i];
        const pathPart = pathParts[i];
        if (patternPart.startsWith(':')) {
            const paramName = patternPart.substring(1);
            params[paramName] = pathPart;
        }
        else if (patternPart !== pathPart) {
            return null;
        }
    }
    return params;
}
class DBOSAdminServer {
    static setupAdminApp(dbosExec) {
        const routes = [];
        // Register HTTP endpoints.
        DBOSAdminServer.registerHealthEndpoint(dbosExec, routes);
        DBOSAdminServer.registerRecoveryEndpoint(dbosExec, routes);
        DBOSAdminServer.registerPerfEndpoint(dbosExec, routes);
        DBOSAdminServer.registerDeactivateEndpoint(dbosExec, routes);
        DBOSAdminServer.registerConductorEndpoint(dbosExec, routes);
        DBOSAdminServer.registerCancelWorkflowEndpoint(dbosExec, routes);
        DBOSAdminServer.registerResumeWorkflowEndpoint(dbosExec, routes);
        DBOSAdminServer.registerRestartWorkflowEndpoint(dbosExec, routes);
        DBOSAdminServer.registerQueueMetadataEndpoint(dbosExec, routes);
        DBOSAdminServer.registerListWorkflowStepsEndpoint(dbosExec, routes);
        DBOSAdminServer.registerListWorkflowsEndpoint(dbosExec, routes);
        DBOSAdminServer.registerListQueuedWorkflowsEndpoint(dbosExec, routes);
        DBOSAdminServer.registerGetWorkflowEndpoint(dbosExec, routes);
        DBOSAdminServer.registerForkWorkflowEndpoint(dbosExec, routes);
        DBOSAdminServer.registerGarbageCollectEndpoint(dbosExec, routes);
        DBOSAdminServer.registerGlobalTimeoutEndpoint(dbosExec, routes);
        // Create the HTTP server
        const server = http.createServer(async (req, res) => {
            // Handle CORS preflight requests
            if (req.method === 'OPTIONS') {
                res.writeHead(200, {
                    'Access-Control-Allow-Origin': '*',
                    'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
                    'Access-Control-Allow-Headers': 'Content-Type',
                    'Access-Control-Max-Age': '86400',
                });
                res.end();
                return;
            }
            const parsedUrl = url.parse(req.url || '', true);
            const pathname = parsedUrl.pathname || '';
            // Find matching route
            let routeFound = false;
            for (const route of routes) {
                if (req.method === route.method) {
                    const params = matchPath(route.path, pathname);
                    if (params !== null) {
                        routeFound = true;
                        try {
                            await route.handler(req, res, params);
                        }
                        catch (error) {
                            dbosExec.logger.error(`Request handler error: ${String(error)}`);
                            sendJson(res, 500, { error: 'Internal server error' });
                        }
                        break;
                    }
                }
            }
            if (!routeFound) {
                res.writeHead(404, { 'Content-Type': 'text/plain' });
                res.end('Not Found');
            }
        });
        return server;
    }
    static async checkPortAvailabilityIPv4Ipv6(port, logger) {
        try {
            await this.checkPortAvailability(port, '127.0.0.1');
        }
        catch (error) {
            const err = error;
            if (err.code === 'EADDRINUSE') {
                logger.warn(`Port ${port} is already used for IPv4 address "127.0.0.1". Please use the -p option to choose another port.\n${err.message}`);
                throw error;
            }
            else {
                logger.warn(`Error occurred while checking port availability for IPv4 address "127.0.0.1" : ${err.code}\n${err.message}`);
            }
        }
        try {
            await this.checkPortAvailability(port, '::1');
        }
        catch (error) {
            const err = error;
            if (err.code === 'EADDRINUSE') {
                logger.warn(`Port ${port} is already used for IPv6 address "::1". Please use the -p option to choose another port.\n${err.message}`);
                throw error;
            }
            else {
                logger.warn(`Error occurred while checking port availability for IPv6 address "::1" : ${err.code}\n${err.message}`);
            }
        }
    }
    static async checkPortAvailability(port, host) {
        return new Promise((resolve, reject) => {
            const server = new net.Server();
            server.on('error', (error) => {
                reject(error);
            });
            server.on('listening', () => {
                server.close();
                resolve();
            });
            server.listen({ port: port, host: host }, () => {
                resolve();
            });
        });
    }
    /**
     * Health check endpoint.
     */
    static registerHealthEndpoint(dbosExec, routes) {
        routes.push({
            method: 'GET',
            path: exports.HealthUrl,
            handler: async (req, res) => {
                sendText(res, 200, 'healthy');
                return Promise.resolve();
            },
        });
        dbosExec.logger.debug(`DBOS Server Registered Healthz GET ${exports.HealthUrl}`);
    }
    /**
     * Register workflow queue metadata endpoint.
     */
    static registerQueueMetadataEndpoint(dbosExec, routes) {
        routes.push({
            method: 'GET',
            path: exports.WorkflowQueuesMetadataUrl,
            handler: async (req, res) => {
                // Merge DB-backed queues with the in-memory registry. In-memory wins
                // on collision so a process running with a code-defined queue exposes
                // its actual configuration even if a stale row exists in the DB.
                const merged = new Map();
                try {
                    const records = await dbosExec.systemDatabase.listQueues();
                    for (const record of records) {
                        if (record.name === utils_1.INTERNAL_QUEUE_NAME)
                            continue;
                        const q = wfqueue_1.WorkflowQueue._fromRecord(record);
                        merged.set(record.name, {
                            name: record.name,
                            concurrency: q.concurrency,
                            workerConcurrency: q.workerConcurrency,
                            rateLimit: q.rateLimit,
                        });
                    }
                }
                catch (e) {
                    dbosExec.logger.warn(`Error listing database-backed queues for metadata endpoint: ${e.message}`);
                }
                wfqueue_1.wfQueueRunner.wfQueuesByName.forEach((q, qn) => {
                    merged.set(qn, {
                        name: qn,
                        concurrency: q.concurrency,
                        workerConcurrency: q.workerConcurrency,
                        rateLimit: q.rateLimit,
                    });
                });
                sendJson(res, 200, [...merged.values()]);
                return Promise.resolve();
            },
        });
        dbosExec.logger.debug(`DBOS Server Registered Queue Metadata GET ${exports.WorkflowQueuesMetadataUrl}`);
    }
    /**
     * Register workflow recovery endpoint.
     * Receives a list of executor IDs and returns a list of workflowUUIDs.
     */
    static registerRecoveryEndpoint(dbosExec, routes) {
        routes.push({
            method: 'POST',
            path: exports.WorkflowRecoveryUrl,
            handler: async (req, res) => {
                const executorIDs = (await parseJsonBody(req));
                dbosExec.logger.info('Recovering workflows for executors: ' + executorIDs.toString());
                const recoverHandles = await dbosExec.recoverPendingWorkflows(executorIDs);
                // Return a list of workflowUUIDs being recovered.
                // eslint-disable-next-line @typescript-eslint/await-thenable
                const result = await Promise.allSettled(recoverHandles.map((i) => i.workflowID)).then((results) => results.filter((i) => i.status === 'fulfilled').map((i) => i.value));
                sendJson(res, 200, result);
            },
        });
        dbosExec.logger.debug(`DBOS Server Registered Recovery POST ${exports.WorkflowRecoveryUrl}`);
    }
    /**
     * Register performance endpoint.
     * Returns information on VM performance since last call.
     */
    static lastELU = perf_hooks_1.performance.eventLoopUtilization();
    static registerPerfEndpoint(dbosExec, routes) {
        routes.push({
            method: 'GET',
            path: exports.PerfUrl,
            handler: async (req, res) => {
                const currELU = perf_hooks_1.performance.eventLoopUtilization();
                const elu = perf_hooks_1.performance.eventLoopUtilization(currELU, DBOSAdminServer.lastELU);
                sendJson(res, 200, elu);
                DBOSAdminServer.lastELU = currELU;
                return Promise.resolve();
            },
        });
        dbosExec.logger.debug(`DBOS Server Registered Perf GET ${exports.PerfUrl}`);
    }
    /**
     * Register Deactivate endpoint.
     * Deactivate consumers so that they don't start new workflows.
     */
    static isDeactivated = false;
    static registerDeactivateEndpoint(dbosExec, routes) {
        routes.push({
            method: 'GET',
            path: exports.DeactivateUrl,
            handler: async (req, res) => {
                if (!DBOSAdminServer.isDeactivated) {
                    dbosExec.logger.info(`Deactivating DBOS executor ${utils_1.globalParams.executorID} with version ${utils_1.globalParams.appVersion}. This executor will complete existing workflows but will not create new workflows.`);
                    DBOSAdminServer.isDeactivated = true;
                }
                await dbosExec.deactivateEventReceivers(false);
                sendText(res, 200, 'Deactivated');
            },
        });
        dbosExec.logger.debug(`DBOS Server Registered Deactivate GET ${exports.DeactivateUrl}`);
    }
    /**
     * Register Conductor status endpoint.
     * Indicates that a Conductor connection is enabled for this executor.
     */
    static registerConductorEndpoint(dbosExec, routes) {
        routes.push({
            method: 'GET',
            path: exports.ConductorUrl,
            handler: async (req, res) => {
                sendJson(res, 200, { status: true });
                return Promise.resolve();
            },
        });
        dbosExec.logger.debug(`DBOS Server Registered Conductor GET ${exports.ConductorUrl}`);
    }
    static registerGarbageCollectEndpoint(dbosExec, routes) {
        const url = '/dbos-garbage-collect';
        routes.push({
            method: 'POST',
            path: url,
            handler: async (req, res) => {
                const body = (await parseJsonBody(req));
                await dbosExec.systemDatabase.garbageCollect(body.cutoff_epoch_timestamp_ms, body.rows_threshold);
                sendNoContent(res);
            },
        });
    }
    static registerGlobalTimeoutEndpoint(dbosExec, routes) {
        const url = '/dbos-global-timeout';
        routes.push({
            method: 'POST',
            path: url,
            handler: async (req, res) => {
                const body = (await parseJsonBody(req));
                await (0, workflow_management_1.globalTimeout)(dbosExec.systemDatabase, body.cutoff_epoch_timestamp_ms);
                sendNoContent(res);
            },
        });
    }
    /**
     * Register Cancel Workflow endpoint.
     * Cancels a workflow by setting its status to CANCELLED.
     */
    static registerCancelWorkflowEndpoint(dbosExec, routes) {
        const workflowCancelUrl = '/workflows/:workflow_id/cancel';
        routes.push({
            method: 'POST',
            path: workflowCancelUrl,
            handler: async (req, res, params) => {
                const workflowId = params.workflow_id;
                await dbosExec.systemDatabase.cancelWorkflows([workflowId]);
                sendNoContent(res);
            },
        });
        dbosExec.logger.debug(`DBOS Server Registered Cancel Workflow POST ${workflowCancelUrl}`);
    }
    /**
     * Register Resume Workflow endpoint.
     * Resume a workflow.
     */
    static registerResumeWorkflowEndpoint(dbosExec, routes) {
        const workflowResumeUrl = '/workflows/:workflow_id/resume';
        routes.push({
            method: 'POST',
            path: workflowResumeUrl,
            handler: async (req, res, params) => {
                const workflowId = params.workflow_id;
                dbosExec.logger.info(`Resuming workflow with ID: ${workflowId}`);
                try {
                    await dbosExec.systemDatabase.resumeWorkflows([workflowId]);
                    sendNoContent(res);
                }
                catch (e) {
                    let errorMessage = '';
                    if (e instanceof error_1.DBOSError) {
                        errorMessage = e.message;
                    }
                    else {
                        errorMessage = `Unknown error`;
                    }
                    dbosExec.logger.error(`Error resuming workflow ${workflowId}: ${errorMessage}`);
                    sendJson(res, 500, {
                        error: `Error resuming workflow ${workflowId}: ${errorMessage}`,
                    });
                }
            },
        });
        dbosExec.logger.debug(`DBOS Server Registered Resume Workflow POST ${workflowResumeUrl}`);
    }
    /**
     * Register Restart Workflow endpoint.
     * Restart a workflow.
     */
    static registerRestartWorkflowEndpoint(dbosExec, routes) {
        const workflowRestartUrl = '/workflows/:workflow_id/restart';
        routes.push({
            method: 'POST',
            path: workflowRestartUrl,
            handler: async (req, res, params) => {
                const workflowId = params.workflow_id;
                dbosExec.logger.info(`Restarting workflow: ${workflowId} with a new id`);
                const workflowID = await dbosExec.forkWorkflow(workflowId, 0);
                sendJson(res, 200, {
                    workflow_id: workflowID,
                });
            },
        });
        dbosExec.logger.debug(`DBOS Server Registered Restart Workflow POST ${workflowRestartUrl}`);
    }
    /**
     * Register Fork Workflow endpoint.
     */
    static registerForkWorkflowEndpoint(dbosExec, routes) {
        const workflowForkUrl = '/workflows/:workflow_id/fork';
        routes.push({
            method: 'POST',
            path: workflowForkUrl,
            handler: async (req, res, params) => {
                const workflowId = params.workflow_id;
                const body = (await parseJsonBody(req));
                if (body.start_step === undefined) {
                    sendJson(res, 400, { error: 'Missing start_step in request body' });
                    return;
                }
                dbosExec.logger.info(`Forking workflow: ${workflowId} from step ${body.start_step} with a new id`);
                try {
                    const workflowID = await dbosExec.forkWorkflow(workflowId, body.start_step, {
                        newWorkflowID: body.new_workflow_id,
                        applicationVersion: body.application_version,
                        timeoutMS: body.timeout_ms,
                    });
                    sendJson(res, 200, {
                        workflow_id: workflowID,
                    });
                }
                catch (e) {
                    let errorMessage = '';
                    if (e instanceof error_1.DBOSError) {
                        errorMessage = e.message;
                    }
                    else {
                        errorMessage = `Unknown error`;
                    }
                    dbosExec.logger.error(`Error forking workflow ${workflowId}: ${errorMessage}`);
                    sendJson(res, 500, {
                        error: `Error forking workflow ${workflowId}: ${errorMessage}`,
                    });
                }
            },
        });
        dbosExec.logger.debug(`DBOS Server Registered Fork Workflow POST ${workflowForkUrl}`);
    }
    /**
     * Register List Workflow Steps endpoint.
     * List steps for a given workflow.
     */
    static registerListWorkflowStepsEndpoint(dbosExec, routes) {
        const workflowStepsUrl = '/workflows/:workflow_id/steps';
        routes.push({
            method: 'GET',
            path: workflowStepsUrl,
            handler: async (req, res, params) => {
                const workflowId = params.workflow_id;
                const steps = await dbosExec.listWorkflowSteps(workflowId);
                const result = steps?.map((step) => new protocol.WorkflowSteps(step));
                sendJson(res, 200, result);
            },
        });
        dbosExec.logger.debug(`DBOS Server Registered List Workflow steps Get ${workflowStepsUrl}`);
    }
    /**
     * Register List Workflows endpoint.
     * List workflows with optional filtering via request body.
     */
    static registerListWorkflowsEndpoint(dbosExec, routes) {
        const listWorkflowsUrl = '/workflows';
        routes.push({
            method: 'POST',
            path: listWorkflowsUrl,
            handler: async (req, res) => {
                const body = (await parseJsonBody(req));
                // Map request body keys to GetWorkflowsInput properties
                const input = {
                    workflowIDs: body.workflow_uuids,
                    workflowName: body.workflow_name,
                    authenticatedUser: body.authenticated_user,
                    startTime: body.start_time,
                    endTime: body.end_time,
                    status: body.status,
                    applicationVersion: body.application_version,
                    forkedFrom: body.fork_from,
                    parentWorkflowID: body.parent_workflow_id,
                    limit: body.limit,
                    offset: body.offset,
                    sortDesc: body.sort_desc,
                    workflow_id_prefix: body.workflow_id_prefix,
                    loadInput: body.load_input ?? false,
                    loadOutput: body.load_output ?? false,
                };
                const workflows = await dbosExec.listWorkflows(input);
                // Map result to the underscore format.
                const result = workflows.map((wf) => new protocol.WorkflowsOutput(wf));
                sendJson(res, 200, result);
            },
        });
        dbosExec.logger.debug(`DBOS Server Registered List Workflows POST ${listWorkflowsUrl}`);
    }
    /**
     * Register List Queued Workflows endpoint.
     * List queued workflows with optional filtering via request body.
     */
    static registerListQueuedWorkflowsEndpoint(dbosExec, routes) {
        const listQueuedWorkflowsUrl = '/queues';
        routes.push({
            method: 'POST',
            path: listQueuedWorkflowsUrl,
            handler: async (req, res) => {
                const body = (await parseJsonBody(req));
                // Map request body keys to GetQueuedWorkflowsInput properties
                const input = {
                    workflowName: body.workflow_name,
                    startTime: body.start_time,
                    endTime: body.end_time,
                    status: body.status,
                    forkedFrom: body.fork_from,
                    parentWorkflowID: body.parent_workflow_id,
                    queueName: body.queue_name,
                    limit: body.limit,
                    offset: body.offset,
                    sortDesc: body.sort_desc,
                    loadInput: body.load_input ?? false,
                };
                const workflows = await dbosExec.listQueuedWorkflows(input);
                // Map result to the underscore format.
                const result = workflows.map((wf) => new protocol.WorkflowsOutput(wf));
                sendJson(res, 200, result);
            },
        });
        dbosExec.logger.debug(`DBOS Server Registered List Queued Workflows POST ${listQueuedWorkflowsUrl}`);
    }
    /**
     * Register Get Workflow endpoint.
     * Get detailed information about a specific workflow by ID.
     */
    static registerGetWorkflowEndpoint(dbosExec, routes) {
        const getWorkflowUrl = '/workflows/:workflow_id';
        routes.push({
            method: 'GET',
            path: getWorkflowUrl,
            handler: async (req, res, params) => {
                const workflowId = params.workflow_id;
                const workflow = await dbosExec.getWorkflowStatus(workflowId);
                if (workflow) {
                    const result = new protocol.WorkflowsOutput(workflow);
                    sendJson(res, 200, result);
                }
                else {
                    sendJson(res, 404, { error: `Workflow ${workflowId} not found` });
                }
            },
        });
        dbosExec.logger.debug(`DBOS Server Registered Get Workflow GET ${getWorkflowUrl}`);
    }
}
exports.DBOSAdminServer = DBOSAdminServer;
//# sourceMappingURL=adminserver.js.map