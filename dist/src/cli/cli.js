#!/usr/bin/env node
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
exports.runAndLog = void 0;
const config_1 = require("../config");
const commander_1 = require("commander");
const migrate_1 = require("./migrate");
const logs_1 = require("../telemetry/logs");
const collector_1 = require("../telemetry/collector");
const exporters_1 = require("../telemetry/exporters");
const readline = __importStar(require("node:readline/promises"));
const node_process_1 = require("node:process");
const __1 = require("..");
const system_database_1 = require("../system_database");
const node_process_2 = require("node:process");
const node_util_1 = require("node:util");
const commands_1 = require("./commands");
const docker_pg_helper_1 = require("./docker_pg_helper");
const database_utils_1 = require("../database_utils");
const node_fs_1 = require("node:fs");
const utils_1 = require("../utils");
const program = new commander_1.Command();
////////////////////////
/* LOCAL DEVELOPMENT  */
////////////////////////
program.version(utils_1.globalParams.dbosVersion);
program
    .command('start')
    .description('Start the server')
    .option('-l, --loglevel <string>', 'Specify log level')
    .action(async (options) => {
    const config = await (0, config_1.readConfigFile)();
    const dbosConfig = (0, config_1.getDbosConfig)(config, { logLevel: options.loglevel });
    const runtimeConfig = (0, config_1.getRuntimeConfig)(config);
    // If no start commands are provided, start the DBOS runtime
    if (runtimeConfig.start.length === 0) {
        console.error('No start commands provided in the configuration file.');
        (0, node_process_2.exit)(1);
    }
    else {
        const logger = getGlobalLogger(dbosConfig);
        for (const command of runtimeConfig.start) {
            try {
                const ret = await (0, commands_1.runCommand)(command, logger);
                if (ret !== 0) {
                    process.exit(ret);
                }
            }
            catch (e) {
                // We always reject the command with a return code
                process.exit(e);
            }
        }
    }
    function getGlobalLogger(configFile) {
        if (configFile.telemetry?.OTLPExporter) {
            return new logs_1.GlobalLogger(new collector_1.TelemetryCollector(new exporters_1.TelemetryExporter(configFile.telemetry.OTLPExporter)), configFile.telemetry?.logs);
        }
        return new logs_1.GlobalLogger();
    }
});
program
    .command('migrate')
    .description('Perform a database migration')
    .action(async () => {
    const configFile = await (0, config_1.readConfigFile)();
    let config = (0, config_1.getDbosConfig)(configFile);
    const runtimeConfig = (0, config_1.getRuntimeConfig)(configFile);
    if (process.env.DBOS__CLOUD === 'true') {
        [config] = (0, config_1.overwriteConfigForDBOSCloud)(config, runtimeConfig, configFile);
    }
    await runAndLog(configFile.database?.migrate ?? [], config, migrate_1.migrate);
});
program
    .command('schema')
    .description('Create the DBOS system database and its internal tables')
    .argument('[systemDatabaseUrl]', 'System database URL')
    .option('-r, --app-role <string>', 'The role with which you will run your DBOS application')
    .option('-s, --schema <string>', 'The schema name for DBOS system tables (default: dbos)')
    .action(async (systemDatabaseUrl, options) => {
    const logger = new logs_1.GlobalLogger();
    // Determine system database URL from argument or config
    const databaseURLs = await getDatabaseURLs(systemDatabaseUrl);
    systemDatabaseUrl = databaseURLs.systemDatabaseURL;
    // Get schema name from CLI option first, then config, then default
    let schemaName = options.schema ?? 'dbos';
    if (!options.schema) {
        try {
            if ((0, node_fs_1.existsSync)(config_1.dbosConfigFilePath)) {
                const configFile = await (0, config_1.readConfigFile)(config_1.dbosConfigFilePath);
                schemaName = configFile.system_database_schema_name ?? 'dbos';
            }
        }
        catch (e) {
            // If config file doesn't exist or can't be read, use default
        }
    }
    try {
        // Load the DBOS system schema.
        logger.info(`Creating DBOS system database and schema: ${schemaName}`);
        await (0, system_database_1.ensureSystemDatabase)(systemDatabaseUrl, logger, undefined, schemaName);
        // Grant permissions to application role if specified
        if (options.appRole) {
            await (0, system_database_1.grantDbosSchemaPermissions)(systemDatabaseUrl, options.appRole, logger, schemaName);
        }
    }
    catch (e) {
        logger.error(e);
        process.exit(1);
    }
});
program
    .command('postgres')
    .alias('pg')
    .description('Helps you setting up a local Postgres database with Docker')
    .addCommand(new commander_1.Command('start').description('Start a local Postgres database with Docker').action(async () => {
    await (0, docker_pg_helper_1.startDockerPg)();
}))
    .addCommand(new commander_1.Command('stop').description('Stop the local Postgres database with Docker').action(async () => {
    await (0, docker_pg_helper_1.stopDockerPg)();
}));
program
    .command('reset')
    .description('reset the system database')
    .option('-y, --yes', 'Skip confirmation prompt', false)
    .option('-s, --sys-db-url <string>', 'Your DBOS system database URL')
    .action(async (options) => {
    if (!options.yes) {
        const rl = readline.createInterface({ input: node_process_1.stdin, output: node_process_1.stdout });
        try {
            const answer = await rl.question('This command resets your DBOS system database, deleting metadata about past workflows and steps. Are you sure you want to proceed? (y/N) ');
            const userConfirmed = answer.toLowerCase() === 'y' || answer.toLowerCase() === 'yes';
            if (!userConfirmed) {
                console.log('Operation cancelled.');
                process.exit(0); // Exit the process if the user cancels
            }
        }
        finally {
            rl.close();
        }
    }
    const urls = await getDatabaseURLs(options.sysDbUrl);
    const res = await (0, database_utils_1.dropPGDatabase)({
        urlToDrop: urls.systemDatabaseURL,
        logger: (msg) => console.log(msg),
    });
    const sysDbName = (0, database_utils_1.getDatabaseNameFromUrl)(urls.systemDatabaseURL);
    if (res.status === 'dropped') {
        console.log(`Dropped '${sysDbName}'.  To use DBOS in the future, you will need to create a new system database.`);
    }
    else if (res.status === 'did_not_exist') {
        console.log(`Database '${sysDbName} was already dropped'.  To use DBOS in the future, you will need to create a new system database.`);
    }
    else if (res.status === 'failed') {
        console.log(`DROP operation for '${sysDbName} could not be attempted: \n ${res.notes.join('\n')} ${res.hint ?? ''}.`);
    }
});
/////////////////////////
/* WORKFLOW MANAGEMENT */
/////////////////////////
const workflowCommands = program
    .command('workflow')
    .alias('workflows')
    .alias('wf')
    .description('Manage DBOS workflows');
workflowCommands
    .command('list')
    .description('List workflows from your application')
    .option('-n, --name <string>', 'Retrieve functions with this name')
    .option('-l, --limit <number>', 'Limit the results returned', '10')
    .option('-u, --user <string>', 'Retrieve workflows run by this user')
    .option('-t, --start-time <string>', 'Retrieve workflows starting after this timestamp (ISO 8601 format)')
    .option('-e, --end-time <string>', 'Retrieve workflows starting before this timestamp (ISO 8601 format)')
    .option('-S, --status <string>', 'Retrieve workflows with this status (PENDING, SUCCESS, ERROR, ENQUEUED, DELAYED, CANCELLED, or MAX_RECOVERY_ATTEMPTS_EXCEEDED)')
    .option('-v, --application-version <string>', 'Retrieve workflows with this application version')
    .option('-s, --sys-db-url <string>', 'Your DBOS system database URL')
    .action(async (options) => {
    const validStatuses = Object.values(__1.StatusString);
    if (options.status && !validStatuses.includes(options.status)) {
        console.error('Invalid status: ', options.status);
        (0, node_process_2.exit)(1);
    }
    const input = {
        workflowName: options.name,
        limit: Number(options.limit),
        authenticatedUser: options.user,
        startTime: options.startTime,
        endTime: options.endTime,
        status: options.status,
        applicationVersion: options.applicationVersion,
    };
    const urls = await getDatabaseURLs(options.sysDbUrl);
    const client = await __1.DBOSClient.create({
        systemDatabaseUrl: urls.systemDatabaseURL,
    });
    try {
        const output = await client.listWorkflows(input);
        console.log(JSON.stringify(output.map((wf) => inspectUnsafeFields(wf, ['input', 'output', 'error']))));
    }
    finally {
        await client.destroy();
    }
});
workflowCommands
    .command('get')
    .description('Retrieve the status of a workflow')
    .argument('<workflowID>', 'Target workflow ID')
    .option('-s, --sys-db-url <string>', 'Your DBOS system database URL')
    .action(async (workflowID, options) => {
    const urls = await getDatabaseURLs(options.sysDbUrl);
    const client = await __1.DBOSClient.create({
        systemDatabaseUrl: urls.systemDatabaseURL,
    });
    try {
        const output = await client.getWorkflow(workflowID);
        console.log(JSON.stringify(output ? inspectUnsafeFields(output, ['input', 'output', 'error']) : output));
    }
    finally {
        await client.destroy();
    }
});
workflowCommands
    .command('steps')
    .description('List the steps of a workflow')
    .argument('<workflowID>', 'Target workflow ID')
    .option('-s, --sys-db-url <string>', 'Your DBOS system database URL')
    .action(async (workflowID, options) => {
    const urls = await getDatabaseURLs(options.sysDbUrl);
    const client = await __1.DBOSClient.create({
        systemDatabaseUrl: urls.systemDatabaseURL,
    });
    try {
        const output = await client.listWorkflowSteps(workflowID);
        console.log(JSON.stringify(output?.map((step) => inspectUnsafeFields(step, ['output', 'error']))));
    }
    finally {
        await client.destroy();
    }
});
workflowCommands
    .command('cancel')
    .description('Cancel a workflow so it is no longer automatically retried or restarted')
    .argument('<workflowID>', 'Target workflow ID')
    .option('-s, --sys-db-url <string>', 'Your DBOS system database URL')
    .action(async (workflowID, options) => {
    const urls = await getDatabaseURLs(options.sysDbUrl);
    const client = await __1.DBOSClient.create({
        systemDatabaseUrl: urls.systemDatabaseURL,
    });
    try {
        await client.cancelWorkflow(workflowID);
    }
    finally {
        await client.destroy();
    }
});
workflowCommands
    .command('resume')
    .description('Resume a workflow from the last step it executed, keeping its workflow ID')
    .argument('<workflowID>', 'Target workflow ID')
    .option('-s, --sys-db-url <string>', 'Your DBOS system database URL')
    .action(async (workflowID, options) => {
    const urls = await getDatabaseURLs(options.sysDbUrl);
    const client = await __1.DBOSClient.create({
        systemDatabaseUrl: urls.systemDatabaseURL,
    });
    try {
        await client.resumeWorkflow(workflowID);
    }
    finally {
        await client.destroy();
    }
});
workflowCommands
    .command('fork')
    .description('Fork a workflow from a step with a new ID')
    .argument('<workflowID>', 'Target workflow ID')
    .requiredOption('-S, --step <number>', 'Restart from this step')
    .option('-f, --forked-workflow-id <string>', 'Custom ID for the forked workflow')
    .option('-v, --application-version <string>', 'Custom application version for the forked workflow')
    .option('-s, --sys-db-url <string>', 'Your DBOS system database URL')
    .action(async (workflowID, options) => {
    const urls = await getDatabaseURLs(options.sysDbUrl);
    const client = await __1.DBOSClient.create({
        systemDatabaseUrl: urls.systemDatabaseURL,
    });
    try {
        await client.forkWorkflow(workflowID, options.step, {
            newWorkflowID: options.forkedWorkflowId,
            applicationVersion: options.applicationVersion,
        });
    }
    finally {
        await client.destroy();
    }
});
const queueCommands = workflowCommands.command('queue').alias('queues').alias('q').description('Manage DBOS queues');
queueCommands
    .command('list')
    .description('List enqueued functions from your application')
    .option('-n, --name <string>', 'Retrieve functions with this name')
    .option('-t, --start-time <string>', 'Retrieve functions starting after this timestamp (ISO 8601 format)')
    .option('-e, --end-time <string>', 'Retrieve functions starting before this timestamp (ISO 8601 format)')
    .option('-S, --status <string>', 'Retrieve functions with this status (PENDING, SUCCESS, ERROR, ENQUEUED, DELAYED, CANCELLED, or MAX_RECOVERY_ATTEMPTS_EXCEEDED)')
    .option('-l, --limit <number>', 'Limit the results returned')
    .option('-q, --queue <string>', 'Retrieve functions run on this queue')
    .option('-s, --sys-db-url <string>', 'Your DBOS system database URL')
    .action(async (options) => {
    const validStatuses = Object.values(__1.StatusString);
    if (options.status && !validStatuses.includes(options.status)) {
        console.error('Invalid status: ', options.status);
        (0, node_process_2.exit)(1);
    }
    const input = {
        limit: Number(options.limit),
        startTime: options.startTime,
        endTime: options.endTime,
        status: options.status,
        workflowName: options.name,
        queueName: options.queue,
    };
    const urls = await getDatabaseURLs(options.sysDbUrl);
    const client = await __1.DBOSClient.create({
        systemDatabaseUrl: urls.systemDatabaseURL,
    });
    try {
        // TOD: Review!
        const output = await client.listQueuedWorkflows(input);
        console.log(JSON.stringify(output.map((wf) => inspectUnsafeFields(wf, ['input', 'output', 'error']))));
    }
    finally {
        await client.destroy();
    }
});
/////////////
/* PARSING */
/////////////
function inspectUnsafeFields(obj, fields) {
    const result = { ...obj };
    for (const field of fields) {
        if (result[field] !== undefined && result[field] !== null) {
            result[field] = (0, node_util_1.inspect)(result[field]);
        }
    }
    return result;
}
program.parse(process.argv);
// If no arguments provided, display help by default
if (!process.argv.slice(2).length) {
    program.outputHelp();
}
async function getDatabaseURLs(systemDatabaseURL) {
    if (process.env.DBOS__CLOUD === 'true') {
        return {
            systemDatabaseURL: process.env.DBOS_SYSTEM_DATABASE_URL,
        };
    }
    if (systemDatabaseURL) {
        return { systemDatabaseURL: systemDatabaseURL };
    }
    if ((0, node_fs_1.existsSync)(config_1.dbosConfigFilePath)) {
        const config = await (0, config_1.readConfigFile)();
        return {
            systemDatabaseURL: (0, config_1.getSystemDatabaseUrl)(config),
        };
    }
    else {
        throw new Error('Error: Missing database URL: please set it using CLI flags or your dbos-config.yaml file.');
    }
}
//Takes an action function(configFile, logger) that returns a numeric exit code.
//If otel exporter is specified in configFile, adds it to the logger and flushes it after.
//If action throws, logs the exception and sets the exit code to 1.
//Finally, terminates the program with the exit code.
async function runAndLog(migrationCommands, config, action) {
    let logger = new logs_1.GlobalLogger();
    let terminate = undefined;
    if (config.telemetry.OTLPExporter) {
        logger = new logs_1.GlobalLogger(new collector_1.TelemetryCollector(new exporters_1.TelemetryExporter({
            logsEndpoint: config.telemetry.OTLPExporter.logsEndpoint ?? [],
            tracesEndpoint: config.telemetry.OTLPExporter.tracesEndpoint ?? [],
        })), config.telemetry?.logs);
        terminate = (code) => {
            void logger.destroy().finally(() => {
                process.exit(code);
            });
        };
    }
    else {
        terminate = (code) => {
            process.exit(code);
        };
    }
    let returnCode = 1;
    try {
        returnCode = await action(migrationCommands, config.systemDatabaseUrl, logger);
    }
    catch (e) {
        logger.error(e);
    }
    terminate(returnCode);
}
exports.runAndLog = runAndLog;
//# sourceMappingURL=cli.js.map