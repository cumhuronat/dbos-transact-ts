"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.overwriteConfigForDBOSCloud = exports.translateRuntimeConfig = exports.getRuntimeConfig = exports.translateDbosConfig = exports.getDbosConfig = exports.getSystemDatabaseUrl = exports.isValidDatabaseName = exports.writeConfigFile = exports.readConfigFile = exports.substituteEnvVars = exports.dbosConfigFilePath = void 0;
const utils_1 = require("./utils");
const yaml_1 = __importDefault(require("yaml"));
const fs_1 = require("fs");
const path_1 = __importDefault(require("path"));
const assert_1 = __importDefault(require("assert"));
const database_utils_1 = require("./database_utils");
const serialization_1 = require("./serialization");
exports.dbosConfigFilePath = 'dbos-config.yaml';
/*
 * Substitute environment variables using a regex for matching.
 * Will find anything in curly braces.
 * TODO: Use a more robust solution.
 */
function substituteEnvVars(content) {
    const regex = /\${([^}]+)}/g; // Regex to match ${VAR_NAME} style placeholders
    return content.replace(regex, (_, g1) => {
        return process.env[g1] || '""'; // If the env variable is not set, return an empty string.
    });
}
exports.substituteEnvVars = substituteEnvVars;
async function readConfigFile(dirPath) {
    dirPath ??= process.cwd();
    const dbosConfigPath = path_1.default.join(dirPath, exports.dbosConfigFilePath);
    const configContent = await readFileHelper(dbosConfigPath);
    const config = configContent ? yaml_1.default.parse(substituteEnvVars(configContent)) : {};
    if (!config.name) {
        const packageJsonPath = path_1.default.join(dirPath, 'package.json');
        const packageContent = await readFileHelper(packageJsonPath);
        const $package = packageContent ? JSON.parse(packageContent) : {};
        config.name = $package.name;
    }
    return config;
    async function readFileHelper(filePath) {
        try {
            return await (0, utils_1.readFile)(filePath);
        }
        catch (error) {
            if (error && typeof error === 'object' && 'code' in error && error.code === 'ENOENT') {
                return undefined; // File does not exist
            }
            throw error; // Rethrow other errors
        }
    }
}
exports.readConfigFile = readConfigFile;
function writeConfigFile(configFile, configFilePath) {
    try {
        const configFileContent = yaml_1.default.stringify(configFile);
        (0, fs_1.writeFileSync)(configFilePath, configFileContent);
    }
    catch (e) {
        if (e instanceof Error) {
            throw new Error(`Failed to write config to ${configFilePath}: ${e.message}`);
        }
        else {
            throw e;
        }
    }
}
exports.writeConfigFile = writeConfigFile;
function isValidDatabaseName(dbName) {
    if (dbName.length < 1 || dbName.length > 63) {
        return false;
    }
    return true;
}
exports.isValidDatabaseName = isValidDatabaseName;
function getSystemDatabaseUrl(configFile) {
    const databaseUrl = configFile.system_database_url || defaultSysDatabaseUrl(configFile.name);
    const url = new URL(databaseUrl);
    const dbName = url.pathname.slice(1);
    const missingFields = [];
    if (!url.username)
        missingFields.push('username');
    if (!url.hostname)
        missingFields.push('hostname');
    if (!dbName)
        missingFields.push('database name');
    if (missingFields.length > 0) {
        throw new Error(`Invalid database URL: missing required field(s): ${missingFields.join(', ')}`);
    }
    if (!isValidDatabaseName(dbName))
        throw new Error(`Database name "${dbName}" in database_url ${(0, database_utils_1.maskDatabaseUrl)(databaseUrl)} is invalid.`);
    return databaseUrl;
    function defaultSysDatabaseUrl(appName) {
        (0, assert_1.default)(appName, 'Application name must be defined to construct a valid database URL.');
        const host = process.env.PGHOST || 'localhost';
        const port = process.env.PGPORT || '5432';
        const username = process.env.PGUSER || 'postgres';
        const password = process.env.PGPASSWORD || 'dbos';
        const database = toDbName(appName) + '_dbos_sys';
        const timeout = process.env.PGCONNECT_TIMEOUT || '10';
        const sslmode = process.env.PGSSLMODE || (host === 'localhost' ? 'disable' : 'allow');
        const dbUrl = new URL(`postgresql://host/database`);
        dbUrl.username = username;
        dbUrl.password = password;
        dbUrl.hostname = host;
        dbUrl.port = port;
        dbUrl.protocol = 'postgresql';
        dbUrl.pathname = `/${database}`;
        dbUrl.searchParams.set('connect_timeout', timeout);
        dbUrl.searchParams.set('sslmode', sslmode);
        return dbUrl.toString();
    }
    function toDbName(appName) {
        const dbName = appName.toLowerCase().replaceAll('-', '_').replaceAll(' ', '_');
        return dbName.match(/^\d/) ? '_' + dbName : dbName;
    }
}
exports.getSystemDatabaseUrl = getSystemDatabaseUrl;
function getDbosConfig(config, options = {}) {
    (0, assert_1.default)(config.language === undefined || config.language === 'node', `Config file specifies invalid language ${config.language}`);
    return translateDbosConfig({
        name: config.name,
        systemDatabaseUrl: config.system_database_url,
        systemDatabaseSchemaName: config.system_database_schema_name,
        logLevel: options.logLevel ?? config.telemetry?.logs?.logLevel,
        addContextMetadata: config.telemetry?.logs?.addContextMetadata,
        otlpTracesEndpoints: toArray(config.telemetry?.OTLPExporter?.tracesEndpoint),
        otlpLogsEndpoints: toArray(config.telemetry?.OTLPExporter?.logsEndpoint),
        runAdminServer: config.runtimeConfig?.runAdminServer,
        adminPort: config.runtimeConfig?.admin_port,
        useListenNotify: config.use_listen_notify,
    }, options.forceConsole);
}
exports.getDbosConfig = getDbosConfig;
function toArray(endpoint) {
    return endpoint ? (Array.isArray(endpoint) ? endpoint : [endpoint]) : [];
}
function translateDbosConfig(options, forceConsole = false) {
    if (options.maxConcurrentQueueDispatches !== undefined &&
        (!Number.isInteger(options.maxConcurrentQueueDispatches) || options.maxConcurrentQueueDispatches <= 0)) {
        throw new Error('maxConcurrentQueueDispatches must be a positive integer');
    }
    if (options.notificationCoalesceMs !== undefined &&
        // Reject NaN/inf too (they slip past a bare < 1 check) so the notifier's sleep can't misbehave.
        (!Number.isFinite(options.notificationCoalesceMs) || options.notificationCoalesceMs < 1)) {
        throw new Error('notificationCoalesceMs must be a finite number at least 1 millisecond');
    }
    const systemDatabaseUrl = getSystemDatabaseUrl({
        system_database_url: options.systemDatabaseUrl,
        name: options.name,
    });
    return {
        name: options.name,
        systemDatabaseUrl,
        sysDbPoolSize: options.systemDatabasePoolSize,
        systemDatabasePollingConcurrency: options.systemDatabasePollingConcurrency,
        systemDatabasePool: options.systemDatabasePool,
        systemDatabaseSchemaName: options.systemDatabaseSchemaName ?? 'dbos',
        serializer: options.serializer ?? serialization_1.DBOSJSON,
        telemetry: {
            logs: {
                logLevel: options.logLevel || 'info',
                addContextMetadata: options.addContextMetadata,
                forceConsole,
                logger: options.logger,
            },
            OTLPExporter: {
                tracesEndpoint: options.otlpTracesEndpoints,
                logsEndpoint: options.otlpLogsEndpoints,
            },
            otelAttributeFormat: options.otelAttributeFormat ?? 'legacy',
        },
        schedulerPollingIntervalMs: options.schedulerPollingIntervalMs,
        maxConcurrentQueueDispatches: options.maxConcurrentQueueDispatches,
        useListenNotify: options.useListenNotify ?? true,
        notificationCoalesceMs: options.notificationCoalesceMs,
        // Fork default ON: the wakes are coalesced and transition-scoped, so they cost the common case
        // nothing measurable. A write-saturated deployment can set false to pay literally nothing.
        enableWakeNotifications: options.enableWakeNotifications ?? true,
    };
}
exports.translateDbosConfig = translateDbosConfig;
function getRuntimeConfig(config) {
    return translateRuntimeConfig(config.runtimeConfig);
}
exports.getRuntimeConfig = getRuntimeConfig;
function translateRuntimeConfig(config = {}) {
    return {
        runAdminServer: config.runAdminServer ?? true,
        admin_port: config.admin_port ?? config.adminPort ?? 3001,
        start: config.start ?? [],
        setup: config.setup ?? [],
    };
}
exports.translateRuntimeConfig = translateRuntimeConfig;
function overwriteConfigForDBOSCloud(providedDBOSConfig, providedRuntimeConfig, configFile) {
    // Load the DBOS configuration file and force the use of:
    // 1. Use the application name from the file. This is a defensive measure to ensure the application name is whatever it was registered with in the cloud
    // 2. use the database URL from environment var
    // 3. OTLP traces endpoints (add the config data to the provided config)
    // 4. Force admin_port and runAdminServer
    const systemDatabaseUrl = process.env.DBOS_SYSTEM_DATABASE_URL;
    (0, assert_1.default)(systemDatabaseUrl, 'DBOS_SYSTEM_DATABASE_URL must be set in DBOS Cloud environment');
    const appName = configFile.name ?? providedDBOSConfig.name;
    const logsSet = new Set(providedDBOSConfig.telemetry.OTLPExporter?.logsEndpoint);
    const logsEndpoint = configFile.telemetry?.OTLPExporter?.logsEndpoint;
    if (logsEndpoint) {
        if (Array.isArray(logsEndpoint)) {
            logsEndpoint.forEach((endpoint) => logsSet.add(endpoint));
        }
        else {
            logsSet.add(logsEndpoint);
        }
    }
    const tracesSet = new Set(providedDBOSConfig.telemetry.OTLPExporter?.tracesEndpoint);
    const tracesEndpoint = configFile.telemetry?.OTLPExporter?.tracesEndpoint;
    if (tracesEndpoint) {
        if (Array.isArray(tracesEndpoint)) {
            tracesEndpoint.forEach((endpoint) => tracesSet.add(endpoint));
        }
        else {
            tracesSet.add(tracesEndpoint);
        }
    }
    const overwritenDBOSConfig = {
        ...providedDBOSConfig,
        name: appName,
        systemDatabaseUrl,
        systemDatabaseSchemaName: configFile.system_database_schema_name ?? providedDBOSConfig.systemDatabaseSchemaName,
        telemetry: {
            logs: {
                ...providedDBOSConfig.telemetry.logs,
            },
            OTLPExporter: {
                logsEndpoint: Array.from(logsSet).filter((e) => !!e),
                tracesEndpoint: Array.from(tracesSet).filter((e) => !!e),
            },
            otelAttributeFormat: providedDBOSConfig.telemetry.otelAttributeFormat,
        },
    };
    const overwriteDBOSRuntimeConfig = {
        ...providedRuntimeConfig,
        admin_port: 3001,
        runAdminServer: true,
    };
    return [overwritenDBOSConfig, overwriteDBOSRuntimeConfig];
}
exports.overwriteConfigForDBOSCloud = overwriteConfigForDBOSCloud;
//# sourceMappingURL=config.js.map