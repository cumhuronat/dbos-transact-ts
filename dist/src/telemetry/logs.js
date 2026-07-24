"use strict";
/* eslint-disable @typescript-eslint/no-unsafe-member-access */
/* eslint-disable @typescript-eslint/no-unsafe-call */
Object.defineProperty(exports, "__esModule", { value: true });
exports.DBOSConsoleLogger = exports.DBOSContextualLogger = exports.GlobalLogger = void 0;
const utils_1 = require("../utils");
const serialization_1 = require("../serialization");
const node_util_1 = require("node:util");
// As DBOS OTLP is optional, OTLP objects must only be dynamically imported
// and only when OTLP is enabled. Importing OTLP types is fine as long
// as signatures using those types are not exported from this file.
var SeverityNumber;
(function (SeverityNumber) {
    SeverityNumber[SeverityNumber["UNSPECIFIED"] = 0] = "UNSPECIFIED";
    SeverityNumber[SeverityNumber["TRACE"] = 1] = "TRACE";
    SeverityNumber[SeverityNumber["TRACE2"] = 2] = "TRACE2";
    SeverityNumber[SeverityNumber["TRACE3"] = 3] = "TRACE3";
    SeverityNumber[SeverityNumber["TRACE4"] = 4] = "TRACE4";
    SeverityNumber[SeverityNumber["DEBUG"] = 5] = "DEBUG";
    SeverityNumber[SeverityNumber["DEBUG2"] = 6] = "DEBUG2";
    SeverityNumber[SeverityNumber["DEBUG3"] = 7] = "DEBUG3";
    SeverityNumber[SeverityNumber["DEBUG4"] = 8] = "DEBUG4";
    SeverityNumber[SeverityNumber["INFO"] = 9] = "INFO";
    SeverityNumber[SeverityNumber["INFO2"] = 10] = "INFO2";
    SeverityNumber[SeverityNumber["INFO3"] = 11] = "INFO3";
    SeverityNumber[SeverityNumber["INFO4"] = 12] = "INFO4";
    SeverityNumber[SeverityNumber["WARN"] = 13] = "WARN";
    SeverityNumber[SeverityNumber["WARN2"] = 14] = "WARN2";
    SeverityNumber[SeverityNumber["WARN3"] = 15] = "WARN3";
    SeverityNumber[SeverityNumber["WARN4"] = 16] = "WARN4";
    SeverityNumber[SeverityNumber["ERROR"] = 17] = "ERROR";
    SeverityNumber[SeverityNumber["ERROR2"] = 18] = "ERROR2";
    SeverityNumber[SeverityNumber["ERROR3"] = 19] = "ERROR3";
    SeverityNumber[SeverityNumber["ERROR4"] = 20] = "ERROR4";
    SeverityNumber[SeverityNumber["FATAL"] = 21] = "FATAL";
    SeverityNumber[SeverityNumber["FATAL2"] = 22] = "FATAL2";
    SeverityNumber[SeverityNumber["FATAL3"] = 23] = "FATAL3";
    SeverityNumber[SeverityNumber["FATAL4"] = 24] = "FATAL4";
})(SeverityNumber || (SeverityNumber = {}));
// Append the `cause` (which Error.stack omits) to the stack; inspect() handles nested/circular/non-Error causes.
function errorStackWithCause(error) {
    const stack = error.stack ?? `${error.name}: ${error.message}`;
    return error.cause === undefined ? stack : `${stack}\n  [cause]: ${(0, node_util_1.inspect)(error.cause)}`;
}
class GlobalLogger {
    telemetryCollector;
    logger;
    addContextMetadata;
    isLogging = false; // Prevent recursive logging
    constructor(telemetryCollector, config, appName = 'dbos') {
        this.telemetryCollector = telemetryCollector;
        this.addContextMetadata = config?.addContextMetadata || false;
        if (config?.logger) {
            this.logger = config.logger;
            return;
        }
        if (!utils_1.globalParams.enableOTLP) {
            this.logger = new DBOSConsoleLogger(config ?? {});
            return;
        }
        const TransportStream = require('winston-transport');
        class OTLPLogQueueTransport extends TransportStream {
            telemetryCollector;
            name = 'OTLPLogQueueTransport';
            otelLogger;
            applicationID;
            executorID;
            constructor(telemetryCollector, logLevel) {
                super();
                this.telemetryCollector = telemetryCollector;
                this.level = logLevel;
                // not sure if we need a more explicit name here
                const { LoggerProvider } = require('@opentelemetry/sdk-logs');
                const logRecordProcessor = {
                    forceFlush: async () => {
                        // no-op
                    },
                    onEmit(logRecord) {
                        telemetryCollector.push(logRecord);
                    },
                    shutdown: async () => {
                        // no-op
                    },
                };
                const loggerProvider = new LoggerProvider({
                    resource: {
                        attributes: {
                            'service.name': appName,
                        },
                    },
                    processors: [logRecordProcessor],
                });
                this.otelLogger = loggerProvider.getLogger('dbos-logger');
                this.applicationID = utils_1.globalParams.appID;
                this.executorID = utils_1.globalParams.executorID;
            }
            // eslint-disable-next-line @typescript-eslint/no-explicit-any
            log(info, callback) {
                const { level, message, stack, span } = info;
                const levelToSeverityNumber = {
                    error: SeverityNumber.ERROR,
                    warn: SeverityNumber.WARN,
                    info: SeverityNumber.INFO,
                    debug: SeverityNumber.DEBUG,
                };
                // Ideally we want to give the spanContext to the logRecord,
                // But there seems to some dependency bugs in opentelemetry-js
                // (span.getValue(SPAN_KEY) undefined when we pass the context, as commented bellow)
                // So for now we get the traceId and spanId directly from the context and pass them through the logRecord attributes
                this.otelLogger.emit({
                    severityNumber: levelToSeverityNumber[level],
                    severityText: level,
                    body: message,
                    timestamp: performance.now(), // So far I don't see a major difference between this and observedTimestamp
                    observedTimestamp: performance.now(),
                    attributes: {
                        ...span?.attributes,
                        traceId: span?.spanContext()?.traceId,
                        spanId: span?.spanContext()?.spanId,
                        stack,
                        applicationID: this.applicationID,
                        applicationVersion: utils_1.globalParams.appVersion,
                        executorID: this.executorID,
                    },
                });
                callback();
            }
        }
        // Import Winston dependencies only when OTLP is enabled
        const { transports, createLogger } = require('winston');
        const winstonTransports = [];
        winstonTransports.push(new transports.Console({
            format: getConsoleFormat(),
            level: config?.logLevel || 'info',
            silent: config?.silent || false,
            forceConsole: config?.forceConsole || false,
        }));
        let otlpTransport = undefined;
        // Only enable the OTLP transport if we have a telemetry collector and an exporter
        if (utils_1.globalParams.enableOTLP && this.telemetryCollector?.exporter) {
            otlpTransport = new OTLPLogQueueTransport(this.telemetryCollector, config?.logLevel || 'info');
            winstonTransports.push(otlpTransport);
        }
        this.logger = createLogger({ transports: winstonTransports });
        if (utils_1.globalParams.enableOTLP && process.env.DBOS__CAPTURE_STD !== 'false' && this.telemetryCollector?.exporter) {
            (0, utils_1.interceptStreams)((msg, stream) => {
                if (stream === 'stdout') {
                    if (!this.isLogging) {
                        otlpTransport?.log({ level: 'info', message: msg.trim() }, () => { });
                    }
                }
                else {
                    if (!this.isLogging) {
                        otlpTransport?.log({ level: 'error', message: msg.trim(), stack: new Error().stack }, () => { });
                    }
                }
            });
        }
    }
    // We use this form of winston logging methods: `(message: string, ...meta: any[])`. See node_modules/winston/index.d.ts
    info(logEntry, metadata) {
        this.isLogging = true;
        if (typeof logEntry === 'string') {
            this.logger.info(logEntry, metadata);
        }
        else {
            this.logger.info(serialization_1.DBOSJSON.stringify(logEntry), metadata);
        }
        this.isLogging = false;
    }
    debug(logEntry, metadata) {
        this.isLogging = true;
        if (typeof logEntry === 'string') {
            this.logger.debug(logEntry, metadata);
        }
        else {
            this.logger.debug(serialization_1.DBOSJSON.stringify(logEntry), metadata);
        }
        this.isLogging = false;
    }
    warn(logEntry, metadata) {
        this.isLogging = true;
        if (typeof logEntry === 'string') {
            this.logger.warn(logEntry, metadata);
        }
        else {
            this.logger.warn(serialization_1.DBOSJSON.stringify(logEntry), metadata);
        }
        this.isLogging = false;
    }
    // metadata can have both ContextualMetadata and the error stack trace
    error(inputError, metadata) {
        this.isLogging = true;
        if (inputError instanceof Error) {
            this.logger.error(inputError.message, { ...metadata, stack: errorStackWithCause(inputError) });
        }
        else if (typeof inputError === 'string') {
            this.logger.error(inputError, { ...metadata, stack: new Error().stack });
        }
        else {
            this.logger.error(serialization_1.DBOSJSON.stringify(inputError), { ...metadata, stack: new Error().stack });
        }
        this.isLogging = false;
    }
    async destroy() {
        await this.telemetryCollector?.destroy();
    }
}
exports.GlobalLogger = GlobalLogger;
class DBOSContextualLogger {
    globalLogger;
    ctx;
    includeContextMetadata;
    constructor(globalLogger, ctx) {
        this.globalLogger = globalLogger;
        this.ctx = ctx;
        this.includeContextMetadata = this.globalLogger.addContextMetadata;
    }
    info(logEntry, metadata) {
        this.globalLogger.info(logEntry, {
            includeContextMetadata: this.includeContextMetadata,
            span: this.ctx(),
            ...metadata,
        });
    }
    debug(logEntry, metadata) {
        this.globalLogger.debug(logEntry, {
            includeContextMetadata: this.includeContextMetadata,
            span: this.ctx(),
            ...metadata,
        });
    }
    warn(logEntry, metadata) {
        this.globalLogger.warn(logEntry, {
            includeContextMetadata: this.includeContextMetadata,
            span: this.ctx(),
            ...metadata,
        });
    }
    error(inputError, metadata) {
        this.globalLogger.error(inputError, {
            includeContextMetadata: this.includeContextMetadata,
            span: this.ctx(),
            ...metadata,
        });
    }
}
exports.DBOSContextualLogger = DBOSContextualLogger;
const logLevelValues = {
    debug: 0,
    info: 1,
    warn: 2,
    error: 3,
};
class DBOSConsoleLogger {
    config;
    levelValue;
    constructor(config) {
        this.config = config;
        const level = config.logLevel ?? 'info';
        if (!(level in logLevelValues)) {
            console.warn(`Unknown log level '${level}', defaulting to 'info'. Valid levels: ${Object.keys(logLevelValues).join(', ')}`);
        }
        this.levelValue = logLevelValues[level] ?? logLevelValues['info'];
    }
    info(logEntry, _metadata) {
        if (this.levelValue <= logLevelValues['info']) {
            console.log(logEntry);
        }
    }
    debug(logEntry, _metadata) {
        if (this.levelValue <= logLevelValues['debug']) {
            console.debug(logEntry);
        }
    }
    warn(logEntry, _metadata) {
        if (this.levelValue <= logLevelValues['warn']) {
            console.warn(logEntry);
        }
    }
    error(inputError, metadata) {
        if (this.levelValue <= logLevelValues['error']) {
            if (inputError instanceof Error) {
                console.error(inputError);
            }
            else if (metadata?.stack) {
                console.error(inputError, '\n', metadata.stack);
            }
            else {
                console.error(inputError);
            }
        }
    }
}
exports.DBOSConsoleLogger = DBOSConsoleLogger;
/***********************/
/* FORMAT & TRANSPORTS */
/***********************/
// Lazily create the console format only when Winston is used
function getConsoleFormat() {
    const { format } = require('winston');
    return format.combine(format.errors({ stack: true }), format.timestamp(), format.colorize(), format.printf((info) => {
        const { timestamp, level, message, stack } = info;
        const applicationVersion = utils_1.globalParams.appVersion;
        const ts = typeof timestamp === 'string' ? timestamp.slice(0, 19).replace('T', ' ') : undefined;
        const formattedStack = typeof stack === 'string' ? stack?.split('\n').slice(1).join('\n') : undefined;
        const messageString = typeof message === 'string' ? message : serialization_1.DBOSJSON.stringify(message);
        const fullMessageString = `${messageString}${info.includeContextMetadata ? ` ${serialization_1.DBOSJSON.stringify(info.span?.attributes)}` : ''}`;
        const versionString = applicationVersion ? ` [version ${applicationVersion}]` : '';
        return `${ts}${versionString} [${level}]: ${fullMessageString} ${stack ? '\n' + formattedStack : ''}`;
    }));
}
//# sourceMappingURL=logs.js.map