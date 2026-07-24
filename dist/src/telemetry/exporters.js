"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.TelemetryExporter = void 0;
const utils_1 = require("../utils");
function isTraceSignal(signal) {
    // Span is an interface that has a property 'kind'
    return 'kind' in signal;
}
function isLogSignal(signal) {
    // LogRecord is an interface that has a property 'severityText' and 'severityNumber'
    return 'severityText' in signal && 'severityNumber' in signal;
}
class TelemetryExporter {
    tracesExporters = [];
    logsExporters = [];
    constructor(config) {
        if (!utils_1.globalParams.enableOTLP) {
            return;
        }
        const { OTLPTraceExporter } = require('@opentelemetry/exporter-trace-otlp-proto');
        const { OTLPLogExporter } = require('@opentelemetry/exporter-logs-otlp-proto');
        const tracesSet = new Set(config.tracesEndpoint);
        for (const endpoint of tracesSet) {
            this.tracesExporters.push(new OTLPTraceExporter({
                url: endpoint,
            }));
            console.log(`Traces will be exported to ${endpoint}`);
        }
        const logsSet = new Set(config.logsEndpoint);
        for (const endpoint of logsSet) {
            this.logsExporters.push(new OTLPLogExporter({
                url: endpoint,
            }));
            console.log(`Logs will be exported to ${endpoint}`);
        }
    }
    async export(signals) {
        if (!utils_1.globalParams.enableOTLP) {
            return;
        }
        const { ExportResultCode } = require('@opentelemetry/core');
        // Sort out traces and logs
        const exportSpans = [];
        const exportLogs = [];
        signals.forEach((signal) => {
            if (isTraceSignal(signal)) {
                exportSpans.push(signal);
            }
            if (isLogSignal(signal)) {
                exportLogs.push(signal);
            }
        });
        const tasks = [];
        // A short-lived app that exits before the callback of export() will lose its data.
        // We wrap these callbacks in promise objects to make sure we wait for them:
        if (exportSpans.length > 0 && this.tracesExporters.length > 0) {
            const traceExportTask = new Promise((resolve) => {
                const exportCallback = (results) => {
                    if (results.code !== ExportResultCode.SUCCESS) {
                        console.warn(`Trace export failed: ${results.code}`);
                        console.warn(results);
                    }
                    resolve();
                };
                for (const exporter of this.tracesExporters) {
                    exporter.export(exportSpans, exportCallback);
                }
            });
            tasks.push(traceExportTask);
        }
        if (exportLogs.length > 0 && this.logsExporters.length > 0) {
            const logExportTask = new Promise((resolve) => {
                const exportCallback = (results) => {
                    if (results.code !== ExportResultCode.SUCCESS) {
                        console.warn(`Log export failed: ${results.code}`);
                        console.warn(results);
                    }
                    resolve();
                };
                for (const exporter of this.logsExporters) {
                    exporter.export(exportLogs, exportCallback);
                }
            });
            tasks.push(logExportTask);
        }
        await Promise.all(tasks);
    }
    async flush() {
        if (!utils_1.globalParams.enableOTLP) {
            return;
        }
        for (const exporter of this.tracesExporters) {
            await exporter.forceFlush();
        }
        for (const exporter of this.logsExporters) {
            await exporter.forceFlush();
        }
    }
}
exports.TelemetryExporter = TelemetryExporter;
//# sourceMappingURL=exporters.js.map