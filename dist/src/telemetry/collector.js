"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.TelemetryCollector = void 0;
class SignalsQueue {
    data = [];
    push(signal) {
        this.data.push(signal);
    }
    pop() {
        return this.data.shift();
    }
    size() {
        return this.data.length;
    }
}
// TODO: Handle temporary workflows properly.
class TelemetryCollector {
    exporter;
    // Signals buffer management
    signals = new SignalsQueue();
    signalBufferID;
    // We iterate on an interval and export whatever has accumulated so far
    processAndExportSignalsIntervalMs = 100;
    constructor(exporter) {
        this.exporter = exporter;
        this.signalBufferID = setInterval(() => {
            void this.processAndExportSignals();
        }, this.processAndExportSignalsIntervalMs);
    }
    async destroy() {
        clearInterval(this.signalBufferID);
        await this.processAndExportSignals();
        await this.exporter?.flush();
    }
    push(signal) {
        this.signals.push(signal);
    }
    pop() {
        return this.signals.pop();
    }
    async processAndExportSignals() {
        const batch = [];
        while (this.signals.size() > 0) {
            const signal = this.pop();
            if (!signal) {
                break;
            }
            batch.push(signal);
        }
        if (batch.length > 0) {
            if (this.exporter) {
                try {
                    await this.exporter.export(batch);
                }
                catch (e) {
                    console.error(e.message);
                }
            }
        }
    }
}
exports.TelemetryCollector = TelemetryCollector;
//# sourceMappingURL=collector.js.map