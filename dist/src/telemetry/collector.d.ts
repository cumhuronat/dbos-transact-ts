import { ITelemetryExporter } from './exporters';
export declare class TelemetryCollector {
    readonly exporter?: ITelemetryExporter | undefined;
    private readonly signals;
    private readonly signalBufferID;
    private readonly processAndExportSignalsIntervalMs;
    constructor(exporter?: ITelemetryExporter | undefined);
    destroy(): Promise<void>;
    push(signal: object): void;
    private pop;
    processAndExportSignals(): Promise<void>;
}
//# sourceMappingURL=collector.d.ts.map