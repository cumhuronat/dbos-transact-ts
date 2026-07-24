import { OTLPExporterConfig } from '../dbos-executor';
export interface ITelemetryExporter {
    export(signal: object[]): Promise<void>;
    flush(): Promise<void>;
}
export declare class TelemetryExporter implements ITelemetryExporter {
    private readonly tracesExporters;
    private readonly logsExporters;
    constructor(config: OTLPExporterConfig);
    export(signals: object[]): Promise<void>;
    flush(): Promise<void>;
}
//# sourceMappingURL=exporters.d.ts.map