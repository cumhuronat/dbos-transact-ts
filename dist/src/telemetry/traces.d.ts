import { TelemetryCollector } from './collector';
import type { OtelAttributeFormat } from '../dbos-executor';
interface Attributes {
    [attributeKey: string]: AttributeValue | undefined;
}
/**
 * Attribute values may be any non-nullish primitive value except an object.
 *
 * null or undefined attribute values are invalid and will result in undefined behavior.
 */
declare type AttributeValue = string | number | boolean | Array<null | undefined | string> | Array<null | undefined | number> | Array<null | undefined | boolean>;
export declare enum SpanStatusCode {
    /**
     * The default status.
     */
    UNSET = 0,
    /**
     * The operation has been validated by an Application developer or
     * Operator to have completed successfully.
     */
    OK = 1,
    /**
     * The operation contains an error.
     */
    ERROR = 2
}
interface SpanStatus {
    /** The status code of this message. */
    code: SpanStatusCode;
    /** A developer-facing error message. */
    message?: string;
}
export type DBOSSpan = {
    setStatus(status: SpanStatus): DBOSSpan;
    attributes: Attributes;
    setAttribute(key: string, attribute: AttributeValue): DBOSSpan;
    addEvent(name: string, attributesOrStartTime?: Attributes, timeStamp?: number): DBOSSpan;
};
export declare function runWithTrace<R>(span: DBOSSpan, func: () => Promise<R>): Promise<R>;
export declare function getActiveSpan(): DBOSSpan | undefined;
export declare function isTraceContextWorking(): boolean;
export declare function installTraceContextManager(appName?: string): void;
export declare class Tracer {
    private readonly telemetryCollector;
    readonly applicationID: string;
    readonly executorID: string;
    private readonly otelAttributeFormat;
    constructor(telemetryCollector: TelemetryCollector, otelAttributeFormat?: OtelAttributeFormat);
    /**
     * Map a legacy DBOS attribute name to the name that should be emitted on
     * the span, per `otelAttributeFormat`. Returns the original key for
     * unknown attributes.
     *
     * Attributes passed into `startSpan` / `startSpanWithContext` are remapped
     * automatically via this method, so call sites don't need to invoke it
     * directly. Exposed for code paths that write attributes after span
     * creation (e.g. `endSpan`, ad-hoc `setAttribute` calls).
     */
    resolveAttributeName(key: string): string;
    private remapAttributes;
    startSpanWithContext(spanContext: unknown, name: string, attributes?: Attributes): DBOSSpan;
    startSpan(name: string, attributes?: Attributes, inputSpan?: DBOSSpan): DBOSSpan;
    endSpan(inputSpan: DBOSSpan): void;
}
export {};
//# sourceMappingURL=traces.d.ts.map