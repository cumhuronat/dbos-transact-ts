"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.Tracer = exports.installTraceContextManager = exports.isTraceContextWorking = exports.getActiveSpan = exports.runWithTrace = exports.SpanStatusCode = void 0;
const utils_1 = require("../utils");
// Legacy DBOS attribute name -> OpenTelemetry semconv-style equivalent.
// Names align with `dbos-transact-py` (Python SDK) so the two SDKs converge
// on the same attribute schema once `otelAttributeFormat: 'semconv'` is
// selected on both sides.
//
// `startSpan` / `startSpanWithContext` sweep the supplied attributes dict
// through `resolveAttributeName`, so call sites can pass legacy DBOS keys
// directly and have them remapped automatically when
// `otelAttributeFormat === 'semconv'`. Keys not in this table pass through
// unchanged. `endSpan` does the same for the few attributes it sets after
// the span has been created. This mirrors the equivalent loop in
// `dbos-transact-py`'s `start_span`.
const LEGACY_TO_SEMCONV = {
    operationUUID: 'dbos.operation.workflow_id',
    operationType: 'dbos.operation.type',
    operationName: 'dbos.operation.name',
    applicationID: 'dbos.application.id',
    applicationVersion: 'dbos.application.version',
    executorID: 'dbos.executor.id',
    queueName: 'dbos.queue.name',
    authenticatedUser: 'dbos.user.name',
    authenticatedRoles: 'dbos.user.roles',
    assumedRole: 'dbos.user.assumed_role',
    requestID: 'dbos.request.id',
    requestIP: 'dbos.request.ip',
    requestURL: 'dbos.request.url',
    requestMethod: 'dbos.request.method',
};
var SpanStatusCode;
(function (SpanStatusCode) {
    /**
     * The default status.
     */
    SpanStatusCode[SpanStatusCode["UNSET"] = 0] = "UNSET";
    /**
     * The operation has been validated by an Application developer or
     * Operator to have completed successfully.
     */
    SpanStatusCode[SpanStatusCode["OK"] = 1] = "OK";
    /**
     * The operation contains an error.
     */
    SpanStatusCode[SpanStatusCode["ERROR"] = 2] = "ERROR";
})(SpanStatusCode || (exports.SpanStatusCode = SpanStatusCode = {}));
class StubSpan {
    attributes = {};
    setStatus(_status) {
        return this;
    }
    setAttribute(_key, _attribute) {
        return this;
    }
    addEvent(_name, _attributesOrStartTime, _timeStamp) {
        return this;
    }
}
function runWithTrace(span, func) {
    if (!utils_1.globalParams.tracingEnabled) {
        return func();
    }
    const { context, trace } = require('@opentelemetry/api');
    return context.with(trace.setSpan(context.active(), span), func);
}
exports.runWithTrace = runWithTrace;
function getActiveSpan() {
    if (!utils_1.globalParams.tracingEnabled) {
        return undefined;
    }
    const { trace } = require('@opentelemetry/api');
    return trace.getActiveSpan();
}
exports.getActiveSpan = getActiveSpan;
function isTraceContextWorking() {
    if (!utils_1.globalParams.tracingEnabled) {
        return false;
    }
    const { context, trace } = require('@opentelemetry/api');
    const span = trace.getTracer('otel-bootstrap-check').startSpan('probe');
    const testContext = trace.setSpan(context.active(), span);
    let visible;
    context.with(testContext, () => {
        visible = trace.getSpan(context.active()) === span;
    });
    span.end?.();
    return visible === true;
}
exports.isTraceContextWorking = isTraceContextWorking;
function installTraceContextManager(appName = 'dbos') {
    if (!utils_1.globalParams.tracingEnabled) {
        return;
    }
    const { AsyncLocalStorageContextManager } = require('@opentelemetry/context-async-hooks');
    const { context, trace } = require('@opentelemetry/api');
    const { BasicTracerProvider } = require('@opentelemetry/sdk-trace-base');
    // setGlobalTracerProvider and setGlobalContextManager are "first one wins."
    // If an external provider is already registered, these calls are safely ignored.
    const contextManager = new AsyncLocalStorageContextManager();
    contextManager.enable();
    context.setGlobalContextManager(contextManager);
    const provider = new BasicTracerProvider({
        resource: {
            attributes: {
                'service.name': appName,
            },
        },
    });
    trace.setGlobalTracerProvider(provider);
}
exports.installTraceContextManager = installTraceContextManager;
class Tracer {
    telemetryCollector;
    applicationID;
    executorID;
    otelAttributeFormat;
    constructor(telemetryCollector, otelAttributeFormat = 'legacy') {
        this.telemetryCollector = telemetryCollector;
        this.applicationID = utils_1.globalParams.appID;
        this.executorID = utils_1.globalParams.executorID;
        this.otelAttributeFormat = otelAttributeFormat;
    }
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
    resolveAttributeName(key) {
        if (this.otelAttributeFormat === 'semconv') {
            return LEGACY_TO_SEMCONV[key] ?? key;
        }
        return key;
    }
    remapAttributes(attributes) {
        if (!attributes || this.otelAttributeFormat !== 'semconv') {
            return attributes;
        }
        const remapped = {};
        for (const [k, v] of Object.entries(attributes)) {
            remapped[LEGACY_TO_SEMCONV[k] ?? k] = v;
        }
        return remapped;
    }
    startSpanWithContext(spanContext, name, attributes) {
        if (!utils_1.globalParams.tracingEnabled) {
            return new StubSpan();
        }
        const opentelemetry = require('@opentelemetry/api');
        const tracer = opentelemetry.trace.getTracer('dbos-tracer');
        const ctx = opentelemetry.trace.setSpanContext(opentelemetry.context.active(), spanContext);
        return tracer.startSpan(name, { startTime: performance.now(), attributes: this.remapAttributes(attributes) }, ctx);
    }
    startSpan(name, attributes, inputSpan) {
        if (!utils_1.globalParams.tracingEnabled) {
            return new StubSpan();
        }
        const parentSpan = inputSpan;
        const opentelemetry = require('@opentelemetry/api');
        const { hrTime } = require('@opentelemetry/core');
        const tracer = opentelemetry.trace.getTracer('dbos-tracer');
        const startTime = hrTime(performance.now());
        const remapped = this.remapAttributes(attributes);
        if (parentSpan) {
            const ctx = opentelemetry.trace.setSpan(opentelemetry.context.active(), parentSpan);
            return tracer.startSpan(name, { startTime: startTime, attributes: remapped }, ctx);
        }
        else {
            return tracer.startSpan(name, { startTime: startTime, attributes: remapped });
        }
    }
    endSpan(inputSpan) {
        if (!utils_1.globalParams.tracingEnabled) {
            return;
        }
        const { hrTime } = require('@opentelemetry/core');
        const span = inputSpan;
        span.setAttributes({
            [this.resolveAttributeName('applicationID')]: this.applicationID,
            [this.resolveAttributeName('applicationVersion')]: utils_1.globalParams.appVersion,
        });
        const executorIDKey = this.resolveAttributeName('executorID');
        if (span.attributes && !(executorIDKey in span.attributes)) {
            span.setAttribute(executorIDKey, this.executorID);
        }
        span.end(hrTime(performance.now()));
        // Only push to DBOS's own collector when DBOS manages export.
        // When an external TracerProvider is used, span.end() triggers its processors.
        if (utils_1.globalParams.enableOTLP) {
            this.telemetryCollector.push(span);
        }
    }
}
exports.Tracer = Tracer;
//# sourceMappingURL=traces.js.map