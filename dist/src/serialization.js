"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.serializeResErrorWithSerializer = exports.serializeResError = exports.serializeArgs = exports.serializeValue = exports.safeParseError = exports.safeParsePositionalArgs = exports.safeParse = exports.deserializeResError = exports.deserializePositionalArgs = exports.deserializeValue = exports.serializeFunctionInputOutputWithSerializer = exports.serializeFunctionInputOutput = exports.DBOSPortableJSON = exports.DBOSJSON = exports.SERIALIZER_MARKER_VALUE = exports.SERIALIZER_MARKER_KEY = exports.DBOSJSONLegacy = exports.DBOSReviver = exports.DBOSReplacer = exports.registerSerializationRecipe = void 0;
const serialize_error_1 = require("serialize-error");
const superjson_1 = __importDefault(require("superjson"));
const system_db_schema_1 = require("../schemas/system_db_schema");
function registerSerializationRecipe(r) {
    superjson_1.default.registerCustom(r, r.name);
}
exports.registerSerializationRecipe = registerSerializationRecipe;
// Register Buffer transformer for SuperJSON
registerSerializationRecipe({
    isApplicable: (v) => Buffer.isBuffer(v),
    serialize: (v) => Array.from(v),
    deserialize: (v) => Buffer.from(v),
    name: 'Buffer',
});
//https://www.typescriptlang.org/docs/handbook/2/functions.html#declaring-this-in-a-function
// eslint-disable-next-line @typescript-eslint/no-explicit-any
function DBOSReplacer(key, value) {
    // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unsafe-assignment
    const actualValue = this[key];
    if (actualValue instanceof Date) {
        const res = {
            dbos_type: 'dbos_Date',
            dbos_data: actualValue.toISOString(),
        };
        return res;
    }
    if (typeof actualValue === 'bigint') {
        const res = {
            dbos_type: 'dbos_BigInt',
            dbos_data: actualValue.toString(),
        };
        return res;
    }
    return value;
}
exports.DBOSReplacer = DBOSReplacer;
function isSerializedBuffer(value) {
    return typeof value === 'object' && value !== null && value.type === 'Buffer';
}
function isSerializedDate(value) {
    return typeof value === 'object' && value !== null && value.dbos_type === 'dbos_Date';
}
function isSerializedBigInt(value) {
    return typeof value === 'object' && value !== null && value.dbos_type === 'dbos_BigInt';
}
function DBOSReviver(_key, value) {
    switch (true) {
        case isSerializedBuffer(value):
            return Buffer.from(value.data);
        case isSerializedDate(value):
            return new Date(Date.parse(value.dbos_data));
        case isSerializedBigInt(value):
            return BigInt(value.dbos_data);
        default:
            return value;
    }
}
exports.DBOSReviver = DBOSReviver;
// Keep the old DBOSJSON implementation for reference/testing
exports.DBOSJSONLegacy = {
    name: () => 'js_legacy',
    parse: (text) => {
        // eslint-disable-next-line @typescript-eslint/no-unsafe-return
        return text === null ? null : JSON.parse(text, DBOSReviver);
    },
    stringify: (value) => {
        return JSON.stringify(value, DBOSReplacer);
    },
};
// Constants for SuperJSON serialization marker
exports.SERIALIZER_MARKER_KEY = '__dbos_serializer';
exports.SERIALIZER_MARKER_VALUE = 'superjson';
const SERIALIZER_MARKER_STRING = `"${exports.SERIALIZER_MARKER_KEY}":"${exports.SERIALIZER_MARKER_VALUE}"`;
/**
 * Detects if a parsed object was serialized by our DBOSJSON with SuperJSON.
 * We check for our explicit marker to avoid ANY ambiguity with user data.
 * Also validates the object has the shape expected by superjson.deserialize().
 */
function isDBOSBrandedSuperjsonRecord(obj) {
    return (typeof obj === 'object' &&
        obj !== null &&
        exports.SERIALIZER_MARKER_KEY in obj &&
        obj[exports.SERIALIZER_MARKER_KEY] === exports.SERIALIZER_MARKER_VALUE &&
        'json' in obj);
}
function sjstringify(value) {
    // Use SuperJSON for all new serialization
    const serialized = superjson_1.default.serialize(value);
    // Add our explicit marker to make detection unambiguous
    return JSON.stringify({
        ...serialized,
        [exports.SERIALIZER_MARKER_KEY]: exports.SERIALIZER_MARKER_VALUE,
    });
}
/**
 * DBOSJSON with SuperJSON support for richer type serialization.
 *
 * Backwards compatible - can deserialize both old DBOSJSON format and new SuperJSON format.
 * New serialization uses SuperJSON to handle Sets, Maps, undefined, RegExp, circular refs, etc.
 */
exports.DBOSJSON = {
    name: () => 'js_superjson',
    parse: (text) => {
        if (text === null || text === undefined)
            return null; // This is from legacy; SuperJSON can do it.
        /**
         * Performance optimization: String check before JSON parsing.
         *
         * Why not just parse once and check the resulting object?
         * - Legacy DBOSJSON data needs the DBOSReviver function during parsing
         * - SuperJSON data must be parsed WITHOUT the reviver (it would corrupt the structure)
         * - We can't know which parser to use without inspecting the data first
         *
         * This string check lets us:
         * 1. Parse legacy data correctly with DBOSReviver in one pass (99% of cases)
         * 2. Only double-parse when we detect new SuperJSON format (rare for now)
         * 3. Avoid corrupting SuperJSON's meta structure with the wrong reviver
         */
        const hasSuperJSONMarker = text.includes(SERIALIZER_MARKER_STRING);
        if (hasSuperJSONMarker) {
            // Parse without reviver first to check if it's really our SuperJSON format
            const vanillaParsed = JSON.parse(text);
            if (isDBOSBrandedSuperjsonRecord(vanillaParsed)) {
                return superjson_1.default.deserialize(vanillaParsed);
            }
            // False positive - user data happened to contain our marker string
            // Fall through to parse with reviver
        }
        // Legacy DBOSJSON format
        return exports.DBOSJSONLegacy.parse(text);
    },
    stringify: sjstringify,
};
function portableJsonReplacer(_key, value) {
    if (value instanceof Date)
        return value.toISOString();
    if (typeof value === 'bigint')
        return value.toString(10);
    if (value instanceof Map) {
        // If keys are strings, represent as a plain JSON object.
        let allStringKeys = true;
        for (const k of value.keys()) {
            if (typeof k !== 'string') {
                allStringKeys = false;
                break;
            }
        }
        if (!allStringKeys) {
            throw new TypeError(`Attempt to do portable JSON serialization of a map with non-string keys`);
            // Other option: list of [key,value] pairs (portable, but needs schema/consumer intent)
            // return Array.from(value.entries());
        }
        const obj = {};
        for (const [k, v] of value.entries())
            obj[k] = v;
        return obj;
    }
    if (value instanceof Set)
        return Array.from(value.values());
    if (value instanceof Error) {
        return { name: value.name, message: value.message };
        // If you want stack too:
        // return { name: value.name, message: value.message, stack: value.stack };
    }
    return value;
}
/**
 * DBOS Portable JSON serializer,
 *   should be something that can be implemented in any language.
 */
exports.DBOSPortableJSON = {
    name: () => 'portable_json',
    parse: (text) => {
        // eslint-disable-next-line @typescript-eslint/no-unsafe-return
        return text === null ? null : JSON.parse(text);
    },
    stringify: (value) => {
        return JSON.stringify(value ?? null, portableJsonReplacer);
    },
};
/**
 * Roundtrips `value` through serialization.  This doesn't preserve functions by default.
 *   So then, we recursively attach function stubs that throw clear errors, for any
 *   functions present on the original (own props + prototype methods) that
 *   aren't present as functions on the deserialized object.
 * The return is both the deserialized object and its serialized string.
 */
async function serializeFunctionInputOutput(value, path = [], serializer, serializationType) {
    const serialization = serializationType === 'portable'
        ? exports.DBOSPortableJSON.name()
        : serializationType === 'native'
            ? exports.DBOSJSON.name()
            : serializer.name();
    return serializeFunctionInputOutputWithSerializer(value, path, serializer, serialization);
}
exports.serializeFunctionInputOutput = serializeFunctionInputOutput;
async function serializeFunctionInputOutputWithSerializer(value, path = [], serializer, serialization) {
    for (const ser of [exports.DBOSPortableJSON, exports.DBOSJSON]) {
        if (serialization === ser.name()) {
            const stringified = ser.stringify(value);
            const deserialized = ser.parse(stringified);
            if (isObjectish(deserialized)) {
                attachFunctionStubs(value, deserialized, path);
            }
            return { deserialized, stringified, sername: ser.name() };
        }
    }
    const sername = serializer.name();
    if (serialization && serialization !== sername) {
        throw new TypeError(`Serializer provided (${sername}) is not compatible with the required serialization (${serialization})`);
    }
    const stringified = await serializer.stringify(value);
    const deserialized = (await serializer.parse(stringified));
    if (serializer.name() === exports.DBOSJSON.name() && isObjectish(deserialized)) {
        attachFunctionStubs(value, deserialized, path);
    }
    return { deserialized, stringified, sername };
}
exports.serializeFunctionInputOutputWithSerializer = serializeFunctionInputOutputWithSerializer;
// Walks original & deserialized in lockstep and attaches stubs for missing functions.
function attachFunctionStubs(original, deserialized, path = []) {
    // Avoid infinite cycles
    const seen = new WeakSet();
    const pairQueue = [{ o: original, d: deserialized, p: path }];
    while (pairQueue.length) {
        const { o, d, p } = pairQueue.pop();
        if (seen.has(o))
            continue;
        seen.add(o);
        // Collect function keys from the original
        for (const key of collectFunctionKeys(o)) {
            if (!(key in d)) {
                defineThrowingStub(d, key, p);
            }
        }
        // Recurse into child properties (plain objects & arrays, but not maps/sets)
        for (const key of getAllKeys(o)) {
            try {
                const childO = o[key];
                const childD = d[key];
                if (!shouldRecurse(childO, childD))
                    continue;
                pairQueue.push({ o: childO, d: childD, p: [...p, key] });
            }
            catch {
                // Ignore property accessors that throw
            }
        }
        // Map/Set values
        if (o instanceof Map && d instanceof Map) {
            for (const [k, vO] of o) {
                const vD = d.get(k);
                if (shouldRecurse(vO, vD)) {
                    const step = isIndexableKey(k) ? String(k) : '[MapValue]';
                    pairQueue.push({ o: vO, d: vD, p: [...p, step] });
                }
            }
        }
        if (o instanceof Set && d instanceof Set) {
            const arrO = Array.from(o);
            const arrD = Array.from(d);
            for (let i = 0; i < Math.min(arrO.length, arrD.length); i++) {
                const vO = arrO[i];
                const vD = arrD[i];
                if (shouldRecurse(vO, vD)) {
                    pairQueue.push({ o: vO, d: vD, p: [...p, i] });
                }
            }
        }
    }
}
function isObjectish(v) {
    return (typeof v === 'object' && v !== null) || typeof v === 'function';
}
function defineThrowingStub(target, key, path) {
    const stub = function (..._args) {
        throw new Error(`Attempted to call '${String(key)}' at path ${formatPath(path)} on an object that is a serialized function input our output value. ` +
            `Functions are not preserved through serialization; see 'DBOS.registerSerialization'. `);
    };
    try {
        Object.defineProperty(target, key, {
            value: stub,
            configurable: true,
            writable: false,
            enumerable: false,
        });
    }
    catch {
        // Fall back to assignment
        target[key] = stub;
    }
}
function shouldRecurse(a, b) {
    if (!a || !b)
        return false;
    if (typeof a !== 'object' || typeof b !== 'object')
        return false;
    // Avoid recursing into special non-plain objects (Date, RegExp, etc.)
    const bad = [Date, RegExp, WeakMap, WeakSet, ArrayBuffer, DataView];
    if (bad.some((t) => a instanceof t))
        return false;
    return true;
}
function getAllKeys(obj) {
    const names = Object.getOwnPropertyNames(obj);
    const syms = Object.getOwnPropertySymbols(obj);
    return [...names, ...syms];
}
function collectFunctionKeys(obj) {
    const keys = new Set();
    // Own props
    for (const k of getAllKeys(obj)) {
        const d = Object.getOwnPropertyDescriptor(obj, k);
        if (d && 'value' in d && typeof d.value === 'function')
            keys.add(k);
    }
    // Prototype chain methods (so we also stub class methods lost after deserialization)
    let proto = Object.getPrototypeOf(obj);
    while (proto && proto !== Object.prototype) {
        for (const k of Object.getOwnPropertyNames(proto)) {
            if (k === 'constructor')
                continue;
            const d = Object.getOwnPropertyDescriptor(proto, k);
            if (d && 'value' in d && typeof d.value === 'function')
                keys.add(k);
        }
        proto = Object.getPrototypeOf(proto);
    }
    return Array.from(keys);
}
function formatPath(path) {
    if (path.length === 0)
        return '(root)';
    return path
        .map((seg) => typeof seg === 'number'
        ? `[${seg}]`
        : typeof seg === 'symbol'
            ? `[${String(seg)}]`
            : /^<?[A-Za-z_$][A-Za-z0-9_$]*>?$/.test(seg)
                ? `.${seg}`
                : `[${JSON.stringify(seg)}]`)
        .join('')
        .replace(/^\./, '');
}
function isIndexableKey(k) {
    return typeof k === 'string' || typeof k === 'number';
}
// Deserialize a plain value (not function inputs) using specified serialization,
//   or the provided default
async function deserializeValue(serializedValue, serialization, serializer) {
    if (serialization === exports.DBOSPortableJSON.name()) {
        return exports.DBOSPortableJSON.parse(serializedValue);
    }
    if (serialization === exports.DBOSJSON.name()) {
        return exports.DBOSJSON.parse(serializedValue);
    }
    if (!serialization || serialization === serializer.name()) {
        return await serializer.parse(serializedValue);
    }
    throw new TypeError(`Value deserialization type ${serialization} is not available`);
}
exports.deserializeValue = deserializeValue;
// Deserialize a plain value (not function inputs) using specified serialization,
//   or the provided default
async function deserializePositionalArgs(serializedValue, serialization, serializer) {
    if (serialization === exports.DBOSPortableJSON.name()) {
        return exports.DBOSPortableJSON.parse(serializedValue).positionalArgs ?? [];
    }
    if (serialization === exports.DBOSJSON.name()) {
        return exports.DBOSJSON.parse(serializedValue);
    }
    if (!serialization || serialization === serializer.name()) {
        return (await serializer.parse(serializedValue));
    }
    throw new TypeError(`Value deserialization type ${serialization} is not available`);
}
exports.deserializePositionalArgs = deserializePositionalArgs;
async function deserializeResError(serializedValue, serialization, serializer) {
    if (serialization === exports.DBOSPortableJSON.name()) {
        const errdata = exports.DBOSPortableJSON.parse(serializedValue);
        throw new system_db_schema_1.PortableWorkflowError(errdata.message, errdata.name, errdata.code, errdata.data);
    }
    if (serialization === exports.DBOSJSON.name()) {
        return (0, serialize_error_1.deserializeError)(exports.DBOSJSON.parse(serializedValue));
    }
    if (!serialization || serialization === serializer.name()) {
        return (0, serialize_error_1.deserializeError)(await serializer.parse(serializedValue));
    }
    throw new TypeError(`Value deserialization type ${serialization} is not available`);
}
exports.deserializeResError = deserializeResError;
// Attempt to deserialize a value, but if it fails, retun the raw string.
// Used for "best-effort" in introspection methods which may encounter
// old undeserializable data.
async function safeParse(serializer, val, serialization) {
    try {
        return await deserializeValue(val, serialization, serializer);
    }
    catch (e) {
        return val;
    }
}
exports.safeParse = safeParse;
async function safeParsePositionalArgs(serializer, val, serialization) {
    try {
        return await deserializePositionalArgs(val, serialization, serializer);
    }
    catch (e) {
        return val;
    }
}
exports.safeParsePositionalArgs = safeParsePositionalArgs;
async function safeParseError(serializer, val, serialization) {
    try {
        return await deserializeResError(val, serialization, serializer);
    }
    catch (e) {
        return new Error(val);
    }
}
exports.safeParseError = safeParseError;
async function serializeValue(value, serializer, serializationFormat) {
    if (serializationFormat === 'portable') {
        return {
            serializedValue: exports.DBOSPortableJSON.stringify(value),
            serialization: exports.DBOSPortableJSON.name(),
        };
    }
    if (serializationFormat === 'native') {
        return {
            serializedValue: exports.DBOSJSON.stringify(value),
            serialization: exports.DBOSJSON.name(),
        };
    }
    return {
        serializedValue: await serializer.stringify(value),
        serialization: serializer.name(),
    };
}
exports.serializeValue = serializeValue;
async function serializeArgs(positionalArgs, namedArgs, serializer, serializationFormat) {
    if (serializationFormat === 'portable') {
        return {
            serializedValue: exports.DBOSPortableJSON.stringify({ positionalArgs, namedArgs }),
            serialization: exports.DBOSPortableJSON.name(),
        };
    }
    if (namedArgs) {
        throw new TypeError(`Serialization format '${serializationFormat}' does not currently support named args.`);
    }
    if (serializationFormat === 'native') {
        return {
            serializedValue: exports.DBOSJSON.stringify(positionalArgs),
            serialization: exports.DBOSJSON.name(),
        };
    }
    return {
        serializedValue: await serializer.stringify(positionalArgs),
        serialization: serializer.name(),
    };
}
exports.serializeArgs = serializeArgs;
async function serializeResError(err, serializer, serializationType) {
    const serialization = serializationType === 'portable'
        ? exports.DBOSPortableJSON.name()
        : serializationType === 'native'
            ? exports.DBOSJSON.name()
            : serializer.name();
    return serializeResErrorWithSerializer(err, serializer, serialization);
}
exports.serializeResError = serializeResError;
async function serializeResErrorWithSerializer(err, serializer, serialization) {
    if (serialization === exports.DBOSPortableJSON.name()) {
        return {
            serializedValue: exports.DBOSPortableJSON.stringify({
                name: err.name,
                message: err.message,
                code: err.code,
                data: err.data,
            }),
            serialization: exports.DBOSPortableJSON.name(),
        };
    }
    if (serialization === exports.DBOSJSON.name()) {
        return {
            serializedValue: exports.DBOSJSON.stringify((0, serialize_error_1.serializeError)(err)),
            serialization: exports.DBOSJSON.name(),
        };
    }
    return {
        serializedValue: await serializer.stringify((0, serialize_error_1.serializeError)(err)),
        serialization: serializer.name(),
    };
}
exports.serializeResErrorWithSerializer = serializeResErrorWithSerializer;
//# sourceMappingURL=serialization.js.map